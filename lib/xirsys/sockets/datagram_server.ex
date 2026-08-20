### ----------------------------------------------------------------------
###
### Copyright (c) 2013 - 2026 Jahred Love and Xirsys LLC <experts@xirsys.com>
###
### All rights reserved.
###
### XTurn is licensed by Xirsys under the Apache
### License, Version 2.0. (the "License");
###
### you may not use this file except in compliance with the License.
### You may obtain a copy of the License at
###
###      http://www.apache.org/licenses/LICENSE-2.0
###
### Unless required by applicable law or agreed to in writing, software
### distributed under the License is distributed on an "AS IS" BASIS,
### WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
### See the License for the specific language governing permissions and
### limitations under the License.
###
### See LICENSE for the full license text.
###
### ----------------------------------------------------------------------

defmodule Xirsys.Sockets.DatagramServer do
  @moduledoc """
  Plain-UDP listener that drains every whole packet from each datagram independently.
  """
  use GenServer
  require Logger

  alias Xirsys.Sockets.{Config, Conn, Engine, Pipeline, Pipeline.Tier, Telemetry}

  @active_opts [:binary, active: :once]
  @max_packet_size 64 * 1024

  # System.monotonic_time/1 is relative to an arbitrary, possibly-negative
  # origin, so 0 is not a safe "unknown" sentinel: on a VM where `now` is
  # itself negative, `now - 0` reads as *fresher* than any real entry and a
  # malformed entry would never age out. Use a value far below any realistic
  # monotonic time so a malformed entry always looks maximally stale and gets
  # reclaimed by the next sweep instead of leaking.
  @min_last_seen -9_223_372_036_854_775_808

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def port(pid), do: GenServer.call(pid, :port)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    transport_mod = Keyword.get(opts, :transport, Xirsys.Sockets.Transport.UDP)
    ip = Keyword.fetch!(opts, :ip)
    port = Keyword.fetch!(opts, :port)
    listen_opts = Keyword.get(opts, :listen_opts, [])
    pipeline = resolve_pipeline(opts)
    tick_interval_ms = Keyword.get(opts, :tick_interval_ms)
    peer_sessions? = peer_sessions?(tick_interval_ms, pipeline)

    case transport_mod.listen(ip, port, listen_opts) do
      {:ok, socket} ->
        :ok = transport_mod.setopts(socket, @active_opts)

        {:ok, server_ip, server_port} = local_endpoint(transport_mod, socket)
        Telemetry.emit(:udp_listener_started, %{}, %{ip: server_ip, port: server_port})

        if tick_interval_ms do
          schedule_tick(tick_interval_ms)
        end

        if peer_sessions? do
          schedule_sweep(Config.udp_session_sweep_ms())
        end

        {:ok,
         %{
           transport: transport_mod,
           socket: socket,
           pipeline: pipeline,
           server_ip: server_ip,
           server_port: server_port,
           assigns: Keyword.get(opts, :assigns, %{}),
           init_opts: Keyword.take(opts, [:handler_state]),
           tick_interval_ms: tick_interval_ms,
           peer_sessions?: peer_sessions?,
           sessions: if(peer_sessions?, do: %{}, else: nil),
           session_monitors: %{}
         }}

      {:error, reason} = error ->
        Logger.error("DatagramServer failed to open socket: #{inspect(reason)}")
        error
    end
  end

  @impl true
  def handle_call(:port, _from, state) do
    {:reply, state.server_port, state}
  end

  @impl true
  def handle_cast({:send, data, ip, port}, state) do
    _ = state.transport.send(state.socket, data, {ip, port})
    {:noreply, state}
  end

  def handle_cast(:stop, state), do: {:stop, :normal, state}

  @impl true
  def handle_info(:tick, state) do
    sessions =
      Enum.reduce(state.sessions, %{}, fn {peer, entry}, sessions ->
        conn = peer_conn(state, peer)
        accs = entry.accs
        states = entry.states
        tier_sessions = entry.tier_sessions
        last_seen = entry_last_seen(entry)

        {accs, states, tier_sessions, _action} =
          Engine.drain(
            state.pipeline,
            :root,
            conn,
            accs,
            states,
            tier_sessions,
            state.transport,
            self()
          )

        Map.put(sessions, peer, new_entry(accs, states, tier_sessions, last_seen))
      end)

    schedule_tick_if_needed(state)
    {:noreply, %{state | sessions: sessions}}
  end

  def handle_info(:sweep, %{sessions: sessions} = state) when is_map(sessions) do
    now = System.monotonic_time(:millisecond)
    idle_ms = Config.udp_session_idle_ms()
    max_sessions = Config.max_udp_sessions()

    peers_to_evict = peers_to_evict(sessions, now, idle_ms, max_sessions)

    state =
      Enum.reduce(peers_to_evict, state, fn peer, state ->
        evict_peer(state, peer)
      end)

    if peers_to_evict != [] do
      Telemetry.emit(:udp_sessions_evicted, %{count: length(peers_to_evict)}, %{})
    end

    schedule_sweep(Config.udp_session_sweep_ms())
    {:noreply, state}
  end

  def handle_info(:sweep, state), do: {:noreply, state}

  def handle_info({:tier_close, _tier_key, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.session_monitors, ref) do
      {{peer, tier, ^pid}, monitors} ->
        Telemetry.emit(:tier_crashed, %{}, %{tier: tier, reason: inspect(reason), peer: peer})

        sessions =
          case state.sessions do
            nil ->
              nil

            sessions ->
              case Map.fetch(sessions, peer) do
                {:ok, %{tier_sessions: tier_sessions} = entry} ->
                  if Map.get(tier_sessions, tier) == pid do
                    Map.put(sessions, peer, %{entry | tier_sessions: Map.delete(tier_sessions, tier)})
                  else
                    sessions
                  end

                _ ->
                  sessions
              end
          end

        {:noreply, %{state | session_monitors: monitors, sessions: sessions}}

      {nil, _} ->
        {:noreply, state}
    end
  end

  def handle_info(msg, state) do
    case state.transport.handle_message(msg, state.socket) do
      {:data, chunk, {client_ip, client_port}} ->
        if byte_size(chunk) > @max_packet_size do
          Telemetry.emit(:oversized_packet, %{bytes: byte_size(chunk)}, %{
            ip: client_ip,
            port: client_port
          })

          rearm(state)
        else
          process_datagram(state, chunk, client_ip, client_port)
        end

      :ignore ->
        rearm(state)

      other ->
        Logger.debug("DatagramServer ignored message: #{inspect(other)}")
        rearm(state)
    end
  end

  @impl true
  def terminate(_reason, state) do
    state.transport.close(state.socket)
    Telemetry.emit(:udp_listener_stopped, %{}, %{})
    :ok
  end

  defp process_datagram(state, chunk, client_ip, client_port) do
    conn = peer_conn(state, {client_ip, client_port})
    peer = {client_ip, client_port}
    now = System.monotonic_time(:millisecond)
    meta = %{from: peer, received_at: now}

    old_peer_entry = if state.sessions, do: Map.get(state.sessions, peer), else: nil

    {sessions, accs, states, tier_sessions} =
      if state.sessions do
        case old_peer_entry do
          %{accs: accs, states: states, tier_sessions: tier_sessions} ->
            {accs, states, tier_sessions} =
              refresh_root_acc(state.pipeline, accs, states, tier_sessions)

            {state.sessions, accs, states, tier_sessions}

          nil ->
            {accs, states} = Pipeline.fresh_session(state.pipeline, state.init_opts)
            {state.sessions, accs, states, %{}}
        end
      else
        {accs, states} = Pipeline.fresh_session(state.pipeline, state.init_opts)
        {state.sessions, accs, states, %{}}
      end

    {accs, states, tier_sessions, _action} =
      Engine.push_and_drain(
        state.pipeline,
        chunk,
        meta,
        conn,
        accs,
        states,
        tier_sessions,
        state.transport,
        self()
      )

    Telemetry.emit(:udp_packet_processed, %{bytes: byte_size(chunk)}, %{
      ip: client_ip,
      port: client_port
    })

    prior_tier_sessions =
      case old_peer_entry do
        %{tier_sessions: tier_sessions} -> tier_sessions
        _ -> %{}
      end

    state =
      state
      |> monitor_new_tier_sessions(peer, tier_sessions, prior_tier_sessions)
      |> then(fn state ->
        if state.sessions do
          %{state | sessions: Map.put(sessions, peer, new_entry(accs, states, tier_sessions, now))}
        else
          state
        end
      end)

    rearm(state)
  end

  defp new_entry(accs, states, tier_sessions, last_seen) do
    %{accs: accs, states: states, tier_sessions: tier_sessions, last_seen: last_seen}
  end

  defp entry_last_seen(%{last_seen: last_seen}) when is_integer(last_seen), do: last_seen
  defp entry_last_seen(_), do: @min_last_seen

  defp peer_sessions?(tick_interval_ms, pipeline) do
    not is_nil(tick_interval_ms) or Pipeline.multi_tier?(pipeline)
  end

  defp refresh_root_acc(pipeline, accs, states, tier_sessions) do
    %Tier{accumulator: root_mod, accumulator_opts: root_opts} = Pipeline.tier_spec(pipeline, :root)
    {Map.put(accs, :root, root_mod.init(root_opts)), states, tier_sessions}
  end

  defp monitor_new_tier_sessions(state, peer, new_sessions, old_sessions) do
    Enum.reduce(new_sessions, state, fn {tier, pid}, state ->
      case Map.get(old_sessions, tier) do
        ^pid ->
          state

        _ ->
          ref = Process.monitor(pid)

          monitors = Map.put(state.session_monitors, ref, {peer, tier, pid})
          %{state | session_monitors: monitors}
      end
    end)
  end

  defp peers_to_evict(sessions, now, idle_ms, max_sessions) do
    idle_peers =
      sessions
      |> Enum.filter(fn {_peer, entry} -> now - entry_last_seen(entry) >= idle_ms end)
      |> Enum.map(fn {peer, _} -> peer end)

    idle_set = MapSet.new(idle_peers)

    over_capacity =
      if map_size(sessions) - length(idle_peers) > max_sessions do
        excess = map_size(sessions) - length(idle_peers) - max_sessions

        sessions
        |> Enum.reject(fn {peer, _} -> MapSet.member?(idle_set, peer) end)
        |> Enum.sort_by(fn {_peer, entry} -> entry_last_seen(entry) end)
        |> Enum.take(excess)
        |> Enum.map(fn {peer, _} -> peer end)
      else
        []
      end

    Enum.uniq(idle_peers ++ over_capacity)
  end

  defp evict_peer(state, peer) do
    case Map.pop(state.sessions, peer) do
      {nil, _} ->
        state

      {entry, sessions} ->
        tier_sessions = Map.get(entry, :tier_sessions, %{})

        {to_drop, to_keep} =
          Enum.split_with(state.session_monitors, fn {_ref, {p, _tier, _pid}} -> p == peer end)

        for {ref, _} <- to_drop, do: Process.demonitor(ref, [:flush])

        for {_tier, pid} <- tier_sessions do
          if Process.alive?(pid), do: Process.exit(pid, :shutdown)
        end

        %{state | sessions: sessions, session_monitors: Map.new(to_keep)}
    end
  end

  defp peer_conn(state, {client_ip, client_port}) do
    %Conn{
      listener: self(),
      socket: state.socket,
      client_ip: client_ip,
      client_port: client_port,
      server_ip: state.server_ip,
      server_port: state.server_port,
      assigns: state.assigns
    }
  end

  defp resolve_pipeline(opts) do
    case Keyword.get(opts, :pipeline) do
      nil ->
        accumulator = Keyword.fetch!(opts, :accumulator)
        handler = Keyword.fetch!(opts, :handler)
        Pipeline.resolve({accumulator, handler})

      pipeline_mod when is_atom(pipeline_mod) ->
        Pipeline.resolve(pipeline_mod)
    end
  end

  defp rearm(state) do
    _ = state.transport.setopts(state.socket, @active_opts)
    {:noreply, state}
  end

  defp schedule_tick_if_needed(%{tick_interval_ms: nil}), do: :ok

  defp schedule_tick_if_needed(%{tick_interval_ms: interval}) when is_integer(interval) do
    schedule_tick(interval)
  end

  defp schedule_tick(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
  end

  defp schedule_sweep(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :sweep, interval)
  end

  defp local_endpoint(transport_mod, socket) do
    case transport_mod.sockname(socket) do
      {:ok, {ip, port}} -> {:ok, ip, port}
      _ -> {:ok, Config.server_ip(), 0}
    end
  end
end
