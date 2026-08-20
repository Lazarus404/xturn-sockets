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

defmodule Xirsys.Sockets.Connection do
  @moduledoc """
  Per-connection process that owns accumulator state across reads and fully drains
  after each inbound chunk.
  """
  use GenServer

  alias Xirsys.Sockets.{Config, Conn, Engine, Pipeline, Pipeline.Tier, Telemetry}

  @active_opts [:binary, active: :once]

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def child_spec(opts) do
    %{
      id: {__MODULE__, opts},
      start: {__MODULE__, :start_link, [opts]},
      restart: :temporary
    }
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    transport_mod = Keyword.fetch!(opts, :transport)
    socket = Keyword.fetch!(opts, :socket)
    pipeline = resolve_pipeline(opts)

    with {:ok, server_ip, server_port} <- local_endpoint(transport_mod, socket),
         {:ok, client_ip, client_port} <- peer_endpoint(transport_mod, socket),
         :ok <- transport_mod.setopts(socket, @active_opts) do
      conn = %Conn{
        listener: Keyword.get(opts, :listener),
        socket: socket,
        client_ip: client_ip,
        client_port: client_port,
        server_ip: server_ip,
        server_port: server_port,
        assigns: Keyword.get(opts, :assigns, %{})
      }

      {accs, states} = Pipeline.init_session(pipeline, conn, opts)
      tick_interval_ms = Keyword.get(opts, :tick_interval_ms)

      if tick_interval_ms do
        schedule_tick(tick_interval_ms)
      end

      {:ok,
       %{
         transport: transport_mod,
         socket: socket,
         pipeline: pipeline,
         conn: conn,
         accs: accs,
         states: states,
         tier_sessions: %{},
         session_monitors: %{},
         tick_interval_ms: tick_interval_ms
       }}
    else
      {:error, reason} ->
        transport_mod.close(socket)
        {:stop, reason}
    end
  end

  @impl true
  def handle_cast({:send, data, ip, port}, state) do
    to = if ip, do: {ip, port}, else: nil
    _ = state.transport.send(state.socket, data, to)
    {:noreply, state}
  end

  def handle_cast(:stop, state), do: {:stop, :normal, state}

  @impl true
  def handle_info(:tick, state) do
    {accs, states, tier_sessions, action} =
      Engine.drain(
        state.pipeline,
        :root,
        state.conn,
        state.accs,
        state.states,
        state.tier_sessions,
        state.transport,
        self()
      )

    state =
      state
      |> Map.merge(%{accs: accs, states: states})
      |> apply_tier_sessions(tier_sessions)

    schedule_tick_if_needed(state)
    rearm(state, action)
  end

  def handle_info({:tier_close, _tier_key, reason}, state) do
    safe_disconnect_all(state.pipeline, state.states, reason)
    {:stop, reason, state}
  end

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.session_monitors, ref) do
      {{tier, ^pid}, monitors} ->
        Telemetry.emit(:tier_crashed, %{}, %{tier: tier, reason: inspect(reason)})

        {:noreply,
         %{
           state
           | tier_sessions: Map.delete(state.tier_sessions, tier),
             session_monitors: monitors
         }}

      {nil, _} ->
        {:noreply, state}
    end
  end

  def handle_info(msg, state) do
    case state.transport.handle_message(msg, state.socket) do
      {:data, chunk, from} ->
        conn = update_from(state.conn, from)
        meta = %{from: from, received_at: System.monotonic_time(:millisecond)}

        {accs, states, tier_sessions, action} =
          Engine.push_and_drain(
            state.pipeline,
            chunk,
            meta,
            conn,
            state.accs,
            state.states,
            state.tier_sessions,
            state.transport,
            self()
          )

        state =
          state
          |> Map.merge(%{accs: accs, states: states, conn: conn})
          |> apply_tier_sessions(tier_sessions)

        rearm(state, action)

      {:closed, reason} ->
        safe_disconnect_all(state.pipeline, state.states, reason)
        {:stop, reason, state}

      :ignore ->
        {:noreply, state}
    end
  end

  @impl true
  def terminate(reason, state) do
    safe_disconnect_all(state.pipeline, state.states, reason)
    state.transport.close(state.socket)
    :ok
  end

  defp rearm(state, :close), do: {:stop, :normal, state}

  defp rearm(state, :ok) do
    _ = state.transport.setopts(state.socket, @active_opts)
    {:noreply, state}
  end

  defp apply_tier_sessions(state, tier_sessions) do
    new_tiers = Map.keys(tier_sessions) -- Map.keys(state.tier_sessions)

    monitors =
      Enum.reduce(new_tiers, state.session_monitors, fn tier, monitors ->
        pid = Map.fetch!(tier_sessions, tier)
        ref = Process.monitor(pid)
        Map.put(monitors, ref, {tier, pid})
      end)

    %{state | tier_sessions: tier_sessions, session_monitors: monitors}
  end

  defp schedule_tick_if_needed(%{tick_interval_ms: nil}), do: :ok

  defp schedule_tick_if_needed(%{tick_interval_ms: interval}) when is_integer(interval) do
    schedule_tick(interval)
  end

  defp schedule_tick(interval) when is_integer(interval) and interval > 0 do
    Process.send_after(self(), :tick, interval)
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

  defp local_endpoint(transport_mod, socket) do
    case transport_mod.sockname(socket) do
      {:ok, {ip, port}} -> {:ok, ip, port}
      _ -> {:ok, Config.server_ip(), 0}
    end
  end

  defp peer_endpoint(transport_mod, socket) do
    case transport_mod.peername(socket) do
      {:ok, {ip, port}} -> {:ok, ip, port}
      _ -> {:ok, nil, nil}
    end
  end

  defp update_from(%Conn{} = conn, {ip, port}) do
    %{conn | client_ip: ip, client_port: port}
  end

  defp update_from(conn, _), do: conn

  defp safe_disconnect_all(pipeline, states, reason) do
    Enum.each(states, fn {tier_key, handler_state} ->
      %Tier{handler: handler_mod} = Pipeline.tier_spec(pipeline, tier_key)

      if function_exported?(handler_mod, :handle_disconnect, 2) do
        handler_mod.handle_disconnect(reason, handler_state)
      end
    end)

    Telemetry.emit(:connection_closed, %{}, %{reason: reason})
  catch
    _, _ -> :ok
  end
end
