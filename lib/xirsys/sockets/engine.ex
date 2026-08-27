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

defmodule Xirsys.Sockets.Engine do
  @moduledoc """
  Drain loop: pop every whole packet from a tier, dispatch the handler, repeat.

  `Connection` and `DatagramServer` call `push_and_drain/9` after each read.
  `drain/8` is used for timer ticks (reorder timeouts) without a new chunk.
  """
  require Logger

  alias Xirsys.Sockets.{
    Conn,
    Pipeline,
    Pipeline.Tier,
    Telemetry,
    TierSupervisor
  }

  @typedoc """
  Result of a drain pass: updated accumulators, handler states, async tier
  pids, and `:ok` or `:close`.
  """
  @type drain_result :: {map(), map(), map(), :ok | :close}

  @doc """
  Pops whole packets from `tier_key` until the accumulator returns `{:more, _}`.

  Handler `{:reply, _, _}` is sent on `conn.socket`. `{:descend, next, payload, _}`
  pushes `payload` into `next`. `{:close, _}` stops the connection after this pass.
  Framing errors emit `:frame_error` and continue.

  ## Parameters

    * `pipeline` - compiled pipeline
    * `tier_key` - tier to drain (`:root`, …)
    * `conn` - connection context used for replies
    * `accs` - map of tier name to accumulator state
    * `states` - map of tier name to handler state
    * `tier_sessions` - map of async tier name to session pid
    * `transport_mod` - `Xirsys.Sockets.Transport` implementation
    * `owner_pid` - connection or datagram-server process
  """
  @spec drain(
          Pipeline.t(),
          atom(),
          Conn.t(),
          map(),
          map(),
          map(),
          module(),
          pid()
        ) :: drain_result()
  def drain(pipeline, tier_key, conn, accs, states, tier_sessions, transport_mod, owner_pid) do
    %Tier{accumulator: accumulator_mod, handler: handler_mod} =
      Pipeline.tier_spec(pipeline, tier_key)

    acc = Map.fetch!(accs, tier_key)
    handler_state = Map.fetch!(states, tier_key)

    case accumulator_mod.pop(acc) do
      {:ok, packet, meta, acc} ->
        accs = Map.put(accs, tier_key, acc)

        case safe_handle(
               handler_mod,
               :handle_packet,
               [packet, meta, conn, handler_state],
               tier_key
             ) do
          {:ok, handler_state} ->
            states = Map.put(states, tier_key, handler_state)

            drain(
              pipeline,
              tier_key,
              conn,
              accs,
              states,
              tier_sessions,
              transport_mod,
              owner_pid
            )

          {:reply, out, handler_state} ->
            send_reply(transport_mod, conn, out, tier_key)
            states = Map.put(states, tier_key, handler_state)

            drain(
              pipeline,
              tier_key,
              conn,
              accs,
              states,
              tier_sessions,
              transport_mod,
              owner_pid
            )

          {:descend, next_tier, payload, handler_state} ->
            states = Map.put(states, tier_key, handler_state)

            case descend_and_drain(
                   pipeline,
                   next_tier,
                   payload,
                   meta,
                   conn,
                   accs,
                   states,
                   tier_sessions,
                   transport_mod,
                   owner_pid
                 ) do
              {accs, states, tier_sessions, :close} ->
                {accs, states, tier_sessions, :close}

              {accs, states, tier_sessions, :ok} ->
                drain(
                  pipeline,
                  tier_key,
                  conn,
                  accs,
                  states,
                  tier_sessions,
                  transport_mod,
                  owner_pid
                )
            end

          {:close, handler_state} ->
            states = Map.put(states, tier_key, handler_state)
            {accs, states, tier_sessions, :close}

          {:error, reason} ->
            Logger.warning("handler error: #{inspect(reason)}")
            Telemetry.emit(:handler_error, %{}, %{reason: inspect(reason), tier: tier_key})

            drain(
              pipeline,
              tier_key,
              conn,
              accs,
              states,
              tier_sessions,
              transport_mod,
              owner_pid
            )
        end

      {:more, acc} ->
        {Map.put(accs, tier_key, acc), states, tier_sessions, :ok}

      {:error, reason, acc} ->
        Telemetry.emit(:frame_error, %{}, %{reason: reason, tier: tier_key})

        drain(
          pipeline,
          tier_key,
          conn,
          Map.put(accs, tier_key, acc),
          states,
          tier_sessions,
          transport_mod,
          owner_pid
        )
    end
  end

  @doc """
  Pushes `chunk` into `:root`, then drains from `:root`.

  Unthrottled: this path carries media as well as control traffic. Apply
  `Config.check_rate_limit/1` in the handler for the message classes that
  need a budget.

  ## Parameters

    * `pipeline` - compiled pipeline
    * `chunk` - inbound bytes from `handle_message/2`
    * `meta` - packet metadata (`:from`, `:received_at`, …)
    * `conn` - connection context
    * `accs` / `states` / `tier_sessions` - session maps
    * `transport_mod` - transport used for `{:reply, _, _}`
    * `owner_pid` - owning process
  """
  @spec push_and_drain(
          Pipeline.t(),
          binary(),
          map(),
          Conn.t(),
          map(),
          map(),
          map(),
          module(),
          pid()
        ) :: drain_result()
  def push_and_drain(
        pipeline,
        chunk,
        meta,
        conn,
        accs,
        states,
        tier_sessions,
        transport_mod,
        owner_pid
      ) do
    %Tier{accumulator: root_acc_mod} = Pipeline.tier_spec(pipeline, :root)
    root_acc = Map.fetch!(accs, :root) |> root_acc_mod.push(chunk, meta)
    accs = Map.put(accs, :root, root_acc)

    drain(
      pipeline,
      :root,
      conn,
      accs,
      states,
      tier_sessions,
      transport_mod,
      owner_pid
    )
  end

  defp descend_and_drain(
         pipeline,
         next_tier,
         payload,
         meta,
         conn,
         accs,
         states,
         tier_sessions,
         transport_mod,
         owner_pid
       ) do
    %Tier{dispatch: dispatch} = Pipeline.tier_spec(pipeline, next_tier)

    case dispatch do
      :inline ->
        inline_descend_and_drain(
          pipeline,
          next_tier,
          payload,
          meta,
          conn,
          accs,
          states,
          tier_sessions,
          transport_mod,
          owner_pid
        )

      mode when mode in [:task, :pool] ->
        async_descend(
          pipeline,
          next_tier,
          payload,
          meta,
          conn,
          accs,
          states,
          tier_sessions,
          transport_mod,
          owner_pid,
          mode
        )
    end
  end

  defp inline_descend_and_drain(
         pipeline,
         next_tier,
         payload,
         meta,
         conn,
         accs,
         states,
         tier_sessions,
         transport_mod,
         owner_pid
       ) do
    %Tier{accumulator: next_acc_mod, accumulator_opts: next_acc_opts} =
      Pipeline.tier_spec(pipeline, next_tier)

    accs =
      accs
      |> Map.put_new(next_tier, next_acc_mod.init(next_acc_opts))
      |> update_in([next_tier], &next_acc_mod.push(&1, payload, meta))

    states = Map.put_new(states, next_tier, nil)

    safe_drain(
      pipeline,
      next_tier,
      conn,
      accs,
      states,
      tier_sessions,
      transport_mod,
      owner_pid
    )
  end

  defp async_descend(
         pipeline,
         next_tier,
         payload,
         meta,
         conn,
         accs,
         states,
         tier_sessions,
         transport_mod,
         owner_pid,
         mode
       ) do
    case Map.get(tier_sessions, next_tier) do
      pid when is_pid(pid) ->
        if Process.alive?(pid) do
          GenServer.cast(pid, {:push, payload, meta})
          {accs, states, tier_sessions, :ok}
        else
          start_async_session(
            pipeline,
            next_tier,
            payload,
            meta,
            conn,
            accs,
            states,
            tier_sessions,
            transport_mod,
            owner_pid,
            mode
          )
        end

      _ ->
        start_async_session(
          pipeline,
          next_tier,
          payload,
          meta,
          conn,
          accs,
          states,
          tier_sessions,
          transport_mod,
          owner_pid,
          mode
        )
    end
  end

  defp start_async_session(
         pipeline,
         next_tier,
         payload,
         meta,
         conn,
         accs,
         states,
         tier_sessions,
         transport_mod,
         owner_pid,
         mode
       ) do
    case start_tier_session(pipeline, next_tier, conn, owner_pid, transport_mod, mode) do
      {:ok, pid} ->
        GenServer.cast(pid, {:push, payload, meta})
        {accs, states, Map.put(tier_sessions, next_tier, pid), :ok}

      {:error, :max_children} ->
        Telemetry.emit(:tier_pool_saturated, %{}, %{tier: next_tier, dropped: true})
        {accs, states, tier_sessions, :ok}

      {:error, :no_supervisor} ->
        Telemetry.emit(:tier_supervisor_unavailable, %{}, %{tier: next_tier, dropped: true})
        {accs, states, tier_sessions, :ok}

      {:error, {:supervisor_error, reason}} ->
        Telemetry.emit(:tier_supervisor_unavailable, %{}, %{
          tier: next_tier,
          dropped: true,
          reason: inspect(reason)
        })

        {accs, states, tier_sessions, :ok}

      {:error, reason} ->
        Logger.warning("failed to start tier session: #{inspect(reason)}")
        Telemetry.emit(:tier_dropped, %{}, %{tier: next_tier, reason: inspect(reason)})
        {accs, states, tier_sessions, :ok}
    end
  end

  defp start_tier_session(pipeline, tier_key, conn, owner_pid, transport_mod, mode) do
    %Tier{task_supervisor: task_supervisor, pool_supervisor: pool_supervisor} =
      Pipeline.tier_spec(pipeline, tier_key)

    supervisor =
      case mode do
        :task -> task_supervisor
        :pool -> pool_supervisor
      end

    try do
      TierSupervisor.start_session(supervisor,
        pipeline: pipeline,
        tier_key: tier_key,
        conn: conn,
        owner: owner_pid,
        transport: transport_mod
      )
    catch
      :exit, {:noproc, _} -> {:error, :no_supervisor}
      :exit, reason -> {:error, {:supervisor_error, reason}}
    end
  end

  defp safe_drain(pipeline, tier_key, conn, accs, states, tier_sessions, transport_mod, owner_pid) do
    drain(pipeline, tier_key, conn, accs, states, tier_sessions, transport_mod, owner_pid)
  rescue
    error ->
      reset_tier(pipeline, tier_key, accs, states, tier_sessions, error)
  catch
    :exit, reason ->
      reset_tier(pipeline, tier_key, accs, states, tier_sessions, reason)
  end

  defp reset_tier(pipeline, tier_key, accs, states, tier_sessions, reason) do
    %Tier{accumulator: mod, accumulator_opts: accumulator_opts} =
      Pipeline.tier_spec(pipeline, tier_key)

    Logger.warning("tier #{inspect(tier_key)} crashed, resetting accumulator: #{inspect(reason)}")

    Telemetry.emit(:tier_crashed, %{}, %{
      tier: tier_key,
      reason: inspect(reason),
      reset: true
    })

    {Map.put(accs, tier_key, mod.init(accumulator_opts)), states, tier_sessions, :ok}
  end

  defp send_reply(transport_mod, %Conn{} = conn, out, tier_key) do
    to = if conn.client_ip, do: {conn.client_ip, conn.client_port}, else: nil
    bytes = IO.iodata_length(out)

    case transport_mod.send(conn.socket, out, to) do
      :ok ->
        Logger.debug(
          "send_reply: sent #{bytes} bytes to #{inspect(to)} via socket #{inspect(conn.socket)} (tier=#{tier_key})"
        )

        Telemetry.emit(:message_sent, %{bytes: bytes}, %{tier: tier_key})

      {:error, reason} ->
        Logger.error(
          "send_reply: FAILED sending #{bytes} bytes to #{inspect(to)} via socket #{inspect(conn.socket)}: #{inspect(reason)} (tier=#{tier_key})"
        )

        Telemetry.emit(:send_error, %{}, %{error: reason, tier: tier_key})
    end
  end

  defp safe_handle(mod, fun, args, tier_key) do
    start = System.monotonic_time(:native)

    try do
      result = apply(mod, fun, args)
      duration = System.monotonic_time(:native) - start
      Telemetry.emit(:tier_dispatch, %{duration: duration}, %{tier: tier_key})
      result
    rescue
      error ->
        duration = System.monotonic_time(:native) - start

        Logger.error(
          "safe_handle rescued exception in #{inspect(mod)}.#{fun}: #{Exception.format(:error, error, __STACKTRACE__)}"
        )

        Telemetry.emit(:tier_dispatch, %{duration: duration}, %{tier: tier_key})
        {:error, error}
    catch
      :exit, reason ->
        duration = System.monotonic_time(:native) - start
        Logger.error("safe_handle caught exit in #{inspect(mod)}.#{fun}: #{inspect(reason)}")
        Telemetry.emit(:tier_dispatch, %{duration: duration}, %{tier: tier_key})
        {:error, reason}
    end
  end
end
