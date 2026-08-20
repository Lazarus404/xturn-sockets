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

defmodule Xirsys.Sockets.TierSession do
  @moduledoc false
  use GenServer

  alias Xirsys.Sockets.{Engine, Pipeline, Pipeline.Tier, Telemetry}

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

    pipeline = Keyword.fetch!(opts, :pipeline)
    tier_key = Keyword.fetch!(opts, :tier_key)
    conn = Keyword.fetch!(opts, :conn)
    owner = Keyword.fetch!(opts, :owner)
    transport = Keyword.fetch!(opts, :transport)

    ref = Process.monitor(owner)

    %Tier{accumulator: acc_mod, accumulator_opts: acc_opts} = Pipeline.tier_spec(pipeline, tier_key)

    {:ok,
     %{
       pipeline: pipeline,
       tier_key: tier_key,
       conn: conn,
       owner: owner,
       owner_ref: ref,
       transport: transport,
             accs: %{tier_key => acc_mod.init(acc_opts)},
             states: %{tier_key => nil},
             tier_sessions: %{},
             session_monitors: %{}
           }}
  end

  @impl true
  def handle_cast({:push, payload, meta}, state) do
    %Tier{accumulator: acc_mod} = Pipeline.tier_spec(state.pipeline, state.tier_key)

    accs =
      state.accs
      |> Map.update!(state.tier_key, &acc_mod.push(&1, payload, meta))

    {accs, states, tier_sessions, action} =
      Engine.drain(
        state.pipeline,
        state.tier_key,
        state.conn,
        accs,
        state.states,
        state.tier_sessions,
        state.transport,
        state.owner
      )

    case action do
      :close ->
        send(state.owner, {:tier_close, state.tier_key, :normal})
        {:stop, :normal, state}

      :ok ->
        state =
          state
          |> Map.merge(%{accs: accs, states: states})
          |> apply_tier_sessions(tier_sessions)

        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:DOWN, ref, :process, _pid, _reason}, %{owner_ref: ref} = state) do
    {:stop, :normal, state}
  end

  def handle_info({:tier_close, tier_key, reason}, state) do
    send(state.owner, {:tier_close, tier_key, reason})
    {:stop, :normal, state}
  end

  def handle_info({:EXIT, _pid, _reason}, state), do: {:stop, :normal, state}

  def handle_info({:DOWN, ref, :process, pid, reason}, state) do
    case Map.pop(state.session_monitors, ref) do
      {{tier, ^pid}, monitors} ->
        Telemetry.emit(:tier_crashed, %{}, %{tier: tier, reason: inspect(reason)})

        tier_sessions =
          if Map.get(state.tier_sessions, tier) == pid do
            Map.delete(state.tier_sessions, tier)
          else
            state.tier_sessions
          end

        {:noreply, %{state | tier_sessions: tier_sessions, session_monitors: monitors}}

      {nil, _} ->
        {:noreply, state}
    end
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
end
