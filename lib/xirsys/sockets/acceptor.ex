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

defmodule Xirsys.Sockets.Acceptor do
  @moduledoc """
  Generic accept loop for connection/association-oriented transports.
  """
  use GenServer
  require Logger

  alias Xirsys.Sockets.{SockSupervisor, Telemetry}

  @accept_timeout 1_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def port(pid), do: GenServer.call(pid, :port)

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    transport_mod = Keyword.fetch!(opts, :transport)
    ip = Keyword.fetch!(opts, :ip)
    port = Keyword.fetch!(opts, :port)
    listen_opts = Keyword.get(opts, :listen_opts, [])

    case transport_mod.listen(ip, port, listen_opts) do
      {:ok, listen_sock} ->
        :ok = transport_mod.setopts(listen_sock, active: false)
        Process.send(self(), :accept, [])

        Telemetry.emit(:listener_started, %{}, %{
          transport: transport_mod,
          ip: ip,
          port: port
        })

        {:ok,
         %{
           transport: transport_mod,
           listen: listen_sock,
           handler: Keyword.get(opts, :handler),
           accumulator: Keyword.get(opts, :accumulator),
           pipeline: Keyword.get(opts, :pipeline),
           assigns: Keyword.get(opts, :assigns, %{}),
           accept_timeout: Keyword.get(opts, :accept_timeout, @accept_timeout)
         }}

      {:error, reason} = error ->
        Logger.error("Acceptor failed to listen: #{inspect(reason)}")
        error
    end
  end

  @impl true
  def handle_call(:port, _from, state) do
    port =
      case state.transport.sockname(state.listen) do
        {:ok, {_, p}} -> p
        _ -> 0
      end

    {:reply, port, state}
  end

  @impl true
  def handle_info(:accept, state) do
    case state.transport.accept(state.listen, state.accept_timeout) do
      {:ok, client_sock} ->
        start_connection(state, client_sock)
        Process.send(self(), :accept, [])
        {:noreply, state}

      {:error, :timeout} ->
        Process.send(self(), :accept, [])
        {:noreply, state}

      {:error, reason} ->
        Logger.warning("Accept failed: #{inspect(reason)}")
        Process.send(self(), :accept, [])
        {:noreply, state}
    end
  end

  def handle_info({:EXIT, _, _}, state) do
    Process.send(self(), :accept, [])
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    state.transport.close(state.listen)
    :ok
  end

  defp start_connection(state, client_sock) do
    child_opts =
      [
        transport: state.transport,
        socket: client_sock,
        listener: self(),
        assigns: state.assigns
      ]
      |> maybe_put(:pipeline, state.pipeline)
      |> maybe_put_legacy_tier(state)

    case SockSupervisor.start_connection(child_opts) do
      {:ok, pid} ->
        transfer_control(state.transport, client_sock, pid)

      {:error, reason} ->
        Logger.warning("Failed to start connection: #{inspect(reason)}")
        state.transport.close(client_sock)
    end
  end

  defp transfer_control(transport_mod, client_sock, pid) do
    if function_exported?(transport_mod, :controlling_process, 2) do
      case transport_mod.controlling_process(client_sock, pid) do
        :ok ->
          :ok

        {:error, reason} ->
          Logger.warning("Failed to transfer socket control: #{inspect(reason)}")
          transport_mod.close(client_sock)
      end
    else
      :ok
    end
  end

  defp maybe_put(keyword, _key, nil), do: keyword
  defp maybe_put(keyword, key, value), do: Keyword.put(keyword, key, value)

  defp maybe_put_legacy_tier(keyword, %{pipeline: nil, handler: handler, accumulator: accumulator})
       when not is_nil(handler) and not is_nil(accumulator) do
    Keyword.merge(keyword, handler: handler, accumulator: accumulator)
  end

  defp maybe_put_legacy_tier(keyword, %{pipeline: pipeline}) when not is_nil(pipeline), do: keyword
  defp maybe_put_legacy_tier(keyword, _state), do: keyword
end
