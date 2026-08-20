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

defmodule Xirsys.Sockets.Transport.UDP do
  @moduledoc false
  @behaviour Xirsys.Sockets.Transport

  alias Xirsys.Sockets.{Config, Telemetry}

  @listen_opts [active: false]

  @impl true
  def listen(ip, port, opts) do
    listen_opts =
      @listen_opts
      |> Keyword.merge(buffer_opts())
      |> Keyword.merge(opts)
      |> Keyword.put(:ip, ip)
      |> Keyword.put(:reuseaddr, true)

    case :gen_udp.open(port, listen_opts) do
      {:ok, sock} = ok ->
        _ = :inet.setopts(sock, [:binary])
        Telemetry.emit(:udp_listener_created, %{}, %{ip: ip, port: port})
        ok

      {:error, _} = error ->
        error
    end
  end

  @impl true
  def accept(_socket, _timeout), do: {:error, :connectionless}

  @impl true
  def send(socket, data, {ip, port}), do: :gen_udp.send(socket, ip, port, data)

  @impl true
  def setopts(socket, opts), do: :inet.setopts(socket, opts)

  @impl true
  def sockname(socket), do: :inet.sockname(socket)

  @impl true
  def peername(_socket), do: {:error, :connectionless}

  @impl true
  def close(socket) do
    :gen_udp.close(socket)
    Telemetry.emit(:socket_closed, %{protocol: :udp}, %{})
    :ok
  end

  @impl true
  def framing(), do: :datagram

  @impl true
  def handle_message({:udp, _port, ip, port, data}, _socket), do: {:data, data, {ip, port}}
  def handle_message(_, _socket), do: :ignore

  defp buffer_opts do
    size = Config.listener_buffer_size()
    [buffer: size, recbuf: size, sndbuf: size]
  end
end
