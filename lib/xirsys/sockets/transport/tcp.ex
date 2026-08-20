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

defmodule Xirsys.Sockets.Transport.TCP do
  @moduledoc false
  @behaviour Xirsys.Sockets.Transport

  alias Xirsys.Sockets.{Config, Telemetry}

  @listen_opts [
    reuseaddr: true,
    keepalive: true,
    backlog: 100,
    active: false,
    nodelay: true
  ]

  @impl true
  def listen(ip, port, opts) do
    listen_opts =
      @listen_opts
      |> Keyword.merge(buffer_opts())
      |> Keyword.merge(opts)
      |> Keyword.put(:ip, ip)

    case :gen_tcp.listen(port, listen_opts) do
      {:ok, _sock} = ok ->
        Telemetry.emit(:tcp_listener_created, %{}, %{ip: ip, port: port})
        ok

      {:error, _} = error ->
        error
    end
  end

  @impl true
  def accept(listen_sock, timeout) do
    case :gen_tcp.accept(listen_sock, timeout) do
      {:ok, cli} = ok ->
        _ = :inet.setopts(cli, [:binary])
        Telemetry.emit(:connection_accepted, %{protocol: :tcp}, %{})
        ok

      {:error, _} = error ->
        error
    end
  end

  @impl true
  def send(socket, data, _to), do: :gen_tcp.send(socket, data)

  @impl true
  def setopts(socket, opts), do: :inet.setopts(socket, opts)

  @impl true
  def sockname(socket), do: :inet.sockname(socket)

  @impl true
  def peername(socket), do: :inet.peername(socket)

  @impl true
  def close(socket) do
    :gen_tcp.close(socket)
    Telemetry.emit(:socket_closed, %{protocol: :tcp}, %{})
    :ok
  end

  @impl true
  def controlling_process(socket, pid), do: :gen_tcp.controlling_process(socket, pid)

  @impl true
  def framing(), do: :stream

  @impl true
  def handle_message({:tcp, _port, data}, _socket), do: {:data, data, nil}
  def handle_message({:tcp_closed, _}, _socket), do: {:closed, :normal}
  def handle_message({:tcp_error, _, reason}, _socket), do: {:closed, reason}
  def handle_message(_, _socket), do: :ignore

  defp buffer_opts do
    size = Config.buffer_size()
    [buffer: size, recbuf: size, sndbuf: size]
  end
end
