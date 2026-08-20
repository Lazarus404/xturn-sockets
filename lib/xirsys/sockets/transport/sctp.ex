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

defmodule Xirsys.Sockets.Transport.SCTP do
  @moduledoc false
  @behaviour Xirsys.Sockets.Transport

  alias Xirsys.Sockets.{Config, Telemetry}

  @listen_opts [
    reuseaddr: true,
    backlog: 100,
    active: false,
    nodelay: true,
    sctp_nodelay: true,
    sctp_autoclose: 0,
    sctp_maxseg: 1400,
    sctp_initmsg: %{num_ostreams: 10, max_instreams: 10, max_attempts: 4, max_init_timeo: 30_000}
  ]

  @impl true
  def listen(ip, port, opts) do
    listen_opts =
      @listen_opts
      |> Keyword.merge(buffer_opts())
      |> Keyword.merge(opts)
      |> Keyword.put(:ip, ip)

    try do
      case :gen_sctp.open(port, listen_opts) do
        {:ok, _sock} = ok ->
          Telemetry.emit(:sctp_listener_created, %{}, %{ip: ip, port: port})
          ok

        {:error, _} = error ->
          error
      end
    rescue
      _ -> {:error, :sctp_not_supported}
    end
  end

  @impl true
  # gen_sctp has no accept/2. Associations arrive as messages on the listening
  # socket and are split off with gen_sctp:peeloff/2, so SCTP cannot currently be
  # driven by Acceptor's accept loop. This previously called the nonexistent
  # function and relied on rescuing UndefinedFunctionError to reach the same
  # result.
  def accept(_listen_sock, _timeout), do: {:error, :sctp_not_supported}

  @impl true
  def send(socket, data, _to) do
    try do
      :gen_sctp.send(socket, 0, 0, data)
    rescue
      _ -> {:error, :sctp_not_supported}
    end
  end

  @impl true
  def setopts(socket, opts), do: :inet.setopts(socket, opts)

  @impl true
  def sockname(socket), do: :inet.sockname(socket)

  @impl true
  def peername(socket), do: :inet.peername(socket)

  @impl true
  def close(socket) do
    try do
      :gen_sctp.close(socket)
    catch
      _, _ -> :ok
    end

    Telemetry.emit(:socket_closed, %{protocol: :sctp}, %{})
    :ok
  end

  @impl true
  def controlling_process(socket, pid), do: :gen_sctp.controlling_process(socket, pid)

  @impl true
  def framing(), do: :datagram

  @impl true
  def handle_message({:sctp, _msg_socket, ip, port, _anc, data}, _socket),
    do: {:data, data, {ip, port}}

  def handle_message({:sctp_closed, _}, _socket), do: {:closed, :normal}
  def handle_message({:sctp_error, _, reason}, _socket), do: {:closed, reason}
  def handle_message(_, _socket), do: :ignore

  defp buffer_opts do
    size = Config.buffer_size()
    [buffer: size, recbuf: size, sndbuf: size]
  end
end
