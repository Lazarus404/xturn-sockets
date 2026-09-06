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
  @moduledoc """
  SCTP transport (`:gen_sctp`) when the OTP driver is available.

  ## What problem this solves

  Some deployments listen for SCTP over IP directly via OTP's `:gen_sctp`. This
  module implements the `Transport` behaviour for that path: bind, send on stream
  0, and normalize `{:sctp, ...}` messages.

  **Not** WebRTC SCTP-over-DTLS; see `SctpAssociation` for the sans-IO path used
  with DTLS application data.

  `accept/2` always returns `{:error, :sctp_not_supported}` because associations
  arrive as messages on the listen socket and cannot be driven by `Acceptor`'s
  accept loop.

      iex> Xirsys.Sockets.Transport.SCTP.framing()
      :datagram
      iex> Xirsys.Sockets.Transport.SCTP.handle_message({:sctp, :sock, {127, 0, 0, 1}, 9, [], "hi"}, :sock)
      {:data, "hi", {{127, 0, 0, 1}, 9}}
      iex> Xirsys.Sockets.Transport.SCTP.handle_message({:sctp_closed, :sock}, :sock)
      {:closed, :normal}

  ## RFCs

  - [RFC 4960](https://www.rfc-editor.org/rfc/rfc4960) - SCTP (streams, associations)
  """
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

  @doc """
  SCTP has no `accept/2`; associations arrive as messages on the listen socket.

  Always returns `{:error, :sctp_not_supported}` so callers use a custom
  association handler instead of `Acceptor`'s accept loop.

  ## Parameters

    * `_listen_sock` - SCTP listen socket (unused)
    * `_timeout` - accept timeout (unused)
  """
  @impl true
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
