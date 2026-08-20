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

defmodule Xirsys.Sockets.Transport.DTLS do
  @moduledoc false
  @behaviour Xirsys.Sockets.Transport

  alias Xirsys.Sockets.Transport.TLS

  @impl true
  def listen(ip, port, opts), do: TLS.listen(ip, port, Keyword.put(opts, :protocol, :dtls))

  @impl true
  def accept(listen_sock, timeout) do
    case TLS.accept(listen_sock, timeout) do
      {:ok, _cli} = ok ->
        ok

      error ->
        error
    end
  end

  @impl true
  def send(socket, data, to), do: TLS.send(socket, data, to)

  @impl true
  def setopts(socket, opts), do: TLS.setopts(socket, opts)

  @impl true
  def sockname(socket), do: TLS.sockname(socket)

  @impl true
  def peername(socket), do: TLS.peername(socket)

  @impl true
  def close(socket), do: TLS.close(socket)

  @impl true
  def controlling_process(socket, pid), do: TLS.controlling_process(socket, pid)

  @impl true
  def framing(), do: :datagram

  @impl true
  def handle_message(msg, socket), do: TLS.handle_message(msg, socket)
end
