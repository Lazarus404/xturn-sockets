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

defmodule Xirsys.Sockets.Conn do
  @moduledoc """
  Transport-level connection context passed to handlers.

  Built by `Connection` (streams) or `DatagramServer` (datagrams). Handlers
  read addresses and `assigns`; they do not own the socket.

      iex> conn = %Xirsys.Sockets.Conn{client_ip: {127, 0, 0, 1}, client_port: 3478}
      iex> conn.client_port
      3478
      iex> conn.assigns
      %{}
  """

  @typedoc """
  Connection context.

    * `listener` - `Acceptor` / `DatagramServer` pid, if any
    * `socket` - opaque transport socket
    * `client_ip` / `client_port` - peer (updated per datagram on UDP)
    * `server_ip` / `server_port` - local bind address
    * `assigns` - caller-supplied map, unchanged by the engine
  """
  @type t :: %__MODULE__{
          listener: pid() | nil,
          socket: term(),
          client_ip: :inet.ip_address() | nil,
          client_port: :inet.port_number() | nil,
          server_ip: :inet.ip_address() | nil,
          server_port: :inet.port_number() | nil,
          assigns: map()
        }

  defstruct listener: nil,
            socket: nil,
            client_ip: nil,
            client_port: nil,
            server_ip: nil,
            server_port: nil,
            assigns: %{}
end
