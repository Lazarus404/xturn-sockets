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

defmodule Xirsys.Sockets.Transport do
  @moduledoc """
  Uniform socket I/O behaviour for all wire protocols.
  """

  @type socket :: term()
  @type ip_address :: :inet.ip_address()
  @type port_number :: :inet.port_number()
  @type from :: {ip_address(), port_number()} | nil

  @callback listen(ip_address(), port_number(), keyword()) ::
              {:ok, socket()} | {:error, term()}

  @callback accept(socket(), timeout()) :: {:ok, socket()} | {:error, term()}

  @callback send(socket(), iodata(), from()) :: :ok | {:error, term()}

  @callback setopts(socket(), keyword()) :: :ok | {:error, term()}

  @callback sockname(socket()) ::
              {:ok, {ip_address(), port_number()}}
              | {:local, binary()}
              | {:unspec, <<>>}
              | {:undefined, any()}
              | {:error, term()}

  @callback peername(socket()) ::
              {:ok, {ip_address(), port_number()}}
              | {:local, binary()}
              | {:unspec, <<>>}
              | {:undefined, any()}
              | {:error, term()}

  @callback close(socket()) :: :ok

  @callback handle_message(term(), socket()) ::
              {:data, binary(), from()}
              | {:closed, term()}
              | :ignore

  @callback controlling_process(socket(), pid()) :: :ok | {:error, term()}

  @doc """
  ChannelData alignment rule for this transport (RFC 5766, Section 11.5).

    * `:stream`   - outgoing ChannelData MUST be padded to a 4-byte boundary
    * `:datagram` - padding is optional and omitted on send
  """
  @callback framing() :: :stream | :datagram

  @optional_callbacks peername: 1, controlling_process: 2
end
