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

defmodule Xirsys.Sockets.Handler do
  @moduledoc """
  Business logic callback for a single framing tier.
  """

  alias Xirsys.Sockets.Conn

  @type state :: term()
  @type meta :: map()

  @callback handle_connect(Conn.t()) :: {:ok, state()}

  @callback handle_packet(binary(), meta(), Conn.t(), state()) ::
              {:ok, state()}
              | {:reply, iodata(), state()}
              | {:descend, atom(), binary(), state()}
              | {:close, state()}

  @callback handle_disconnect(term(), state()) :: :ok

  @optional_callbacks handle_connect: 1, handle_disconnect: 2
end
