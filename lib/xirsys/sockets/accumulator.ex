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

defmodule Xirsys.Sockets.Accumulator do
  @moduledoc """
  Per-tier packet framing state machine.

  ## Bounded buffers

  Every `Accumulator` implementation should support a `:max_size` option limiting
  how much data or how many whole packets may be held before signalling overflow.
  Because `push/3` returns only `acc()`, implementations typically record overflow
  during `push/3` (for example by dropping the oldest entry and setting a flag) and
  surface it once from the next `pop/1` as `{:error, :buffer_overflow, acc}`. The
  engine logs `:frame_error` telemetry and continues draining after overflow.
  """

  @type acc :: term()
  @type meta :: map()

  @callback init(keyword()) :: acc()

  @callback push(acc(), binary(), meta()) :: acc()

  @callback pop(acc()) ::
              {:ok, binary(), meta(), acc()}
              | {:more, acc()}
              | {:error, term(), acc()}
end
