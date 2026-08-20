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

defmodule Xirsys.Sockets.Accumulator.Raw do
  @moduledoc """
  No framing: each `push/3` produces one immediately poppable packet.

  ## Options

    * `:max_size` - maximum queued whole packets (default: `1024`)
  """
  @behaviour Xirsys.Sockets.Accumulator

  @default_max 1024

  @impl true
  def init(opts) do
    %{
      queue: :queue.new(),
      max_size: Keyword.get(opts, :max_size, @default_max),
      overflow: false
    }
  end

  @impl true
  def push(%{queue: queue, max_size: max_size} = acc, chunk, meta) do
    queue = :queue.in({chunk, meta}, queue)

    if :queue.len(queue) > max_size do
      {{:value, _dropped}, queue} = :queue.out(queue)
      %{acc | queue: queue, overflow: true}
    else
      %{acc | queue: queue}
    end
  end

  @impl true
  def pop(%{overflow: true} = acc) do
    {:error, :buffer_overflow, %{acc | overflow: false}}
  end

  def pop(%{queue: queue} = acc) do
    case :queue.out(queue) do
      {{:value, {packet, meta}}, rest} ->
        {:ok, packet, meta, %{acc | queue: rest}}

      {:empty, _} ->
        {:more, acc}
    end
  end
end
