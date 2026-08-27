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

defmodule Xirsys.Sockets.TierSupervisor.Task do
  @moduledoc """
  Named supervisor for `dispatch: :task` tiers (unbounded by default).
  """

  alias Xirsys.Sockets.TierSupervisor

  @doc """
  Starts `TierSupervisor` registered as this module.

  ## Parameters

    * `opts` - `:max_children` (default `:infinity`)
  """
  def start_link(opts \\ []) do
    max_children = Keyword.get(opts, :max_children, :infinity)

    TierSupervisor.start_link(
      name: __MODULE__,
      max_children: max_children
    )
  end
end
