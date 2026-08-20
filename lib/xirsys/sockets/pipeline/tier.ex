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

defmodule Xirsys.Sockets.Pipeline.Tier do
  @moduledoc false

  @enforce_keys [:accumulator, :accumulator_opts, :handler]
  defstruct [
    :accumulator,
    :accumulator_opts,
    :handler,
    dispatch: :inline,
    pool_size: nil,
    task_supervisor: Xirsys.Sockets.TierSupervisor.Task,
    pool_supervisor: Xirsys.Sockets.TierSupervisor.Pool
  ]

  @type t :: %__MODULE__{
          accumulator: module(),
          accumulator_opts: keyword(),
          handler: module(),
          dispatch: :inline | :task | :pool,
          pool_size: pos_integer() | nil,
          task_supervisor: atom(),
          pool_supervisor: atom()
        }
end
