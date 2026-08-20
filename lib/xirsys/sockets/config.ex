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

defmodule Xirsys.Sockets.Config do
  @moduledoc """
  Runtime configuration for `xturn_sockets`.

  Set `config :xturn_sockets, config_app: :my_app` in the host project so
  `config :my_app, ...` overrides library defaults.
  """

  @rate_limit_window 60_000
  @max_requests_per_window 1000

  @default_reorder [
    enabled: true,
    window: 16,
    max_delay_ms: 100,
    on_overflow: :flush_oldest
  ]

  @reorder_keys [:enabled, :window, :max_delay_ms, :on_overflow]

  @doc """
  Looks up configuration with precedence: host application (see `:config_app`),
  then `:xturn_sockets`, then `default`.
  """
  @spec get(atom(), any()) :: any()
  def get(key, default \\ nil) do
    case host_env(key) do
      {:ok, value} ->
        value

      :error ->
        Application.get_env(:xturn_sockets, key, default)
    end
  end

  @doc false
  @spec ssl_handshake_timeout() :: pos_integer()
  def ssl_handshake_timeout(), do: get(:ssl_handshake_timeout, 10_000)

  @doc false
  @spec buffer_size() :: pos_integer()
  def buffer_size(), do: get(:buffer_size, 256 * 1024)

  @doc """
  Receive/send buffer size for shared client-facing listener sockets (UDP/TCP accept).

  Defaults larger than `buffer_size/0` so the kernel queue can absorb bursts while
  worker processes drain packets. Relay sockets use their own hard-coded buffers.
  """
  @spec listener_buffer_size() :: pos_integer()
  def listener_buffer_size(), do: get(:listener_buffer_size, 4 * 1024 * 1024)

  @doc false
  @spec tier_pool_size() :: pos_integer()
  def tier_pool_size(), do: get(:tier_pool_size, System.schedulers_online())

  @doc false
  @spec udp_session_idle_ms() :: pos_integer()
  def udp_session_idle_ms(), do: get(:udp_session_idle_ms, 30_000)

  @doc false
  @spec max_udp_sessions() :: pos_integer()
  def max_udp_sessions(), do: get(:max_udp_sessions, 100_000)

  @doc false
  @spec udp_session_sweep_ms() :: pos_integer()
  def udp_session_sweep_ms(), do: get(:udp_session_sweep_ms, 5_000)

  @doc false
  @spec rate_limit_enabled?() :: boolean()
  def rate_limit_enabled?(), do: get(:rate_limit_enabled, true)

  @doc false
  @spec server_ip() :: :inet.ip_address()
  def server_ip(), do: get(:server_ip, {0, 0, 0, 0})

  @doc false
  @spec server_local_ip() :: :inet.ip_address()
  def server_local_ip(), do: get(:server_local_ip, {0, 0, 0, 0})

  @doc """
  Merged reorder tunables for a named tier.

  Precedence (lowest to highest): library defaults, `:xturn_sockets` tier config,
  host `:config_app` tier config, explicit keys in `opts`.
  """
  @spec reorder_opts(atom() | nil, keyword()) :: keyword()
  def reorder_opts(name, opts) when is_list(opts) do
    lib_opts = reorder_tier(:xturn_sockets, name)
    host_opts = reorder_tier(host_app(), name)
    explicit = Keyword.take(opts, @reorder_keys)

    @default_reorder
    |> Keyword.merge(lib_opts)
    |> Keyword.merge(host_opts)
    |> Keyword.merge(explicit)
  end

  @doc """
  Fixed-window request counter for one client IP.

  Intended for control-plane requests only. Callers must not apply this to
  relayed data (TURN ChannelData, Send/Data indications): media runs at
  hundreds of packets per second, so any request-shaped budget silently starves
  it within seconds.

  Each IP holds a single row carrying the current window index and its count,
  so both the check and the update are O(1) and memory stays constant per IP.
  """
  @spec check_rate_limit(:inet.ip_address()) :: :ok | {:error, :rate_limited}
  def check_rate_limit(client_ip) do
    if rate_limit_enabled?() do
      table = rate_limit_table()
      window = get(:rate_limit_window, @rate_limit_window)
      max_requests = get(:max_requests_per_window, @max_requests_per_window)
      current_window = div(System.monotonic_time(:millisecond), window)

      case :ets.lookup(table, client_ip) do
        [{^client_ip, ^current_window, count}] when count >= max_requests ->
          {:error, :rate_limited}

        [{^client_ip, ^current_window, _count}] ->
          :ets.update_counter(table, client_ip, {3, 1})
          :ok

        _ ->
          # No row yet, or the previous window has rolled over.
          :ets.insert(table, {client_ip, current_window, 1})
          :ok
      end
    else
      :ok
    end
  end

  @spec rate_limit_table() :: :ets.table()
  defp rate_limit_table do
    case :ets.whereis(:turn_rate_limits) do
      :undefined ->
        :ets.new(:turn_rate_limits, [:named_table, :public, {:write_concurrency, true}])

      table ->
        table
    end
  end

  @spec host_app() :: atom() | nil
  defp host_app do
    Application.get_env(:xturn_sockets, :config_app)
  end

  @spec host_env(atom()) :: {:ok, term()} | :error
  defp host_env(key) do
    case host_app() do
      nil ->
        :error

      app ->
        case Application.fetch_env(app, key) do
          {:ok, value} -> {:ok, value}
          :error -> :error
        end
    end
  end

  @spec reorder_tier(atom() | nil, atom() | nil) :: keyword()
  defp reorder_tier(_app, nil), do: []

  defp reorder_tier(app, name) when is_atom(app) and is_atom(name) do
    Application.get_env(app, :reorder, [])
    |> Keyword.get(name, [])
  end
end
