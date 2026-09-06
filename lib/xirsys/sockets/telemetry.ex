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

defmodule Xirsys.Sockets.Telemetry do
  @moduledoc """
  Optional `:telemetry` events under `[:xturn_sockets, event]`.

  `emit/3` no-ops when `config :xturn_sockets, telemetry_enabled: false`, and
  swallows errors if `:telemetry` is not started. `attach_handlers/0` installs
  library log/counter handlers; callers may attach their own instead.

  ## What problem this solves

  Operators running [XTurn](https://github.com/Lazarus404/xturn) need
  connection counts, byte totals, rate-limit hits, and SSL handshake outcomes
  without instrumenting every transport module by hand. This module centralizes
  emit helpers, default handlers, and coarse health heuristics.

  ## RFCs

  No STUN/TURN RFC; operational telemetry for XTurn socket listeners.
  """

  require Logger

  alias Xirsys.Sockets.Config

  @enabled_key {__MODULE__, :enabled}

  @doc """
  Executes `[:xturn_sockets, event_name]` when telemetry is enabled.

  ## Parameters

    * `event_name` - last segment of the event name (atom)
    * `measurements` - numeric map (`%{bytes: n}`, ...)
    * `metadata` - context map (`%{ip: tuple, port: n}`, ...)

      iex> Xirsys.Sockets.Telemetry.emit(:doctest_event, %{count: 1}, %{})
      :ok
  """
  @spec emit(atom(), map(), map()) :: :ok
  def emit(event_name, measurements, metadata) do
    if enabled?() do
      :telemetry.execute([:xturn_sockets, event_name], measurements, metadata)
    end

    :ok
  end

  @doc false
  @spec enabled?() :: boolean()
  def enabled?() do
    :persistent_term.get(@enabled_key)
  rescue
    ArgumentError ->
      value = Config.get(:telemetry_enabled, true)
      :persistent_term.put(@enabled_key, value)
      value
  end

  @doc false
  @spec refresh_enabled!() :: :ok
  def refresh_enabled!() do
    :persistent_term.put(@enabled_key, Config.get(:telemetry_enabled, true))
    :ok
  end

  @doc """
  Attaches library log and counter handlers for `[:xturn_sockets, ...]` events.

  Idempotent per handler id. Safe to call from a host application's start.
  """
  def attach_handlers() do
    handlers = [
      # Connection events
      {[:xturn_sockets, :socket_opened], &handle_socket_opened/4},
      {[:xturn_sockets, :socket_closed], &handle_socket_closed/4},
      {[:xturn_sockets, :connection_accepted], &handle_connection_accepted/4},

      # Data transfer events
      {[:xturn_sockets, :message_sent], &handle_message_sent/4},
      {[:xturn_sockets, :udp_packet_processed], &handle_packet_processed/4},

      # Rate limiting events
      {[:xturn_sockets, :rate_limit_exceeded], &handle_rate_limit/4},
      {[:xturn_sockets, :tcp_connection_limit_exceeded], &handle_connection_limit/4},

      # SSL/TLS events
      {[:xturn_sockets, :ssl_handshake_success], &handle_ssl_handshake/4},
      {[:xturn_sockets, :ssl_handshake_error], &handle_ssl_error/4},
      {[:xturn_sockets, :ssl_handshake_timeout], &handle_ssl_timeout/4},

      # Error events
      {[:xturn_sockets, :send_error], &handle_send_error/4},
      {[:xturn_sockets, :socket_error], &handle_socket_error/4},

      # Cleanup events
      {[:xturn_sockets, :connections_cleaned], &handle_cleanup/4},

      # Listener events
      {[:xturn_sockets, :udp_listener_started], &handle_listener_started/4},
      {[:xturn_sockets, :tcp_listener_started], &handle_listener_started/4},
      {[:xturn_sockets, :sctp_listener_started], &handle_listener_started/4},

      # SCTP-specific events
      {[:xturn_sockets, :sctp_connection_established], &handle_sctp_connection/4},
      {[:xturn_sockets, :sctp_connection_closed], &handle_sctp_connection/4},
      {[:xturn_sockets, :sctp_message_received], &handle_sctp_message/4},
      {[:xturn_sockets, :sctp_packet_too_large], &handle_sctp_packet_large/4},
      {[:xturn_sockets, :sctp_rate_limit_exceeded], &handle_rate_limit/4},
      {[:xturn_sockets, :sctp_connection_limit_exceeded], &handle_connection_limit/4},
      {[:xturn_sockets, :sctp_connections_cleaned], &handle_cleanup/4}
    ]

    Enum.each(handlers, fn {event, handler} ->
      :telemetry.attach(
        "xturn_sockets_#{Enum.join(event, "_")}",
        event,
        handler,
        %{}
      )
    end)

    Logger.info("XTurn Sockets telemetry handlers attached")
  end

  @doc """
  Detaches every handler whose id starts with `"xturn_sockets_"`.
  """
  def detach_handlers() do
    :telemetry.list_handlers([])
    |> Enum.filter(&String.starts_with?(&1.id, "xturn_sockets_"))
    |> Enum.each(&:telemetry.detach(&1.id))

    Logger.info("XTurn Sockets telemetry handlers detached")
  end

  @doc """
  Snapshot of process-local counters kept in `:persistent_term`.

  Keys include connection counts, bytes sent/received, rate-limit hits, and
  SSL handshake totals. `last_updated` is `System.system_time(:second)`.

  Takes no parameters; counters are global to the VM via `:persistent_term`.
  """
  def get_metrics() do
    %{
      total_connections: get_counter(:total_connections),
      active_udp_connections: get_counter(:active_udp_connections),
      active_tcp_connections: get_counter(:active_tcp_connections),
      active_sctp_connections: get_counter(:sctp_connections),
      messages_sent: get_counter(:messages_sent),
      packets_processed: get_counter(:packets_processed),
      bytes_sent: get_counter(:bytes_sent),
      bytes_received: get_counter(:bytes_received),
      rate_limit_hits: get_counter(:rate_limit_hits),
      ssl_handshake_successes: get_counter(:ssl_handshake_successes),
      ssl_handshake_errors: get_counter(:ssl_handshake_errors),
      send_errors: get_counter(:send_errors),
      # SCTP-specific metrics
      sctp_messages_received: get_counter(:sctp_messages_received),
      sctp_bytes_received: get_counter(:sctp_bytes_received),
      sctp_multistream_messages: get_counter(:sctp_multistream_messages),
      sctp_oversized_packets: get_counter(:sctp_oversized_packets),
      last_updated: System.system_time(:second)
    }
  end

  # Event Handlers

  defp handle_socket_opened(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    increment_counter(:total_connections)
    increment_counter(:"#{protocol}_sockets_opened")

    Logger.debug("Socket opened: #{protocol}", metadata)
  end

  defp handle_socket_closed(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    increment_counter(:"#{protocol}_sockets_closed")

    Logger.debug("Socket closed: #{protocol}", metadata)
  end

  defp handle_connection_accepted(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    increment_counter(:connections_accepted)
    increment_counter(:"#{protocol}_connections_accepted")

    Logger.debug("Connection accepted: #{protocol}", metadata)
  end

  defp handle_message_sent(_name, measurements, metadata, _config) do
    bytes = Map.get(measurements, :bytes, 0)
    protocol = Map.get(measurements, :protocol, :unknown)

    increment_counter(:messages_sent)
    increment_counter(:bytes_sent, bytes)
    increment_counter(:"#{protocol}_bytes_sent", bytes)

    # Track high bandwidth usage
    # > 1MB
    if bytes > 1024 * 1024 do
      Logger.debug("Large message sent: #{bytes} bytes via #{protocol}", metadata)
    end
  end

  defp handle_packet_processed(_name, measurements, metadata, _config) do
    bytes = Map.get(measurements, :bytes, 0)
    increment_counter(:packets_processed)
    increment_counter(:bytes_received, bytes)

    Logger.debug("UDP packet processed: #{bytes} bytes", metadata)
  end

  defp handle_rate_limit(_name, _measurements, metadata, _config) do
    increment_counter(:rate_limit_hits)

    ip = Map.get(metadata, :ip, "unknown")
    Logger.info("Rate limit exceeded for IP: #{inspect(ip)}")
  end

  defp handle_connection_limit(_name, measurements, metadata, _config) do
    count = Map.get(measurements, :count, 0)
    increment_counter(:connection_limit_hits)

    Logger.warning("Connection limit exceeded: #{count} connections", metadata)
  end

  defp handle_ssl_handshake(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    increment_counter(:ssl_handshake_successes)
    increment_counter(:"#{protocol}_handshake_successes")

    Logger.debug("SSL handshake successful: #{protocol}", metadata)
  end

  defp handle_ssl_error(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    error = Map.get(measurements, :error, :unknown)
    increment_counter(:ssl_handshake_errors)

    Logger.warning("SSL handshake failed: #{protocol} - #{inspect(error)}", metadata)
  end

  defp handle_ssl_timeout(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    increment_counter(:ssl_handshake_timeouts)

    Logger.warning("SSL handshake timeout: #{protocol}", metadata)
  end

  defp handle_send_error(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    error = Map.get(measurements, :error, :unknown)
    increment_counter(:send_errors)
    increment_counter(:"#{protocol}_send_errors")

    Logger.warning("Send error: #{protocol} - #{inspect(error)}", metadata)
  end

  defp handle_socket_error(_name, measurements, metadata, _config) do
    protocol = Map.get(measurements, :protocol, :unknown)
    error = Map.get(measurements, :error, :unknown)
    increment_counter(:socket_errors)

    Logger.error("Socket error: #{protocol} - #{inspect(error)}", metadata)
  end

  defp handle_cleanup(_name, measurements, metadata, _config) do
    count = Map.get(measurements, :count, 0)
    increment_counter(:connections_cleaned, count)

    Logger.debug("Cleaned up #{count} stale connections", metadata)
  end

  defp handle_listener_started(_name, _measurements, metadata, _config) do
    ip = Map.get(metadata, :ip, "unknown")
    port = Map.get(metadata, :port, "unknown")
    ssl = Map.get(metadata, :ssl, false)

    listener_type = if ssl, do: "secure", else: "plain"
    Logger.info("#{listener_type} listener started at #{inspect(ip)}:#{port}")

    increment_counter(:listeners_started)
  end

  # SCTP-specific event handlers

  defp handle_sctp_connection(_name, _measurements, metadata, _config) do
    ip = Map.get(metadata, :ip, "unknown")
    increment_counter(:sctp_connections)

    Logger.debug("SCTP connection event for IP: #{inspect(ip)}")
  end

  defp handle_sctp_message(_name, measurements, metadata, _config) do
    bytes = Map.get(measurements, :bytes, 0)
    stream_id = Map.get(measurements, :stream_id, 0)
    ppid = Map.get(measurements, :ppid, 0)

    increment_counter(:sctp_messages_received)
    increment_counter(:sctp_bytes_received, bytes)

    # Track multi-stream usage
    if stream_id > 0 do
      increment_counter(:sctp_multistream_messages)
    end

    Logger.debug("SCTP message: #{bytes} bytes, stream #{stream_id}, ppid #{ppid}", metadata)
  end

  defp handle_sctp_packet_large(_name, measurements, metadata, _config) do
    bytes = Map.get(measurements, :bytes, 0)
    ip = Map.get(metadata, :ip, "unknown")
    port = Map.get(metadata, :port, "unknown")

    increment_counter(:sctp_oversized_packets)

    Logger.warning("SCTP packet too large: #{bytes} bytes from #{inspect(ip)}:#{port}")
  end

  # Counter management using persistent_term for performance

  defp increment_counter(key, amount \\ 1) do
    :atomics.add(counter_ref(key), 1, amount)
  end

  defp get_counter(key) do
    :atomics.get(counter_ref(key), 1)
  rescue
    ArgumentError -> 0
  end

  defp counter_ref(key) do
    counter_key = {__MODULE__, :counter, key}

    case :persistent_term.get(counter_key, nil) do
      nil ->
        ref = :atomics.new(1, signed: false)
        :persistent_term.put(counter_key, ref)
        ref

      ref ->
        ref
    end
  end

  @doc """
  Erases this module's `:persistent_term` counters.

      iex> Xirsys.Sockets.Telemetry.reset_counters()
      :ok
  """
  def reset_counters() do
    :persistent_term.erase(@enabled_key)
    :ok
  end

  @doc """
  Coarse health atom from current counters.

  Returns `:healthy`, `:degraded` (SSL error rate), `:unhealthy` (send error
  rate), or `:under_attack` (rate-limit hits).

  Takes no parameters; reads counters via `get_metrics/0`.
  """
  def health_status() do
    metrics = get_metrics()

    error_rate = safe_divide(metrics.send_errors, metrics.messages_sent)
    ssl_error_rate = safe_divide(metrics.ssl_handshake_errors, metrics.ssl_handshake_successes)

    cond do
      # > 10% error rate
      error_rate > 0.1 -> :unhealthy
      # > 20% SSL error rate
      ssl_error_rate > 0.2 -> :degraded
      # High rate limiting
      metrics.rate_limit_hits > 100 -> :under_attack
      true -> :healthy
    end
  end

  defp safe_divide(_numerator, 0), do: 0.0
  defp safe_divide(numerator, denominator), do: numerator / denominator
end
