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

defmodule Xirsys.Sockets.Transport.TLS do
  @moduledoc false
  @behaviour Xirsys.Sockets.Transport

  alias Xirsys.Sockets.{Config, Telemetry}

  @listen_opts [
    reuseaddr: true,
    active: false
  ]

  @tcp_listen_opts [
    keepalive: true,
    backlog: 100,
    nodelay: true
  ]

  @secure_defaults [
    versions: [:"tlsv1.2", :"tlsv1.3"],
    secure_renegotiate: true,
    reuse_sessions: false,
    honor_cipher_order: true,
    fail_if_no_peer_cert: false,
    verify: :verify_none,
    depth: 2
  ]

  @dtls_versions [:"dtlsv1.2"]

  @impl true
  def listen(ip, port, opts) do
    with {:ok, cert_opts} <- cert_options(),
         listen_opts <- build_listen_opts(ip, opts ++ cert_opts) do
      case :ssl.listen(port, listen_opts) do
        {:ok, _sock} = ok ->
          Telemetry.emit(:tls_listener_created, %{}, %{ip: ip, port: port})
          ok

        {:error, _} = error ->
          error
      end
    end
  end

  @impl true
  def accept(listen_sock, timeout) do
    ssl_timeout = Config.ssl_handshake_timeout()
    accept_timeout = if timeout == :infinity, do: ssl_timeout, else: timeout

    with {:ok, transport} <- :ssl.transport_accept(listen_sock, accept_timeout),
         {:ok, cli} <- :ssl.handshake(transport, ssl_timeout) do
      Telemetry.emit(:ssl_handshake_success, %{protocol: :tls}, %{})
      {:ok, cli}
    else
      {:error, :timeout} = error ->
        Telemetry.emit(:ssl_handshake_timeout, %{protocol: :tls}, %{})
        error

      {:error, reason} = error ->
        Telemetry.emit(:ssl_handshake_error, %{protocol: :tls, error: reason}, %{})
        error
    end
  end

  @impl true
  def send(socket, data, _to), do: :ssl.send(socket, data)

  @impl true
  def setopts(socket, opts), do: :ssl.setopts(socket, opts)

  @impl true
  def sockname(socket), do: :ssl.sockname(socket)

  @impl true
  def peername(socket), do: :ssl.peername(socket)

  @impl true
  def close(socket) do
    :ssl.close(socket)
    Telemetry.emit(:socket_closed, %{protocol: :tls}, %{})
    :ok
  end

  @impl true
  def controlling_process(socket, pid), do: :ssl.controlling_process(socket, pid)

  @impl true
  def framing(), do: :stream

  @impl true
  def handle_message({:ssl, _port, data}, _socket), do: {:data, data, nil}
  def handle_message({:ssl_closed, _}, _socket), do: {:closed, :normal}
  def handle_message({:ssl_error, _, reason}, _socket), do: {:closed, reason}
  def handle_message({:ssl_passive, _}, socket) do
    :ssl.setopts(socket, active: :once)
    :ignore
  end

  def handle_message(_, _socket), do: :ignore

  defp build_listen_opts(ip, opts) do
    opts = normalize_ssl_options(opts)
    protocol = Keyword.get(opts, :protocol, :tls)

    secure =
      Config.get(:ssl_options, @secure_defaults)
      |> normalize_ssl_options()
      |> apply_protocol_versions(protocol)

    @listen_opts
    |> maybe_merge_tcp_opts(protocol)
    |> Keyword.merge(buffer_opts())
    |> Keyword.merge(secure)
    |> Keyword.merge(opts)
    |> Keyword.put(:ip, ip)
  end

  defp maybe_merge_tcp_opts(opts, :dtls), do: opts
  defp maybe_merge_tcp_opts(opts, _protocol), do: Keyword.merge(opts, @tcp_listen_opts)

  defp apply_protocol_versions(secure, :dtls) do
    Keyword.put(secure, :versions, @dtls_versions)
  end

  defp apply_protocol_versions(secure, _protocol), do: secure

  defp normalize_ssl_options(opts) when is_list(opts) do
    opts
    |> Enum.map(&normalize_ssl_opt/1)
    |> Keyword.new()
  end

  defp normalize_ssl_opt({key, value}) when is_atom(key) do
    {key, normalize_ssl_value(key, value)}
  end

  defp normalize_ssl_opt({key, value}) when is_list(key) do
    {List.to_atom(key), normalize_ssl_value(List.to_atom(key), value)}
  end

  defp normalize_ssl_value(:versions, versions) when is_list(versions) do
    Enum.map(versions, &normalize_tls_version/1)
  end

  defp normalize_ssl_value(_key, value), do: value

  defp normalize_tls_version(version) when is_atom(version), do: version

  defp normalize_tls_version(version) when is_list(version) do
    version |> List.to_atom()
  end

  defp normalize_tls_version(version) when is_binary(version) do
    String.to_atom(version)
  end

  defp buffer_opts do
    size = Config.buffer_size()
    [buffer: size, recbuf: size, sndbuf: size]
  end

  defp cert_options do
    certs =
      Application.get_env(:certs, :certs) ||
        Application.get_env(:xturn, :certs)

    case certs do
      list when is_list(list) -> {:ok, normalize_ssl_options(list)}
      _ -> {:error, :no_certificates_configured}
    end
  end
end
