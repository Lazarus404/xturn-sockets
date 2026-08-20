defmodule XturnSockets.TLSTest do
  use ExUnit.Case

  alias Xirsys.Sockets.Transport.TLS

  @test_ip {127, 0, 0, 1}

  setup do
    previous_ssl = Application.get_env(:xturn_sockets, :ssl_options)
    previous_certs = Application.get_env(:xturn, :certs)

    on_exit(fn ->
      restore_env(:xturn_sockets, :ssl_options, previous_ssl)
      restore_env(:xturn, :certs, previous_certs)
      File.rm_rf(Path.join(System.tmp_dir!(), "xturn_sockets_tls_test"))
    end)

    :ok
  end

  test "listen without certificates returns error" do
    Application.delete_env(:xturn, :certs)
    assert {:error, :no_certificates_configured} = TLS.listen(@test_ip, 0, [])
  end

  test "listen normalizes charlist TLS versions from ssl_options" do
    {certfile, keyfile} = generate_self_signed_cert!()

    Application.put_env(:xturn_sockets, :ssl_options, [
      versions: [~c"tlsv1.2", ~c"tlsv1.3"],
      verify: :verify_none
    ])

    Application.put_env(:xturn, :certs, certfile: certfile, keyfile: keyfile)

    assert {:ok, sock} = TLS.listen(@test_ip, 0, [])
    :ssl.close(sock)
  end

  defp generate_self_signed_cert! do
    dir = Path.join(System.tmp_dir!(), "xturn_sockets_tls_test")
    File.mkdir_p!(dir)

    certfile = Path.join(dir, "server.crt")
    keyfile = Path.join(dir, "server.key")

    {_, 0} =
      System.cmd("openssl", [
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-keyout",
        keyfile,
        "-out",
        certfile,
        "-days",
        "1",
        "-nodes",
        "-subj",
        "/CN=localhost"
      ])

    {certfile, keyfile}
  end

  defp restore_env(app, key, value) do
    case value do
      nil -> Application.delete_env(app, key)
      value -> Application.put_env(app, key, value)
    end
  end
end
