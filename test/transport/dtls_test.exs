defmodule XturnSockets.DTLSTest do
  use ExUnit.Case

  alias Xirsys.Sockets.Transport.DTLS

  @test_ip {127, 0, 0, 1}

  setup do
    previous_ssl = Application.get_env(:xturn_sockets, :ssl_options)
    previous_certs = Application.get_env(:xturn, :certs)

    on_exit(fn ->
      restore_env(:xturn_sockets, :ssl_options, previous_ssl)
      restore_env(:xturn, :certs, previous_certs)
      File.rm_rf(Path.join(System.tmp_dir!(), "xturn_sockets_dtls_test"))
    end)

    :ok
  end

  test "listen without certificates returns error" do
    Application.delete_env(:xturn, :certs)
    Application.delete_env(:certs, :certs)
    assert {:error, :no_certificates_configured} = DTLS.listen(@test_ip, 0, [])
  end

  test "listen with certs only in opts does not require application env" do
    {certfile, keyfile} = generate_self_signed_cert!()

    Application.delete_env(:xturn, :certs)
    Application.delete_env(:certs, :certs)

    assert {:ok, sock} =
             DTLS.listen(@test_ip, 0, certfile: certfile, keyfile: keyfile, verify: :verify_none)

    :ssl.close(sock)
  end

  test "listen uses DTLS versions even when ssl_options specifies TLS versions" do
    {certfile, keyfile} = generate_self_signed_cert!()

    Application.put_env(:xturn_sockets, :ssl_options,
      versions: [~c"tlsv1.2", ~c"tlsv1.3"],
      verify: :verify_none
    )

    Application.put_env(:xturn, :certs, certfile: certfile, keyfile: keyfile)

    assert {:ok, sock} = DTLS.listen(@test_ip, 0, [])
    :ssl.close(sock)
  end

  defp generate_self_signed_cert! do
    dir = Path.join(System.tmp_dir!(), "xturn_sockets_dtls_test")
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
