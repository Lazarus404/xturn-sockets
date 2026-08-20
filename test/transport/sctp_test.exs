defmodule XturnSockets.SCTPTest do
  use ExUnit.Case

  alias Xirsys.Sockets.Transport.SCTP

  @test_ip {127, 0, 0, 1}

  @tag :sctp
  test "listen on loopback when SCTP is available" do
    case SCTP.listen(@test_ip, 0, []) do
      {:ok, sock} ->
        SCTP.close(sock)
        assert true

      {:error, :sctp_not_supported} ->
        assert true

      {:error, reason} ->
        flunk("unexpected SCTP listen error: #{inspect(reason)}")
    end
  end
end
