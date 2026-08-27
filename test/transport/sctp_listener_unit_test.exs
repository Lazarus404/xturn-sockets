defmodule XturnSockets.SCTPListenerUnitTest do
  use ExUnit.Case, async: true

  alias Xirsys.Sockets.Transport.SCTP

  test "handle_message normalizes sctp payload" do
    data = "payload"

    assert {:data, ^data, {{127, 0, 0, 1}, 5000}} =
             SCTP.handle_message({:sctp, :sock, {127, 0, 0, 1}, 5000, [], data}, :sock)
  end
end
