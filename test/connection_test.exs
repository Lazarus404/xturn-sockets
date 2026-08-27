defmodule XturnSockets.ConnectionTest do
  use ExUnit.Case, async: true

  alias Xirsys.Sockets.SockSupervisor

  defmodule StubTransport do
    @behaviour Xirsys.Sockets.Transport

    def listen(_ip, _port, _opts), do: {:ok, :listen}
    def accept(_sock, _timeout), do: {:error, :connectionless}
    def send(_sock, _data, _from), do: :ok
    def setopts(_sock, _opts), do: :ok
    def sockname(_sock), do: {:ok, {{127, 0, 0, 1}, 1}}
    def peername(_sock), do: {:ok, {{127, 0, 0, 1}, 2}}
    def close(_sock), do: :ok
    def framing(), do: :stream

    def handle_message(:icmp, _sock),
      do: {:icmp, %{type: 3, code: 4, error_data: 1280, peer: {{8, 8, 8, 8}, 3478}}}

    def handle_message(:weird, _sock), do: {:other, :not_in_contract}
    def handle_message(_msg, _sock), do: :ignore
  end

  defmodule NoopHandler do
    @behaviour Xirsys.Sockets.Handler

    @impl true
    def handle_packet(_packet, _meta, _conn, state), do: {:ok, state}
  end

  test "ignores icmp and unknown handle_message tags without crashing" do
    assert {:ok, pid} =
             SockSupervisor.start_connection(
               transport: StubTransport,
               socket: :sock,
               handler: NoopHandler,
               accumulator: Xirsys.Sockets.Accumulator.Raw
             )

    send(pid, :icmp)
    send(pid, :weird)
    send(pid, :ignore_me)
    assert Process.alive?(pid)
    Process.exit(pid, :kill)
  end
end
