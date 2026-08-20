defmodule XturnSockets.SCTPUnitTest do
  use ExUnit.Case, async: true

  alias Xirsys.Sockets.Transport.SCTP

  test "module exports transport callbacks" do
    behaviours = SCTP.__info__(:attributes)[:behaviour] || []
    assert Xirsys.Sockets.Transport in behaviours
  end
end
