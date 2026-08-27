defmodule XturnSockets.DoctestTest do
  use ExUnit.Case, async: false

  doctest Xirsys.Sockets.Config
  doctest Xirsys.Sockets.Spec
  doctest Xirsys.Sockets.Conn
  doctest Xirsys.Sockets.Pipeline
  doctest Xirsys.Sockets.Pipeline.Tier
  doctest Xirsys.Sockets.Accumulator.Raw
  doctest Xirsys.Sockets.Accumulator.LengthPrefixed
  doctest Xirsys.Sockets.Accumulator.Reorder
  doctest Xirsys.Sockets.Transport.TCP
  doctest Xirsys.Sockets.Transport.UDP
  doctest Xirsys.Sockets.Transport.TLS
  doctest Xirsys.Sockets.Transport.DTLS
  doctest Xirsys.Sockets.Transport.SCTP
  doctest Xirsys.Sockets.Telemetry
end
