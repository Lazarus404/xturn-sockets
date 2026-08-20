defmodule XturnSockets.EnginePipelineTest do
  use ExUnit.Case, async: true

  alias Xirsys.Sockets.{Accumulator.Raw, Conn, Engine, Pipeline}
  alias XturnSockets.PipelineSupport.{
    AsyncTwoTierPipeline,
    ClosePipeline,
    CrashPipeline,
    PoisonPipeline,
    RootOnlyPipeline,
    ThreeTierPipeline,
    TwoTierPipeline
  }

  @conn %Conn{
    client_ip: {127, 0, 0, 1},
    client_port: 40_000,
    assigns: %{agent: nil}
  }

  setup do
    {:ok, agent} = Agent.start_link(fn -> [] end)
    on_exit(fn -> if Process.alive?(agent), do: Agent.stop(agent) end)
    {:ok, agent: agent, conn: %{@conn | assigns: %{agent: agent}}}
  end

  test "root-only pipeline drains like single-tier", %{conn: conn, agent: agent} do
    pipeline = Pipeline.resolve(RootOnlyPipeline)
    {accs, states} = Pipeline.fresh_session(pipeline, [])

    {accs, states, _sessions, :ok} =
      push(pipeline, "one", conn, accs, states)

    {accs, states, _sessions, :ok} =
      push(pipeline, "two", conn, accs, states)

    assert Agent.get(agent, & &1) |> Enum.reverse() == ["one", "two"]
    assert map_size(accs) == 1
    assert map_size(states) == 1
  end

  test "two-tier descend delivers to inner handler", %{conn: conn, agent: agent} do
    pipeline = Pipeline.resolve(TwoTierPipeline)
    {accs, states} = Pipeline.fresh_session(pipeline, [])

    payload = XturnSockets.TestSupport.frame("inner:hello")

    {accs, states, _sessions, :ok} = push(pipeline, payload, conn, accs, states)

    assert Agent.get(agent, & &1) |> Enum.reverse() == ["hello"]
    assert Map.has_key?(accs, :inner)
    assert Map.has_key?(states, :inner)
  end

  test "inner tier crash is isolated and root keeps draining", %{conn: conn, agent: agent} do
    pipeline = Pipeline.resolve(CrashPipeline)
    {accs, states} = Pipeline.fresh_session(pipeline, [])

    {accs, states, _sessions, :ok} = push(pipeline, "trigger", conn, accs, states)

    assert Agent.get(agent, & &1) == []

    {accs, states, _sessions, :ok} = push(pipeline, "still-root", conn, accs, states)

    assert accs != nil
    assert states != nil
  end

  test "accumulator pop crash resets tier and next descend succeeds", %{conn: conn, agent: agent} do
    ref = :telemetry_test.attach_event_handlers(self(), [[:xturn_sockets, :tier_crashed]])

    pipeline = Pipeline.resolve(PoisonPipeline)
    {accs, states} = Pipeline.fresh_session(pipeline, [])

    {accs, states, _sessions, :ok} = push(pipeline, "poison", conn, accs, states)
    assert Agent.get(agent, & &1) == []

    {accs, _states, _sessions, :ok} = push(pipeline, "good", conn, accs, states)
    assert Agent.get(agent, & &1) == ["good"]
    assert Map.has_key?(accs, :inner)

    assert_receive {[:xturn_sockets, :tier_crashed], ^ref, %{}, metadata}
    assert metadata.tier == :inner
    assert metadata.reset == true
    refute_receive {[:xturn_sockets, :tier_crashed], ^ref, _, _}
  end

  test "async two-tier descend does not add inner tier to owner accs", %{conn: conn, agent: agent} do
    pipeline = Pipeline.resolve(AsyncTwoTierPipeline)
    {accs, states} = Pipeline.fresh_session(pipeline, [])

    payload = XturnSockets.TestSupport.frame("inner:async")

    {accs, _states, sessions, :ok} = push(pipeline, payload, conn, accs, states)

    assert map_size(sessions) == 1
    refute Map.has_key?(accs, :inner)
    assert eventually(fn -> Agent.get(agent, & &1) == ["async"] end)
  end

  test "async two-tier descend delivers via tier session", %{conn: conn, agent: agent} do
    pipeline = Pipeline.resolve(AsyncTwoTierPipeline)
    {accs, states} = Pipeline.fresh_session(pipeline, [])

    payload = XturnSockets.TestSupport.frame("inner:async")

    {_accs, _states, sessions, :ok} = push(pipeline, payload, conn, accs, states)

    assert map_size(sessions) == 1
    assert eventually(fn -> Agent.get(agent, & &1) == ["async"] end)
  end

  test "async two-tier delivers multiple packets in order", %{conn: conn, agent: agent} do
    pipeline = Pipeline.resolve(AsyncTwoTierPipeline)
    {accs, states} = Pipeline.fresh_session(pipeline, [])

    for label <- ["one", "two", "three"] do
      payload = XturnSockets.TestSupport.frame("inner:#{label}")
      push(pipeline, payload, conn, accs, states)
    end

    assert eventually(fn ->
             Agent.get(agent, & &1) |> Enum.reverse() == ["one", "two", "three"]
           end)
  end

  test "three-tier descend chain", %{conn: conn, agent: agent} do
    pipeline = Pipeline.resolve(ThreeTierPipeline)
    {accs, states} = Pipeline.fresh_session(pipeline, [])

    {_accs, _states, _sessions, :ok} =
      push(pipeline, "middle:leaf:payload", conn, accs, states)

    assert Agent.get(agent, & &1) |> Enum.reverse() == ["payload"]
  end

  test "inner tier close propagates to connection action", %{conn: conn} do
    pipeline = Pipeline.resolve(ClosePipeline)
    {accs, states} = Pipeline.fresh_session(pipeline, [])

    {_accs, _states, _sessions, action} = push(pipeline, "trigger", conn, accs, states)

    assert action == :close
  end

  test "legacy sugar resolve works through Engine", %{conn: conn, agent: agent} do
    pipeline = Pipeline.resolve({Raw, XturnSockets.PipelineSupport.InnerCollectHandler})
    {accs, states} = Pipeline.fresh_session(pipeline, [])

    {_accs, _states, _sessions, :ok} = push(pipeline, "legacy", conn, accs, states)

    assert Agent.get(agent, & &1) == ["legacy"]
  end

  defp push(pipeline, chunk, conn, accs, states) do
    Engine.push_and_drain(
      pipeline,
      chunk,
      %{},
      conn,
      accs,
      states,
      %{},
      FakeTransport,
      self()
    )
  end

  defp eventually(fun, attempts \\ 20) do
    if fun.() do
      :ok
    else
      if attempts > 0 do
        Process.sleep(20)
        eventually(fun, attempts - 1)
      else
        flunk("condition not met")
      end
    end
  end

end

defmodule FakeTransport do
  @behaviour Xirsys.Sockets.Transport

  @impl true
  def listen(_ip, _port, _opts), do: {:ok, :fake}
  @impl true
  def accept(_socket, _timeout), do: {:ok, :fake}
  @impl true
  def send(_socket, _data, _to), do: :ok
  @impl true
  def setopts(_socket, _opts), do: :ok
  @impl true
  def peername(_socket), do: {:ok, {{127, 0, 0, 1}, 0}}
  @impl true
  def sockname(_socket), do: {:ok, {{127, 0, 0, 1}, 0}}
  @impl true
  def close(_socket), do: :ok
  @impl true
  def handle_message(_msg, _socket), do: :ignore
end
