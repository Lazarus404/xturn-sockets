defmodule XturnSockets.ConfigTest do
  use ExUnit.Case, async: false

  alias Xirsys.Sockets.Config

  setup do
    original_host = Application.get_env(:xturn_sockets, :config_app)
    original_lib = Application.get_env(:xturn_sockets, :buffer_size)
    original_host_buffer = Application.get_env(:xturn_sockets_test_host, :buffer_size)

    on_exit(fn ->
      restore_env(:xturn_sockets, :config_app, original_host)
      restore_env(:xturn_sockets, :buffer_size, original_lib)
      restore_env(:xturn_sockets_test_host, :buffer_size, original_host_buffer)
    end)

    :ok
  end

  test "get/2 prefers host application config over library config" do
    Application.put_env(:xturn_sockets, :config_app, :xturn_sockets_test_host)
    Application.put_env(:xturn_sockets, :buffer_size, 256 * 1024)
    Application.put_env(:xturn_sockets_test_host, :buffer_size, 128 * 1024)

    assert Config.get(:buffer_size, 0) == 128 * 1024
  end

  test "get/2 falls back to library config when host has no value" do
    Application.put_env(:xturn_sockets, :config_app, :xturn_sockets_test_host)
    Application.put_env(:xturn_sockets, :buffer_size, 256 * 1024)
    Application.delete_env(:xturn_sockets_test_host, :buffer_size)

    assert Config.get(:buffer_size, 0) == 256 * 1024
  end

  test "get/2 falls back to default when neither host nor library set the key" do
    Application.delete_env(:xturn_sockets, :config_app)
    Application.delete_env(:xturn_sockets, :buffer_size)

    assert Config.get(:buffer_size, 64 * 1024) == 64 * 1024
  end

  test "reorder_opts/2 merges host tier config over library tier config" do
    Application.put_env(:xturn_sockets, :config_app, :xturn_sockets_test_host)
    Application.put_env(:xturn_sockets, :reorder, rtp: [window: 32, max_delay_ms: 250])
    Application.put_env(:xturn_sockets_test_host, :reorder, rtp: [window: 48])

    assert Config.reorder_opts(:rtp, [])[:window] == 48
    assert Config.reorder_opts(:rtp, [])[:max_delay_ms] == 250
  end

  defp restore_env(app, key, value) do
    if value do
      Application.put_env(app, key, value)
    else
      Application.delete_env(app, key)
    end
  end
end
