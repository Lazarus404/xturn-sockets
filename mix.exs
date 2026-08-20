defmodule Xirsys.XTurn.Sockets.MixProject do
  use Mix.Project

  def project do
    [
      app: :xturn_sockets,
      version: "2.0.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      build_embedded: Mix.env() == :prod,
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Format-agnostic socket library with pluggable packet framing, a reusable drain engine, and multi-protocol transport support.",
      source_url: "https://github.com/Lazarus404/xturn-sockets",
      homepage_url: "https://xturn.me",
      package: package(),
      docs: [
        extras: ["README.md", "LICENSE.md"],
        main: "readme"
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger, :ssl]
    ]
  end

  defp deps do
    [
      {:telemetry, "~> 1.0"},
      {:benchee, "~> 1.3", only: :dev},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    %{
      files: ["lib", "mix.exs", "README.md", "LICENSE.md", "config"],
      maintainers: ["Jahred Love"],
      licenses: ["Apache-2.0"],
      links: %{"Github" => "https://github.com/Lazarus404/xturn-sockets"}
    }
  end
end
