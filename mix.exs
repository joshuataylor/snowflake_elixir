defmodule SnowflakeEx.MixProject do
  use Mix.Project

  def project do
    [
      app: :snowflake_elixir,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {SnowflakeEx.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:db_connection, "~> 2.2"},
      {:db_connection, "~> 2.2"},
      {:ecto_sql, "~> 3.5"},
      {:jason, "~> 1.2"},
      {:hackney, "~> 1.16"},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:plug, "~> 1.10"},
      {:httpoison, "~> 1.7"},
      {:uuid, "~> 1.1"},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
    ]
  end
end
