defmodule SnowflakeEx.MixProject do
  use Mix.Project

  @source_url "https://github.com/joshuataylor/snowflake_elixir"

  def project do
    [
      app: :snowflake_elixir,
      version: "0.1.0",
      elixir: "~> 1.10",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      description: "Snowflake driver written in pure Elixir, using db_connection",
      package: package(),
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {SnowflakeEx.Application, []}
    ]
  end

  defp deps do
    [
      {:db_connection, "~> 2.2"},
      {:ecto_sql, "~> 3.7"},
      {:jason, "~> 1.2"},
      {:hackney, "~> 1.16"},
      {:mix_test_watch, "~> 1.0", only: :dev, runtime: false},
      {:plug, "~> 1.10"},
      {:httpoison, "~> 1.7"},
      {:uuid, "~> 1.1"},
      {:credo, "~> 1.5", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.0", only: [:dev], runtime: false},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:table, "~> 0.1.1", optional: true}
    ]
  end

  defp package do
    [
      maintainers: ["Josh Taylor"],
      licenses: ["MIT"],
      links: %{
        GitHub: @source_url
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      extras: [
        "CHANGELOG.md",
        "README.md"
      ]
    ]
  end
end
