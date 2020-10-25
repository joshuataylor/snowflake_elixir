defmodule SnowflakeEx.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      :hackney_pool.child_spec(:s3_pool, timeout: 15000, max_connections: 100),
      :hackney_pool.child_spec(:snowflake_pool, timeout: 180_000, max_connections: 5)
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: SnowflakeEx.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
