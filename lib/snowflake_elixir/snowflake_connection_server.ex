defmodule SnowflakeEx.SnowflakeConnectionServer do
  @moduledoc ~S"""
  Connection server for interacting from DBConnection to Snowflake. A GenServer is used to limit the amount
  of queries that are issued to Snowflake at one time, to prevent it from being overwhelmed.

  Keep an eye on the warehouses page in Snowflake to find a concurrency limit that works well for you, and then
  tweak as required.

  The pool size for HTTPoison for snowflake_pool is set to 5 by default.
  """
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def stop(pid) do
    GenServer.stop(pid, :normal)
  end

  def query(pid, statement) do
    GenServer.call(pid, {:select_query, statement}, 10_000_000)
  end

  def query(pid, statement, [], _opts) do
    query(pid, statement)
  end

  def query(pid, statement, params, opts) do
    if Keyword.get(opts, :field_types) do
      GenServer.call(pid, {:opts_query, statement, params, opts}, 10_000_000)
    else
      GenServer.call(pid, {:select_query, statement, params, opts}, 10_000_000)
    end
  end

  # Server

  @impl true
  def init(opts) do
    host = Keyword.get(opts, :host, nil)
    account_name = Keyword.get(opts, :account_name, nil)
    username = Keyword.get(opts, :username, nil)
    password = Keyword.get(opts, :password, nil)
    database = Keyword.get(opts, :database, nil)
    warehouse = Keyword.get(opts, :warehouse, nil)
    schema = Keyword.get(opts, :schema, nil)

    result =
      SnowflakeEx.HTTPClient.login(
        host,
        account_name,
        warehouse,
        database,
        schema,
        username,
        password,
        false
      )

    case result do
      {:ok, %{token: token, session_id: session_id}} -> {:ok, token: token, opts: opts, session_id: session_id}
      {:error, reason} -> {:stop, reason}
    end
  end

  #  @impl true
  #  def handle_call({:select_query, statement, params, fopts}, from, state) do
  #    connect_opts = Keyword.get(state, :opts)
  #    host = Keyword.get(connect_opts, :host)
  #    token = Keyword.get(opts, :token)
  #
  #    {:reply, SnowflakeEx.HTTPClient.query(host, token, statement, connect_opts), opts}
  #  end
  #
  @impl true
  def handle_call({:select_query, statement}, _state, opts) do
    connect_opts = Keyword.get(opts, :opts)
    host = Keyword.get(connect_opts, :host)
    token = Keyword.get(opts, :token)

    {:reply, SnowflakeEx.HTTPClient.query(host, token, statement, connect_opts), opts}
  end

  @impl true
  def handle_call({:opts_query, statement, params, fopts}, _state, opts) when is_list(params) do
    connect_opts =
      Keyword.get(opts, :opts)
      |> Keyword.put(:field_types, Keyword.get(fopts, :field_types))

    host = Keyword.get(connect_opts, :host)
    token = Keyword.get(opts, :token)

    {:reply, SnowflakeEx.HTTPClient.insert(host, token, statement, params, connect_opts), opts}
  end

  @impl true
  def handle_call(:data, _state, opts) do
    {:reply, opts, opts}
  end
end
