defmodule SnowflakeEx.Protocol do
  require Logger
  use DBConnection
  alias DBConnection.ConnectionError
  alias SnowflakeEx.SnowflakeConnectionServer

  defstruct pid: nil, snowflake: :idle, opts: nil, session_id: nil

  @impl true
  def connect(opts) do
    {:ok, pid} = SnowflakeConnectionServer.start_link(opts)

    d = GenServer.call(pid, :data)

    s = %__MODULE__{
      snowflake: :idle,
      pid: pid,
      opts: opts,
      session_id: Keyword.get(d, :session_id)
    }

    {:ok, s}
  end

  @impl true
  def disconnect(_err, _state) do
    :ok
  end

  @impl true
  def checkout(state), do: {:ok, state}

  @impl true
  def ping(state), do: {:ok, state}

  @impl true
  def checkin(state), do: {:ok, state}

  @impl true
  def handle_execute(%SnowflakeEx.Query{statement: statement} = query, params, opts, state)
      when is_binary(statement) do
    case SnowflakeConnectionServer.query(state.pid, statement, params, opts) do
      {:ok, %SnowflakeEx.Result{} = result} ->
        {:ok, query, result, state}

      {:error, e} ->
        {:error, %SnowflakeEx.Error{message: e}, %{state | snowflake: :idle}}
    end
  end

  @impl true
  def handle_execute(%SnowflakeEx.Query{statement: statement} = query, params, opts, state) do
    statement = IO.iodata_to_binary(statement)
    # hack to fix migrations, @todo fix
    statement = if String.contains?(String.downcase(statement), "CREATE TABLE IF NOT EXISTS") do
      schema = Keyword.get(state.opts, :schema)
      schema = Keyword.get(state.opts, :database)
      "#{statement}"
    else
      statement
    end

    case SnowflakeConnectionServer.query(state.pid, statement, params, opts) do
      {:ok, %SnowflakeEx.Result{} = result} ->
        {:ok, query, result, state}

      {:error, e} ->
        {:disconnect, %ConnectionError{message: e}, state}
    end
  end

  @impl true
  def handle_prepare(query, _opts, state), do: {:ok, query, state}

  @impl true
  def handle_close(query, _opts, state), do: {:ok, query, state}

  # Not implemented

  @impl true
  def handle_declare(_query, _params, _opts, _state) do
    raise("Not implemented handle_declare")
  end

  @impl true
  def handle_deallocate(_query, _cursor, _opts, _state) do
    raise("Not implemented handle_deallocate")
  end

  @impl true
  def handle_begin(opts, %{snowflake: snowflake} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when snowflake == :idle ->
        statement = "BEGIN name foo"
        handle_transaction(statement, opts, s, :transaction)

      mode when mode in [:transaction] ->
        {snowflake, s}
    end
  end

  @impl true
  def handle_commit(opts, %{snowflake: snowflake} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when snowflake == :transaction ->
        statement = "COMMIT"
        handle_transaction(statement, opts, s, :idle)

      mode when mode in [:transaction] ->
        {snowflake, s}
    end
  end

  @impl true
  def handle_rollback(opts, %{snowflake: snowflake} = s) do
    case Keyword.get(opts, :mode, :transaction) do
      :transaction when snowflake in [:transaction, :error] ->
        statement = "ROLLBACK"
        handle_transaction(statement, opts, s, :idle)

      mode when mode in [:transaction] ->
        {snowflake, s}
    end
  end

  @impl true
  def handle_fetch(_query, _cursor, _opts, _state) do
    raise("Not implemented handle_fetch")
  end

  @impl true
  def handle_status(_opts, _state) do
    raise("Not implemented handle_status")
  end

  defp handle_transaction(query, statement, state, mode) do
    case SnowflakeEx.SnowflakeConnectionServer.query(state.pid, query) do
      {:ok, %SnowflakeEx.Result{} = result} -> {:ok, result, %{state | snowflake: :transaction}}

      {:error, foo} ->
        message = "Query execution error"
        exception = %ConnectionError{message: message}
        {:disconnect, exception, state}
    end
  end
end
