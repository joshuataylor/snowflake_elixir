defmodule SnowflakeEx do
  @moduledoc """
  Snowflake driver for Elixir.

  It uses the Snowflake REST API to communicate with Snowflake, with an earlier version set for JSON.
  There isn't an Elixir Arrow library (yet!), so it seems that setting an earlier Java version seems
  to give us back JSON results.

  Right now the library doesn't support MFA, so you'll need to either use private key auth
  (https://docs.snowflake.com/en/user-guide/odbc-parameters.html#using-key-pair-authentication) or connecting using a
  username & password. A private key auth is highly recommended as you can rotate passwords easier. MFA with Snowflake
  is a mess.

  This module handles the connection to Snowflake, providing support for queries, transactions, connection backoff,
  logging,

  A Genserver logs into Snowflake for you, then uses this session for all other interactions (queries, etc) and ensuring
  we don't overload the Snowflake server with requests. If you issue too many requests to Snowflake, your
  query might be queued.
  """

  @typedoc """
  A connection process name, pid or reference.

  A connection reference is used when making multiple requests to the same
  connection, see `transaction/3`.
  """
  @type conn :: DBConnection.conn()

  @type start_option ::
          {:host, String.t()}
          | {:database, String.t()}
          | {:username, String.t()}
          | {:password, String.t()}
          | {:warehouse, String.t()}
          | {:parameters, keyword}
          | {:schema, String.t()}
          | {:account_name, String.t()}
          | {:timeout, timeout}
          | {:connect_timeout, timeout}
          | {:prepare, :named | :unnamed}
          | {:transactions, :strict | :naive}
          | {:types, module}
          | {:disconnect_on_error_codes, [atom]}
          | DBConnection.start_option()

  @type option ::
          {:mode, :transaction}
          | DBConnection.option()

  @type execute_option ::
          {:decode_mapper, (list -> term)}
          | option

  def child_spec(opts) do
    DBConnection.child_spec(SnowflakeEx.Protocol, opts)
  end

  def prepare_execute(conn, name, statement, params, opts \\ []) do
    query = %SnowflakeEx.Query{name: name, statement: statement}
    DBConnection.prepare_execute(conn, query, params, opts)
  end

  def prepare_execute!(conn, name, statement, params, opts \\ []) do
    query = %SnowflakeEx.Query{name: name, statement: statement}
    DBConnection.prepare_execute!(conn, query, params, opts)
  end

  @doc """
  Executes a prepared query.

  ## Options

  Options are passed to `DBConnection.execute/4`, see it's documentation for
  all available options.

  ## Examples

      iex> {:ok, query} = SnowflakeEx.prepare(conn, "", "SELECT ? * ?")
      iex> {:ok, %SnowflakeEx.Result{rows: [row]}} = SnowflakeEx.execute(conn, query, [2, 3])
      iex> row
      [6]

  """
  @spec execute(conn(), SnowflakeEx.Query.t(), list(), [option()]) ::
          {:ok, SnowflakeEx.Query.t(), SnowflakeEx.Result.t()} | {:error, Exception.t()}
  defdelegate execute(conn, query, params, opts \\ []), to: DBConnection

  @doc """
  Executes a prepared query.

  Returns `%SnowflakeEx.Result{}` on success, or raises an exception if there was an error.

  See: `execute/4`.
  """
  @spec execute!(conn(), SnowflakeEx.Query.t(), list(), keyword()) :: SnowflakeEx.Result.t()
  defdelegate execute!(conn, query, params, opts \\ []), to: DBConnection

  def query(conn, statement, params, opts \\ []) do
    if name = Keyword.get(opts, :cache_statement) do
      query = %SnowflakeEx.Query{
        name: name,
        cache: :statement,
        statement: IO.iodata_to_binary(statement)
      }

      case DBConnection.prepare_execute(conn, query, params, opts) do
        {:ok, _, result} ->
          {:ok, result}

        {:error, %SnowflakeEx.Error{snowflake: %{code: :feature_not_supported}}} = error ->
          with %DBConnection{} <- conn,
               :error <- DBConnection.status(conn) do
            error
          else
            _ -> query_prepare_execute(conn, query, params, opts)
          end

        {:error, _} = error ->
          error
      end
    else
      query_prepare_execute(
        conn,
        %SnowflakeEx.Query{name: "", statement: statement},
        params,
        opts
      )
    end
  end

  @doc """
  Runs an (extended) query and returns the result or raises `SnowflakeEx.Error` if
  there was an error. See `query/3`.
  """
  @spec query!(conn, iodata, list, [execute_option]) :: SnowflakeEx.Result.t()
  def query!(conn, statement, params, opts \\ []) do
    case query(conn, statement, params, opts) do
      {:ok, result} -> result
      {:error, err} -> raise err
    end
  end

  defp query_prepare_execute(conn, query, params, opts) do
    case DBConnection.prepare_execute(conn, query, params, opts) do
      {:ok, _, result} -> {:ok, result}
      {:error, _} = error -> error
    end
  end
end
