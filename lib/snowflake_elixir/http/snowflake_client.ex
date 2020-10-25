defmodule SnowflakeEx.HTTPClient do
  @moduledoc ~S"""
  Helper library for interacting with Snowflakes REST API.
  """
  require Logger

  def login(host, account_name, warehouse, database, schema, username, password, partial) do
    data = %{
      data: %{
        ACCOUNT_NAME: account_name,
        PASSWORD: password,
        CLIENT_APP_ID: "JavaScript",
        CLIENT_APP_VERSION: "1.5.3",
        LOGIN_NAME: username,
        SESSION_PARAMETERS: %{
          ROWS_PER_RESULTSET: partial,
          VALIDATE_DEFAULT_PARAMETERS: true
        },
        CLIENT_ENVIRONMENT: %{
          schema: schema,
          tracing: "DEBUG",
          OS: "Linux",
          OCSP_MODE: "FAIL_OPEN",
          APPLICATION: "SnowflakeEx",
          warehouse: warehouse,
          database: database,
          serverURL: host,
          user: username,
          account: account_name
        }
      }
    }

    HTTPoison.post!(
      "#{host}/session/v1/login-request?databaseName=#{database}&schemaName=#{schema}&warehouse=#{
        warehouse
      }",
      Jason.encode!(data),
      [
        {"Content-Type", "application/json"},
        {"'Accept", "application/json"}
      ],
      hackney: [
        :insecure,
        pool: :snowflake_pool
      ]
    )
    |> Map.get(:body)
    |> Jason.decode!()
    |> process_login()
  end

  def query(host, token, query, opts) do
    params = Keyword.get(opts, :params, [])
    async = Keyword.get(opts, :async, false)
    first_only = Keyword.get(opts, :first_only, false)

    query(host, token, query, params, async, first_only)
  end

  defp query(host, token, query, [], false, first_chunk) do
    Logger.debug("Running query #{query} non-async, first chunk: #{first_chunk}")

    HTTPoison.post!(
      snowflake_query_url(host),
      snowflake_query_headers(query, false),
      [
        {"Content-Type", "application/json"},
        {"accept", "application/snowflake"},
        {"Authorization", "Snowflake Token=\"#{token}\""}
      ],
      hackney: [
        :insecure,
        pool: :snowflake_pool
      ]
    )
    |> process_query(first_chunk)
  end

  defp query(host, token, query, [], true, _first_chunk) do
    Logger.debug("Running query #{query} async")

    HTTPoison.post!(
      snowflake_query_url(host),
      snowflake_query_headers(query, true),
      [
        {"Content-Type", "application/json"},
        {"accept", "application/snowflake"},
        {"Authorization", "Snowflake Token=\"#{token}\""}
      ],
      hackney: [
        :insecure,
        pool: :snowflake_pool
      ]
    )
    |> Map.get(:body)
    |> Jason.decode!()
    |> Map.get("data")
    |> Map.get("queryId")
    |> monitor_query_id(host, token, 1)
  end

  defp query(host, token, query, _params, true, _first_chunk) do
    HTTPoison.post!(
      snowflake_query_url(host),
      snowflake_query_headers(query, false),
      [
        {"Content-Type", "application/json"},
        {"accept", "application/snowflake"},
        {"Authorization", "Snowflake Token=\"#{token}\""}
      ],
      hackney: [
        :insecure,
        pool: :snowflake_pool
      ]
    )
    |> Map.get(:body)
    |> Jason.decode!()
  end

  def insert(host, token, query, _params, connect_opts) do
    HTTPoison.post!(
      snowflake_query_url(host),
      snowflake_insert_headers(query, Keyword.get(connect_opts, :field_types, %{})),
      [
        {"Content-Type", "application/json"},
        {"accept", "application/snowflake"},
        {"Authorization", "Snowflake Token=\"#{token}\""}
      ],
      hackney: [
        :insecure,
        pool: :snowflake_pool
      ]
    )
    |> Map.get(:body)
    |> Jason.decode!()
    |> process_response(false)
  end

  def monitor_query_id(monitor_id, host, token, num) when num < 1000 do
    :timer.sleep(50)

    resp =
      HTTPoison.get!(
        "#{host}/queries/#{monitor_id}/result",
        [
          {"Content-Type", "application/json"},
          {"accept", "application/snowflake"},
          {"Authorization", "Snowflake Token=\"#{token}\""}
        ],
        hackney: [
          :insecure,
          pool: :snowflake_pool
        ]
      )

    resp
    |> Map.get(:body)
    |> Jason.decode!()
    |> if do
      process_query(resp, false)
    else
      monitor_query_id(monitor_id, host, token, num + 1)
    end
  end

  def monitor_query_id(_monitor_id, _host, _token, num) do
    {:error, "failed after #{num} attempts"}
  end

  def s3_download(url, encryption_key, encryption_key_md5) do
    HTTPoison.get!(
      url,
      [
        {"accept", "application/snowflake"},
        {"Accept-Encoding", "gzip,deflate"},
        {"x-amz-server-side-encryption-customer-key", encryption_key},
        {"x-amz-server-side-encryption-customer-key-md5", encryption_key_md5}
      ],
      hackney: [
        :insecure,
        pool: :s3_pool
      ]
    )
    |> Map.get(:body)
    |> :zlib.gunzip()
  end

  defp decode_column(%{"scale" => 0, "type" => "fixed", "byteLength" => nil}, nil) do
    nil
  end

  defp decode_column(%{"scale" => 0, "type" => "fixed", "byteLength" => nil}, value) do
    case Integer.parse(value) do
      {num, ""} ->
        num

      _ ->
        value
    end
  end

  defp decode_column(_, value), do: value

  defp process_query(%{status_code: 200, body: body}, first_chunk) do
    Jason.decode!(body)
    |> process_response(first_chunk)
  end

  defp process_query(_, _), do: {:error, "error"}

  defp process_response(%{"success" => true} = data, first_chunk) do
    data
    |> Map.get("data")
    |> Map.get("queryResultFormat")
    |> process_query_result_format(data["data"], first_chunk)
  end

  defp process_response(%{"success" => false, "message" => message, "code" => error_code, "data" => %{"sqlState" => sql_error}}, _) do
    {:error, %SnowflakeEx.Result{messages: [%{message: message, severity: :error, error_code: error_code, sql_error: sql_error}]}}
  end

  defp process_query_result_format(
         "json",
         %{
           "rowset" => [],
           "rowtype" => row_type,
           "total" => total,
           "chunks" => chunks,
           "chunkHeaders" => %{
             "x-amz-server-side-encryption-customer-key" => key,
             "x-amz-server-side-encryption-customer-key-md5" => md5
           }
         } = data,
         first_chunk
       ) do
    Logger.debug("found #{length(chunks)} chunks")

    parsed =
      if first_chunk do
        chunks
        |> Enum.map(fn %{"url" => url} -> url end)
        |> Enum.map(fn url -> s3_download(url, key, md5) end)
        |> Enum.join(", ")
      else
        url =
          chunks
          |> Enum.map(fn %{"url" => url} -> url end)
          |> hd

        s3_download(url, key, md5)
      end

    rows = Jason.decode!("[#{parsed}]")

    row_data = process_row_data(rows, row_type)

    columns = Enum.map(row_type, fn %{"name" => name} -> name end)

    {:ok, %SnowflakeEx.Result{rows: row_data, columns: columns, num_rows: total, metadata: data, messages: %{message: row_data, severity: :debug}}}
  end

  defp process_query_result_format(
         "json",
         %{
           "rowset" => rowset,
           "rowtype" => row_type,
           "total" => total,
           "chunks" => chunks,
           "chunkHeaders" => %{
             "x-amz-server-side-encryption-customer-key" => key,
             "x-amz-server-side-encryption-customer-key-md5" => md5
           }
         } = data,
         first_chunk
       ) do
    Logger.debug("found #{length(chunks)} chunks")

    parsed =
      if first_chunk do
        chunks
        |> Enum.map(fn %{"url" => url} -> url end)
        |> Enum.map(fn url -> s3_download(url, key, md5) end)
        |> Enum.join(", ")
      else
        url =
          chunks
          |> Enum.map(fn %{"url" => url} -> url end)
          |> hd

        s3_download(url, key, md5)
      end

    rows = Jason.decode!("[#{parsed}]")

    row_data = process_row_data(rowset, row_type) ++ process_row_data(rows, row_type)

    columns = Enum.map(row_type, fn %{"name" => name} -> name end)

    {:ok, %SnowflakeEx.Result{rows: row_data, columns: columns, num_rows: total, metadata: data, messages: [%{message: row_data, severity: :info}]}}
  end

  defp process_query_result_format(
         "json",
         %{"rowset" => rows, "rowtype" => row_type, "total" => total} = data,
         _first_chunk
       ) do
    row_data = process_row_data(rows, row_type)

    columns = Enum.map(row_type, fn %{"name" => name} -> name end)
    {:ok, %SnowflakeEx.Result{rows: row_data, columns: columns, num_rows: total, metadata: data, messages: [%{message: row_data, severity: :info}]}}
  end

  defp process_row_data(rows, row_type) do
    rows
    |> Stream.map(fn r ->
      r
      |> Stream.with_index()
      |> Stream.map(fn {rr, column_no} ->
        decode_column(Enum.at(row_type, column_no), rr)
      end)
      |> Enum.to_list()
    end)
    |> Enum.to_list()
  end

  defp uuid(host) do
    if String.contains?(host, "127.0.0.1"),
      do: "11111111-1111-1111-1111-111111111111",
      else: UUID.uuid4()
  end

  defp process_login(%{"success" => false, "message" => message}), do: {:error, message}

  defp process_login(%{
         "data" => %{
           "token" => token,
           "sessionId" => session_id
         }
       }),
       do: {:ok, %{token: token, session_id: session_id}}

  defp process_login(_), do: {:error, "Invalid user/pass or host."}

  defp snowflake_query_headers(query, async) when is_binary(query) and is_boolean(async) do
    %{
      sqlText: query,
      sequenceId: 0,
      bindings: nil,
      bindStage: nil,
      describeOnly: false,
      parameters: %{
        CLIENT_RESULT_CHUNK_SIZE: 48
      },
      describedJobId: nil,
      isInternal: false,
      asyncExec: async
    }
    |> Jason.encode!()
  end

  defp snowflake_insert_headers(query, bindings) when is_binary(query) and is_map(bindings) do
    %{
      sqlText: query,
      sequenceId: 0,
      bindStage: nil,
      describeOnly: false,
      parameters: %{
        CLIENT_RESULT_CHUNK_SIZE: 48
      },
      describedJobId: nil,
      isInternal: false,
      bindings: bindings
    }
    |> Jason.encode!()
  end

  defp snowflake_query_url(host) do
    "#{host}/queries/v1/query-request?requestId=#{uuid(host)}"
  end
end
