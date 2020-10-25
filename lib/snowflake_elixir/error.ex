defmodule SnowflakeEx.Error do
  defexception [:message, :snowflake, :connection_id, :query]

  @type t :: %SnowflakeEx.Error{}
end

defmodule SnowflakeEx.QueryError do
  defexception [:message]
end
