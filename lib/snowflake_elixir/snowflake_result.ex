defmodule SnowflakeEx.Result do
  @type t :: %__MODULE__{
          columns: nil | [String.t()],
          rows: [tuple],
          num_rows: integer,
          metadata: [map()],
          messages: [map()],
          statement: nil | String.t()
        }

  defstruct columns: nil, rows: nil, num_rows: 0, metadata: [], messages: [], statement: nil
end
