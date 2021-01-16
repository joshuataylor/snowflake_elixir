# SnowflakeEx

[![hex.pm](https://img.shields.io/hexpm/v/snowflake_elixir.svg)](https://hex.pm/packages/snowflake_elixir)
[![hexdocs.pm](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/snowflake_elixir/)
[![hex.pm](https://img.shields.io/hexpm/dt/snowflake_elixir.svg)](https://hex.pm/packages/snowflake_elixir)
[![hex.pm](https://img.shields.io/hexpm/l/snowflake_elixir.svg)](https://hex.pm/packages/snowflake_elixir)
[![github.com](https://img.shields.io/github/last-commit/joshuataylor/snowflake_elixir.svg)](https://github.com/joshuataylor/snowflake_elixir/commits/master)

**WIP, NOT PRODUCTION READY YET**

NOTE: THIS DRIVER/CONNECTOR IS NOT OFFICIALLY AFFILIATED WITH SNOWFLAKE, NOR HAS OFFICIAL SUPPORT FROM THEM.

A pure-elixir driver for [Snowflake](https://www.snowflake.com/), the cloud data platform.

## Installation

Add `snowflake_elixir` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:snowflake_elixir, "~> 0.1.0"}
  ]
end
```

# What this is

It uses the Snowflake REST API to communicate with Snowflake, with an earlier version set for JSON.
There isn't an Elixir Parquet/Arrow/IPC library (yet!), but like the [Javascript library](https://github.com/snowflakedb/snowflake-connector-nodejs)
it seems that we just get back JSON instead of an Arrow file.

Once I have time I will write a library that will manage this, as [apparently it's faster](https://www.snowflake.com/blog/fetching-query-results-from-snowflake-just-got-a-lot-faster-with-apache-arrow/)

If you want to use this as an [ecto_sql](https://github.com/elixir-ecto/ecto_sql) adapter, go and grab the [snowflake_elixir_ecto](https://github.com/joshuataylor/snowflake_elixir_ecto) adapter
as well, as you can use that with ecto. This is just the raw library that uses dbconnection.

One of the major notes when using Ecto is you will need to enable Snowflakes `QUOTED_IDENTIFIERS_IGNORE_CASE` setting, which you can
find here: https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html#third-party-tools-and-case-insensitive-identifier-resolution

Note that this can be done on an account or if needed on a session level which you can set below.

## Features
* Nothing yet :-(

## Short term Roadmap
- Support downloading multiple S3 chunks at the same time
- Implement testing the endpoints

## Long term roadmap
- Support Arrow/Parquet/IPC/etc
- Use a better login/session approach

## Thanks
I just want to thank the opensource community, especially dbconnection/ecto/ecto_sql/postgrex for being amazing, and
and being able to copy most of the decoding code from that.

## Options

Snowflake is a little bit different than most other adapters (Postgres, MySQL, etc) as it communicates over
HTTP and not a binary protocol. There is support for both waiting for a query (synchronous) and async queries.

It's recommended to set async on, as it will query Snowflake every 1000ms (changeable setting) to see if the
query has finished, instead of leaving the HTTP session open.

## Connection options

* `:host` - Server hostname, including https. Example: "https://xxx.us-east-1.snowflakecomputing.com"
* `:username` - Username for your account.
* `:password` - Password for your account.
* `:warehouse` - Warehouse to use on Snowflake. If none set, will use default for the account.
* `:account_name` - Account name. This is usually the name between the https:// and us-east-1 (or whatever region).
* `:database` - the database to connect to.
* `:schema` - the schema to connect to.
* `:async` - If set to true, will issue a query then connect every `:async_interval` to see if the query has completed.
* `:async_query_interval` - How often to check if the query has completed.
* `:pool` - The connection pool module, defaults to `DBConnection.ConnectionPool`
* `:connect_timeout` - The timeout for establishing new connections (default: 30000)
* `:prepare` - How to prepare queries, either `:named` to use named queries
  or `:unnamed` to force unnamed queries (default: `:named`)
* `:socket_options` - Specifies socket configuration
* `:show_sensitive_data_on_connection_error` - show connection data and
  configuration whenever there is an error attempting to connect to the
  database

## Example usage with a raw connection

You can create a simple connection and query using the following

```
{:ok, pid} = DBConnection.start_link(SnowflakeEx.Protocol, 
  [host: "https://youraccount.snowflakecomputing.com/",
  username: "user",
  password: "pass",
  account_name: "youraccount",
  database: "database"]
)

{:ok, query} = SnowflakeEx.query(pid, "select * from database.schema.table limit 1", [])
```
