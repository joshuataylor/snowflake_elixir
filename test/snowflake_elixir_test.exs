defmodule SnowflakeExTest do
  use ExUnit.Case, async: true

  setup do
    bypass = Bypass.open()

    username = Application.put_env(:snowflake_elixir, :username, "TEST123")
    password = Application.put_env(:snowflake_elixir, :password, "FOO123")
    host = Application.put_env(:snowflake_elixir, :host, "http://127.0.0.1:#{bypass.port}")
    account_name = Application.put_env(:snowflake_elixir, :account_name, "account_name")

    {:ok,
     bypass: bypass,
     username: username,
     opts: %{password: password, host: host, account_name: account_name}}
  end

  test "can login to snowflake", %{bypass: bypass} do
    valid_login_bypass(bypass)

    assert SnowflakeEx.login() == "validtoken"
  end

  test "can't login to snowflake with invalid credentials", %{bypass: bypass} do
    invalid_login_bypass(bypass)

    assert SnowflakeEx.login() == {:error, "Incorrect username or password was specified."}
  end

  defp valid_login_bypass(bypass) do
    Bypass.expect_once(bypass, "POST", "/session/v1/login-request", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        Path.join([:code.priv_dir(:snowflake_elixir), "valid_login.json"]) |> File.read!()
      )
    end)
  end

  defp invalid_login_bypass(bypass) do
    Bypass.expect_once(bypass, "POST", "/session/v1/login-request", fn conn ->
      Plug.Conn.resp(
        conn,
        200,
        ~s<{"code": "390100","data": {"nextAction": "RETRY_LOGIN","pwdChangeInfo": null,"inFlightCtx": null,"redirectUrl": null,"licenseAgreementPDFFilePath": null,"licenseAgreementHTMLFilePath": null,"authnMethod": "USERNAME_PASSWORD","oAuthSessionStorageData": null,"relayState": null},"message": "Incorrect username or password was specified.","success": false,"headers": null}>
      )
    end)
  end
end
