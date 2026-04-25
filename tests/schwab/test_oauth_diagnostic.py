import httpx

from scripts.ops import schwab_oauth_diagnostic as diagnostic


def test_load_effective_env_matches_broker_smoke_file_order(tmp_path):
    env_path = tmp_path / ".env"
    env_web_path = tmp_path / ".env.web"
    env_path.write_text(
        "\n".join(
            [
                "SCHWAB_CLIENT_ID=client-id",
                "SCHWAB_CLIENT_SECRET=client-secret",
                "SCHWAB_APP_CALLBACK_URL=https://deployed.example.com/api/providers/schwab/connect/callback",
            ]
        ),
        encoding="utf-8",
    )
    env_web_path.write_text(
        "SCHWAB_APP_CALLBACK_URL=https://127.0.0.1",
        encoding="utf-8",
    )

    snapshot = diagnostic.load_effective_env(
        [env_path, env_web_path],
        base_env={"SCHWAB_CLIENT_ID": "from-process-env"},
    )

    assert snapshot.loaded_paths == (env_path.resolve(), env_web_path.resolve())
    assert snapshot.values["SCHWAB_CLIENT_ID"] == "client-id"
    assert snapshot.values["SCHWAB_CLIENT_SECRET"] == "client-secret"
    assert snapshot.values["SCHWAB_APP_CALLBACK_URL"] == "https://127.0.0.1"
    assert snapshot.callback_sources == (
        (env_path.resolve(), "https://deployed.example.com/api/providers/schwab/connect/callback"),
        (env_web_path.resolve(), "https://127.0.0.1"),
    )


def test_build_authorization_variants_compares_repo_and_pdf_shapes():
    variants = diagnostic.build_authorization_variants(
        client_id="client-id",
        effective_callback_url="https://127.0.0.1",
        callback_candidates=["https://api.example.com/api/providers/schwab/connect/callback"],
        state="opaque-state",
    )

    assert [variant.label for variant in variants] == [
        "current-effective",
        "minimal-effective",
        "current-alternate-1",
        "minimal-alternate-1",
    ]
    assert variants[0].url == (
        "https://api.schwabapi.com/v1/oauth/authorize"
        "?response_type=code&client_id=client-id&redirect_uri=https%3A%2F%2F127.0.0.1&state=opaque-state"
    )
    assert variants[1].url == (
        "https://api.schwabapi.com/v1/oauth/authorize"
        "?client_id=client-id&redirect_uri=https%3A%2F%2F127.0.0.1"
    )
    assert variants[2].callback_url == "https://api.example.com/api/providers/schwab/connect/callback"
    assert "response_type=code" in variants[2].url
    assert "response_type=code" not in variants[3].url


def test_snapshot_output_does_not_reveal_client_secret(capsys, tmp_path):
    snapshot = diagnostic.EnvSnapshot(
        loaded_paths=(tmp_path / ".env",),
        values={
            "SCHWAB_CLIENT_ID": "client-id",
            "SCHWAB_CLIENT_SECRET": "client-secret-value",
            "SCHWAB_APP_CALLBACK_URL": "https://127.0.0.1",
        },
        callback_sources=(),
    )

    diagnostic._print_snapshot(snapshot)

    output = capsys.readouterr().out
    assert "client-secret-value" not in output
    assert "suffix=alue" not in output
    assert "SCHWAB_CLIENT_SECRET: present len=19" in output


def test_probe_authorization_variant_preserves_schwab_correlation_headers():
    variant = diagnostic.AuthorizationVariant(
        label="minimal-effective",
        request_shape="pdf-minimal",
        callback_url="https://127.0.0.1",
        url="https://api.schwabapi.com/v1/oauth/authorize?client_id=client-id&redirect_uri=https%3A%2F%2F127.0.0.1",
        diagnostic_purpose="test",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url) == variant.url
        return httpx.Response(
            401,
            headers={
                "content-type": "application/json",
                "schwab-client-correlid": "correlation-123",
                "x-request-id": "request-456",
            },
            json={
                "error": "invalid_client",
                "error_description": "Unauthorized",
            },
        )

    result = diagnostic.probe_authorization_variant(
        variant,
        http_client=httpx.Client(transport=httpx.MockTransport(handler)),
    )

    assert result.status_code == 401
    assert result.request_body == "<empty>"
    assert result.response_body == '{"error":"invalid_client","error_description":"Unauthorized"}'
    assert result.error == "invalid_client"
    assert result.error_description == "Unauthorized"
    assert result.schwab_client_correlid == "correlation-123"
    assert result.request_id == "request-456"
    assert result.classification == (
        "client ID, app approval, product subscription, or exact redirect_uri rejected before login"
    )


def test_probe_summary_prioritizes_app_registration_when_every_variant_is_invalid_client():
    results = [
        diagnostic.ProbeResult(
            label="current-effective",
            status_code=401,
            location="",
            content_type="application/json",
            request_body="<empty>",
            response_body='{"error":"invalid_client"}',
            error="invalid_client",
            error_description="Unauthorized",
            schwab_client_correlid="correlation-1",
            request_id="request-1",
            classification="client ID, app approval, product subscription, or exact redirect_uri rejected before login",
        ),
        diagnostic.ProbeResult(
            label="minimal-effective",
            status_code=401,
            location="",
            content_type="application/json",
            request_body="<empty>",
            response_body='{"error":"invalid_client"}',
            error="invalid_client",
            error_description="Unauthorized",
            schwab_client_correlid="correlation-2",
            request_id="request-2",
            classification="client ID, app approval, product subscription, or exact redirect_uri rejected before login",
        ),
    ]

    assert diagnostic.summarize_probe_results(results) == (
        "Every probed variant returned invalid_client; prioritize Schwab app/client/product approval checks."
    )


def test_probe_output_includes_raw_request_and_response_bodies(capsys):
    results = [
        diagnostic.ProbeResult(
            label="minimal-effective",
            status_code=401,
            location="",
            content_type="application/json",
            request_body="<empty>",
            response_body='{"error":"invalid_client","error_description":"Unauthorized"}',
            error="invalid_client",
            error_description="Unauthorized",
            schwab_client_correlid="correlation-1",
            request_id="request-1",
            classification="client ID, app approval, product subscription, or exact redirect_uri rejected before login",
        )
    ]

    diagnostic._print_probe_results(results)

    output = capsys.readouterr().out
    assert "request_body: <empty>" in output
    assert 'response_body: {"error":"invalid_client","error_description":"Unauthorized"}' in output


def test_callback_inspection_checks_code_and_state_without_exchanging_tokens():
    inspection = diagnostic.inspect_callback_url(
        "https://127.0.0.1/?code=abc%40example.com&state=opaque-state",
        expected_state="opaque-state",
    )

    assert inspection.has_code is True
    assert inspection.code_length == len("abc@example.com")
    assert inspection.code_suffix == ".com"
    assert inspection.returned_state == "opaque-state"
    assert inspection.state_matches is True
