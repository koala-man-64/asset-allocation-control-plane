from __future__ import annotations

import logging

from api.endpoints.system_modules.jobs import _extract_log_lines
from api.service.auth import _summarize_unverified_token_for_logs
from core.logging_config import JsonFormatter
from core.redaction import redact_sensitive_text, summarize_query_params


def test_redaction_removes_secret_material_from_text() -> None:
    text = (
        "Bearer abc.def.ghi "
        "postgresql://user:pass@db.example.com:5432/app "
        "https://acct.blob.core.windows.net/c/blob?sig=sas-secret&code=oauth-code "
        "client_secret=secret-value AccountKey=storage-key"
    )

    redacted = redact_sensitive_text(text)

    assert "abc.def.ghi" not in redacted
    assert "user:pass" not in redacted
    assert "sas-secret" not in redacted
    assert "oauth-code" not in redacted
    assert "secret-value" not in redacted
    assert "storage-key" not in redacted
    assert "[REDACTED]" in redacted


def test_query_summary_keeps_keys_not_values() -> None:
    summary = summarize_query_params("code=oauth-code&state=visible&sig=sas-secret")

    assert summary == {
        "count": 3,
        "keys": ["code", "sig", "state"],
        "hasSensitiveKeys": True,
    }


def test_json_formatter_redacts_messages_and_context() -> None:
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="token Bearer abc.def.ghi",
        args=(),
        exc_info=None,
    )
    record.context = {"dsn": "postgresql://user:pass@db.example.com/app"}

    rendered = formatter.format(record)

    assert "abc.def.ghi" not in rendered
    assert "user:pass" not in rendered
    assert "[REDACTED]" in rendered


def test_log_tail_redacts_provider_and_storage_secrets() -> None:
    payload = {
        "tables": [
            {
                "columns": [{"name": "Log_s"}],
                "rows": [["failed POSTGRES_DSN=postgresql://user:pass@db/app&sig=sas-secret"]],
            }
        ]
    }

    lines = _extract_log_lines(payload)

    assert len(lines) == 1
    assert "user:pass" not in lines[0]
    assert "sas-secret" not in lines[0]
    assert "[REDACTED]" in lines[0]


def test_unverified_token_summary_does_not_decode_claims() -> None:
    summary = _summarize_unverified_token_for_logs("not-a-jwt")

    assert summary["claims"] == "<not-decoded-before-verification>"
    assert "sha256_12" in summary
    assert "not-a-jwt" not in str(summary)
