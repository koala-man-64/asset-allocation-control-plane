from __future__ import annotations

import logging
import sys

from uvicorn.logging import AccessFormatter

from core.log_redaction import REDACTED, install_log_redaction, redact_text, redact_value


def test_redact_text_covers_query_params_bearer_tokens_and_connection_strings() -> None:
    text = (
        "GET /path?apikey=alpha-secret&symbol=AAPL "
        "Authorization: Bearer bearer-secret "
        "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=storage-secret;EndpointSuffix=core.windows.net"
    )

    redacted = redact_text(text)

    assert "alpha-secret" not in redacted
    assert "bearer-secret" not in redacted
    assert "storage-secret" not in redacted
    assert "symbol=AAPL" in redacted
    assert redacted.count(REDACTED) >= 3


def test_redact_value_recurses_through_headers_query_params_and_context() -> None:
    payload = {
        "headers": {
            "Authorization": "Bearer header-secret",
            "X-Request-ID": "req-123",
        },
        "query_params": {
            "apikey": "query-secret",
            "symbol": "AAPL",
        },
        "nested": ["token=list-secret", {"client_secret": "client-secret"}],
    }

    redacted = redact_value(payload)

    assert redacted["headers"]["Authorization"] == REDACTED
    assert redacted["headers"]["X-Request-ID"] == "req-123"
    assert redacted["query_params"]["apikey"] == REDACTED
    assert redacted["query_params"]["symbol"] == "AAPL"
    assert "list-secret" not in str(redacted)
    assert "client-secret" not in str(redacted)


def test_installed_log_redaction_sanitizes_message_args_context_and_traceback(
    caplog,
) -> None:
    install_log_redaction()
    logger = logging.getLogger("tests.redaction")

    with caplog.at_level(logging.ERROR, logger="tests.redaction"):
        try:
            raise RuntimeError("failed with apikey=exception-secret and Bearer traceback-token")
        except RuntimeError:
            logger.exception(
                "provider call failed url=%s",
                "https://provider.example/query?access_token=arg-secret",
                extra={
                    "context": {
                        "headers": {"Authorization": "Bearer context-secret"},
                        "query_params": {"apikey": "context-query-secret", "symbol": "AAPL"},
                    }
                },
            )

    text = caplog.text
    assert "exception-secret" not in text
    assert "traceback-token" not in text
    assert "arg-secret" not in text
    assert "context-secret" not in text
    assert "context-query-secret" not in text
    assert REDACTED in text

    record = next(record for record in caplog.records if record.name == "tests.redaction")
    assert record.context["headers"]["Authorization"] == REDACTED
    assert record.context["query_params"]["apikey"] == REDACTED
    assert record.context["query_params"]["symbol"] == "AAPL"


def test_installed_log_redaction_formats_before_redacting_sensitive_placeholder_names(caplog) -> None:
    install_log_redaction()
    logger = logging.getLogger("tests.redaction.formatting")

    with caplog.at_level(logging.INFO, logger="tests.redaction.formatting"):
        logger.info(
            "provider request token=%s path=%s",
            {"claims": {"sub": "user-1"}, "api_key": "arg-secret"},
            "/api/providers/alpha-vantage",
        )

    text = caplog.text
    assert "arg-secret" not in text
    assert "/api/providers/alpha-vantage" in text
    assert REDACTED in text

    record = next(record for record in caplog.records if record.name == "tests.redaction.formatting")
    assert record.args == ()


def test_installed_log_redaction_preserves_uvicorn_access_formatter_args() -> None:
    install_log_redaction()
    logger = logging.getLogger("uvicorn.access")

    record = logger.makeRecord(
        "uvicorn.access",
        logging.INFO,
        __file__,
        1,
        '%s - "%s %s HTTP/%s" %d',
        ("127.0.0.1:12345", "GET", "/readyz?apikey=access-secret&symbol=AAPL", "1.1", 200),
        None,
    )

    rendered = AccessFormatter(
        fmt='%(client_addr)s - "%(request_line)s" %(status_code)s',
        use_colors=False,
    ).format(record)

    assert len(record.args) == 5
    assert "access-secret" not in rendered
    assert "apikey=[REDACTED]" in rendered
    assert "symbol=AAPL" in rendered
    assert '"GET /readyz?' in rendered


def test_installed_log_redaction_sanitizes_formatter_exception_text() -> None:
    install_log_redaction()

    try:
        raise RuntimeError("failed with apikey=formatter-secret and Bearer formatter-token")
    except RuntimeError:
        formatted = logging.Formatter().formatException(sys.exc_info())

    assert "formatter-secret" not in formatted
    assert "formatter-token" not in formatted
    assert "apikey=[REDACTED]" in formatted
    assert "Bearer [REDACTED]" in formatted
