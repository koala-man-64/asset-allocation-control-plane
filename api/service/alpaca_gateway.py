from __future__ import annotations

import hashlib
import logging
import threading
import time
from dataclasses import dataclass, is_dataclass
from datetime import datetime
from typing import Any, Literal, Optional

from alpaca import (
    AlpacaEnvironmentConfig,
    AlpacaError,
    AlpacaNotConfiguredError,
    AlpacaTradingClient,
    AlpacaValidationError,
    HttpConfig,
)
from api.service.settings import AlpacaSettings

logger = logging.getLogger("asset-allocation.api.alpaca")

AlpacaEnvironment = Literal["paper", "live"]
_SUPPORTED_ENVIRONMENTS: tuple[AlpacaEnvironment, AlpacaEnvironment] = ("paper", "live")


@dataclass(frozen=True)
class _ClientSnapshot:
    api_key_hash: str
    api_secret_hash: str
    base_url: str
    timeout_seconds: float
    max_retries: int
    backoff_base_seconds: float


def _hash_secret(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _normalize_environment(environment: str) -> AlpacaEnvironment:
    normalized = str(environment or "").strip().lower()
    if normalized not in _SUPPORTED_ENVIRONMENTS:
        raise AlpacaValidationError(f"Unsupported Alpaca environment={environment!r}.")
    return normalized  # type: ignore[return-value]


def _status_for_exception(exc: Exception) -> str:
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        return str(status_code)
    if isinstance(exc, AlpacaNotConfiguredError):
        return "503"
    return type(exc).__name__


def _summarize_exception(exc: Exception) -> str:
    detail = str(exc).strip() or type(exc).__name__
    payload = getattr(exc, "payload", None)
    if payload:
        detail = f"{detail} payload={payload}"
    if len(detail) > 240:
        return f"{detail[:237]}..."
    return detail


def _get_value(payload: Any, key: str) -> str | None:
    if payload is None:
        return None
    if isinstance(payload, dict):
        value = payload.get(key)
        if value is None:
            return None
        return str(value)
    if is_dataclass(payload):
        value = getattr(payload, key, None)
        if value is None:
            return None
        return str(value)
    value = getattr(payload, key, None)
    if value is None:
        return None
    return str(value)


class AlpacaGateway:
    def __init__(self, settings: AlpacaSettings) -> None:
        self._settings = settings
        self._lock = threading.RLock()
        self._clients: dict[AlpacaEnvironment, AlpacaTradingClient] = {}
        self._snapshots: dict[AlpacaEnvironment, _ClientSnapshot] = {}

    def close(self) -> None:
        with self._lock:
            clients = list(self._clients.values())
            self._clients.clear()
            self._snapshots.clear()
        for client in clients:
            try:
                client.close()
            except Exception:
                logger.debug("Alpaca client shutdown failed.", exc_info=True)

    def _provider_config_for(self, environment: AlpacaEnvironment) -> AlpacaEnvironmentConfig:
        if environment == "paper":
            if not self._settings.paper_configured:
                raise AlpacaNotConfiguredError("Alpaca paper credentials are not configured.")
            return AlpacaEnvironmentConfig(
                environment="paper",
                api_key=str(self._settings.paper_api_key_id),
                api_secret=str(self._settings.paper_secret_key),
                trading_base_url=self._settings.paper_trading_base_url,
                http=HttpConfig(
                    timeout_s=self._settings.timeout_seconds,
                    max_retries=self._settings.max_retries,
                    backoff_base_s=self._settings.backoff_base_seconds,
                ),
            )

        if not self._settings.live_configured:
            raise AlpacaNotConfiguredError("Alpaca live credentials are not configured.")
        return AlpacaEnvironmentConfig(
            environment="live",
            api_key=str(self._settings.live_api_key_id),
            api_secret=str(self._settings.live_secret_key),
            trading_base_url=self._settings.live_trading_base_url,
            http=HttpConfig(
                timeout_s=self._settings.timeout_seconds,
                max_retries=self._settings.max_retries,
                backoff_base_s=self._settings.backoff_base_seconds,
            ),
        )

    @staticmethod
    def _snapshot_from_config(config: AlpacaEnvironmentConfig) -> _ClientSnapshot:
        return _ClientSnapshot(
            api_key_hash=_hash_secret(config.api_key),
            api_secret_hash=_hash_secret(config.api_secret),
            base_url=config.get_trading_base_url(),
            timeout_seconds=float(config.http.timeout_s),
            max_retries=int(config.http.max_retries),
            backoff_base_seconds=float(config.http.backoff_base_s),
        )

    def _client_for(self, environment: AlpacaEnvironment) -> AlpacaTradingClient:
        config = self._provider_config_for(environment)
        snapshot = self._snapshot_from_config(config)
        with self._lock:
            current = self._clients.get(environment)
            if current is None or self._snapshots.get(environment) != snapshot:
                old = current
                current = AlpacaTradingClient(config)
                self._clients[environment] = current
                self._snapshots[environment] = snapshot
                if old is not None:
                    try:
                        old.close()
                    except Exception:
                        logger.debug("Failed to close replaced Alpaca client for %s.", environment, exc_info=True)
            return current

    def _execute(
        self,
        *,
        environment: AlpacaEnvironment,
        operation: str,
        subject: Optional[str],
        write_operation: bool,
        call,
        audit_fields: dict[str, Any] | None = None,
    ) -> Any:
        client = self._client_for(environment)
        started_at = time.perf_counter()
        try:
            payload = call(client)
        except Exception as exc:
            logger.warning(
                "Alpaca provider failure environment=%s operation=%s subject=%s status=%s request_id=%s latency_ms=%s error=%s",
                environment,
                operation,
                subject or "-",
                _status_for_exception(exc),
                client.last_request_id or "n/a",
                int((time.perf_counter() - started_at) * 1000),
                _summarize_exception(exc),
                exc_info=not isinstance(exc, AlpacaError),
            )
            raise

        request_id = client.last_request_id or "n/a"
        latency_ms = int((time.perf_counter() - started_at) * 1000)
        if write_operation:
            fields = audit_fields or {}
            logger.info(
                "Alpaca trade audit environment=%s operation=%s subject=%s symbol=%s order_id=%s client_order_id=%s request_id=%s latency_ms=%s",
                environment,
                operation,
                subject or "-",
                fields.get("symbol") or _get_value(payload, "symbol") or "n/a",
                fields.get("order_id") or _get_value(payload, "id") or "n/a",
                fields.get("client_order_id") or _get_value(payload, "client_order_id") or "n/a",
                request_id,
                latency_ms,
            )
        else:
            logger.info(
                "Alpaca provider success environment=%s operation=%s subject=%s request_id=%s latency_ms=%s payload_type=%s",
                environment,
                operation,
                subject or "-",
                request_id,
                latency_ms,
                type(payload).__name__,
            )
        return payload

    def get_account(self, *, environment: str, subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="account",
            subject=subject,
            write_operation=False,
            call=lambda client: client.get_account(),
        )

    def list_positions(self, *, environment: str, subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="positions",
            subject=subject,
            write_operation=False,
            call=lambda client: client.list_positions(),
        )

    def list_orders(
        self,
        *,
        environment: str,
        subject: Optional[str],
        status: str = "open",
        limit: int = 500,
        after: datetime | None = None,
        until: datetime | None = None,
        nested: bool = False,
        symbols: list[str] | None = None,
    ) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="orders",
            subject=subject,
            write_operation=False,
            call=lambda client: client.list_orders(
                status=status,
                limit=limit,
                after=after,
                until=until,
                nested=nested,
                symbols=symbols,
            ),
        )

    def get_order(self, *, environment: str, order_id: str, subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="order",
            subject=subject,
            write_operation=False,
            call=lambda client: client.get_order(order_id),
        )

    def get_order_by_client_order_id(
        self,
        *,
        environment: str,
        client_order_id: str,
        subject: Optional[str],
    ) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="order_by_client_order_id",
            subject=subject,
            write_operation=False,
            call=lambda client: client.get_order_by_client_order_id(client_order_id),
        )

    def submit_order(self, *, environment: str, order: dict[str, Any], subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="submit_order",
            subject=subject,
            write_operation=True,
            call=lambda client: client.submit_order(**order),
            audit_fields={
                "symbol": order.get("symbol"),
                "client_order_id": order.get("client_order_id"),
            },
        )

    def replace_order(
        self,
        *,
        environment: str,
        order_id: str,
        order: dict[str, Any],
        subject: Optional[str],
    ) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="replace_order",
            subject=subject,
            write_operation=True,
            call=lambda client: client.replace_order(order_id, **order),
            audit_fields={
                "order_id": order_id,
                "client_order_id": order.get("client_order_id"),
            },
        )

    def cancel_order(self, *, environment: str, order_id: str, subject: Optional[str]) -> dict[str, Any]:
        env = _normalize_environment(environment)

        def _call(client: AlpacaTradingClient) -> dict[str, Any]:
            client.cancel_order(order_id)
            return {
                "environment": env,
                "order_id": order_id,
                "canceled": True,
            }

        return self._execute(
            environment=env,
            operation="cancel_order",
            subject=subject,
            write_operation=True,
            call=_call,
            audit_fields={"order_id": order_id},
        )

    def cancel_all_orders(self, *, environment: str, subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="cancel_all_orders",
            subject=subject,
            write_operation=True,
            call=lambda client: client.cancel_all_orders(),
        )
