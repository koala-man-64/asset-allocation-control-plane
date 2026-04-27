from __future__ import annotations

import hashlib
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Literal, Optional

from kalshi import (
    HttpConfig,
    KalshiEnvironmentConfig,
    KalshiError,
    KalshiNotConfiguredError,
    KalshiTradingClient,
    KalshiValidationError,
)

from api.service.settings import KalshiSettings

logger = logging.getLogger("asset-allocation.api.kalshi")

KalshiEnvironment = Literal["demo", "live"]
_SUPPORTED_ENVIRONMENTS: tuple[KalshiEnvironment, KalshiEnvironment] = ("demo", "live")


@dataclass(frozen=True)
class _ClientSnapshot:
    api_key_hash: str
    private_key_hash: str
    base_url: str
    timeout_seconds: float
    read_retry_attempts: int
    read_retry_base_delay_seconds: float


def _hash_secret(value: str | None) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()[:12]


def _normalize_environment(environment: str) -> KalshiEnvironment:
    normalized = str(environment or "").strip().lower()
    if normalized not in _SUPPORTED_ENVIRONMENTS:
        raise KalshiValidationError(f"Unsupported Kalshi environment={environment!r}.")
    return normalized  # type: ignore[return-value]


def _status_for_exception(exc: Exception) -> str:
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        return str(status_code)
    if isinstance(exc, KalshiNotConfiguredError):
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


class KalshiGateway:
    def __init__(self, settings: KalshiSettings) -> None:
        self._settings = settings
        self._lock = threading.RLock()
        self._clients: dict[KalshiEnvironment, KalshiTradingClient] = {}
        self._snapshots: dict[KalshiEnvironment, _ClientSnapshot] = {}

    def close(self) -> None:
        with self._lock:
            clients = list(self._clients.values())
            self._clients.clear()
            self._snapshots.clear()
        for client in clients:
            try:
                client.close()
            except Exception:
                logger.debug("Kalshi client shutdown failed.", exc_info=True)

    def _provider_config_for(self, environment: KalshiEnvironment) -> KalshiEnvironmentConfig:
        if environment == "demo":
            return KalshiEnvironmentConfig(
                environment="demo",
                http=HttpConfig(
                    timeout_s=self._settings.timeout_seconds,
                    read_retry_attempts=self._settings.read_retry_attempts,
                    read_retry_base_s=self._settings.read_retry_base_delay_seconds,
                ),
            )
        return KalshiEnvironmentConfig(
            environment="live",
            api_key_id=self._settings.live_api_key_id,
            private_key_pem=self._settings.live_private_key_pem,
            base_url=self._settings.live_base_url,
            http=HttpConfig(
                timeout_s=self._settings.timeout_seconds,
                read_retry_attempts=self._settings.read_retry_attempts,
                read_retry_base_s=self._settings.read_retry_base_delay_seconds,
            ),
        )

    @staticmethod
    def _snapshot_from_config(config: KalshiEnvironmentConfig) -> _ClientSnapshot:
        return _ClientSnapshot(
            api_key_hash=_hash_secret(config.api_key_id),
            private_key_hash=_hash_secret(config.private_key_pem),
            base_url=config.get_base_url(),
            timeout_seconds=float(config.http.timeout_s),
            read_retry_attempts=int(config.http.read_retry_attempts),
            read_retry_base_delay_seconds=float(config.http.read_retry_base_s),
        )

    def _client_for(self, environment: KalshiEnvironment) -> KalshiTradingClient:
        config = self._provider_config_for(environment)
        snapshot = self._snapshot_from_config(config)
        with self._lock:
            current = self._clients.get(environment)
            if current is None or self._snapshots.get(environment) != snapshot:
                old = current
                current = KalshiTradingClient(config)
                self._clients[environment] = current
                self._snapshots[environment] = snapshot
                if old is not None:
                    try:
                        old.close()
                    except Exception:
                        logger.debug("Failed to close replaced Kalshi client for %s.", environment, exc_info=True)
            return current

    def _execute(
        self,
        *,
        environment: KalshiEnvironment,
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
                "Kalshi provider failure environment=%s operation=%s subject=%s status=%s request_id=%s latency_ms=%s error=%s",
                environment,
                operation,
                subject or "-",
                _status_for_exception(exc),
                client.last_request_id or "n/a",
                int((time.perf_counter() - started_at) * 1000),
                _summarize_exception(exc),
                exc_info=not isinstance(exc, KalshiError),
            )
            raise

        request_id = client.last_request_id or "n/a"
        latency_ms = int((time.perf_counter() - started_at) * 1000)
        if write_operation:
            fields = audit_fields or {}
            logger.info(
                "Kalshi trade audit environment=%s operation=%s subject=%s ticker=%s order_id=%s client_order_id=%s request_id=%s latency_ms=%s",
                environment,
                operation,
                subject or "-",
                fields.get("ticker") or "n/a",
                fields.get("order_id") or "n/a",
                fields.get("client_order_id") or "n/a",
                request_id,
                latency_ms,
            )
        else:
            logger.info(
                "Kalshi provider success environment=%s operation=%s subject=%s request_id=%s latency_ms=%s payload_type=%s",
                environment,
                operation,
                subject or "-",
                request_id,
                latency_ms,
                type(payload).__name__,
            )
        return payload

    def list_markets(self, *, environment: str, subject: Optional[str], **kwargs: Any) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="markets",
            subject=subject,
            write_operation=False,
            call=lambda client: client.list_markets(**kwargs),
        )

    def get_market(self, *, environment: str, ticker: str, subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="market",
            subject=subject,
            write_operation=False,
            call=lambda client: client.get_market(ticker),
        )

    def get_orderbook(self, *, environment: str, ticker: str, depth: int, subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="orderbook",
            subject=subject,
            write_operation=False,
            call=lambda client: client.get_orderbook(ticker, depth=depth),
        )

    def get_balance(self, *, environment: str, subaccount: int, subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="balance",
            subject=subject,
            write_operation=False,
            call=lambda client: client.get_balance(subaccount=subaccount),
        )

    def list_positions(self, *, environment: str, subject: Optional[str], **kwargs: Any) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="positions",
            subject=subject,
            write_operation=False,
            call=lambda client: client.list_positions(**kwargs),
        )

    def list_orders(self, *, environment: str, subject: Optional[str], **kwargs: Any) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="orders",
            subject=subject,
            write_operation=False,
            call=lambda client: client.list_orders(**kwargs),
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

    def get_order_queue_position(self, *, environment: str, order_id: str, subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="order_queue_position",
            subject=subject,
            write_operation=False,
            call=lambda client: client.get_order_queue_position(order_id),
        )

    def get_queue_positions(self, *, environment: str, subject: Optional[str], **kwargs: Any) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="queue_positions",
            subject=subject,
            write_operation=False,
            call=lambda client: client.get_queue_positions(**kwargs),
        )

    def get_account_limits(self, *, environment: str, subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="account_limits",
            subject=subject,
            write_operation=False,
            call=lambda client: client.get_account_limits(),
        )

    def create_order(self, *, environment: str, order: dict[str, Any], subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="create_order",
            subject=subject,
            write_operation=True,
            call=lambda client: client.create_order(order),
            audit_fields={
                "ticker": order.get("ticker"),
                "client_order_id": order.get("client_order_id"),
            },
        )

    def cancel_order(
        self,
        *,
        environment: str,
        order_id: str,
        subaccount: int | None,
        subject: Optional[str],
    ) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="cancel_order",
            subject=subject,
            write_operation=True,
            call=lambda client: client.cancel_order(order_id, subaccount=subaccount),
            audit_fields={"order_id": order_id},
        )

    def amend_order(self, *, environment: str, order_id: str, order: dict[str, Any], subject: Optional[str]) -> Any:
        env = _normalize_environment(environment)
        return self._execute(
            environment=env,
            operation="amend_order",
            subject=subject,
            write_operation=True,
            call=lambda client: client.amend_order(order_id, order),
            audit_fields={
                "ticker": order.get("ticker"),
                "order_id": order_id,
                "client_order_id": order.get("updated_client_order_id") or order.get("client_order_id"),
            },
        )
