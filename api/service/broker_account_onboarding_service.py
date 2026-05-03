from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Literal, Mapping

from asset_allocation_contracts.broker_accounts import (
    BrokerAccountConfiguration,
    BrokerAccountConfigurationAuditRecord,
    BrokerAccountOnboardingCandidate,
    BrokerAccountOnboardingCandidateListResponse,
    BrokerAccountOnboardingRequest,
    BrokerAccountOnboardingResponse,
)
from asset_allocation_contracts.trade_desk import (
    TradeAccountDetail,
    TradeAccountSummary,
    TradeCapabilityFlags,
    TradeDataFreshness,
)

from api.service.broker_account_configuration_service import (
    BrokerAccountConfigurationError,
    BrokerAccountConfigurationService,
)
from api.service.broker_account_operations_service import (
    BrokerAccountOperationsError,
    BrokerAccountOperationsService,
)
from api.service.broker_account_status_refresh_service import BrokerAccountStatusRefreshService
from api.service.settings import TradeDeskSettings
from core.broker_account_configuration_repository import BrokerAccountConfigurationRepository
from core.trade_desk_repository import TradeDeskRepository, utc_now

Provider = Literal["alpaca", "etrade", "schwab"]
Environment = Literal["paper", "sandbox", "live"]
Posture = Literal["monitor_only", "paper", "sandbox", "live"]

_PROVIDERS = {"alpaca", "etrade", "schwab"}
_ENVIRONMENTS = {"paper", "sandbox", "live"}
_POSTURES: tuple[Posture, Posture, Posture, Posture] = ("monitor_only", "paper", "sandbox", "live")


class BrokerAccountOnboardingError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True)
class DiscoveredOnboardingCandidate:
    contract: BrokerAccountOnboardingCandidate
    providerAccountKey: str | None
    metadata: dict[str, Any]


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, Mapping):
        return [value]
    return []


def _get_value(payload: Any, *keys: str) -> Any:
    for key in keys:
        if isinstance(payload, Mapping) and key in payload:
            value = payload.get(key)
            if value is not None and value != "":
                return value
        value = getattr(payload, key, None)
        if value is not None and value != "":
            return value

    lower_keys = {key.lower() for key in keys}
    if isinstance(payload, Mapping):
        for actual_key, value in payload.items():
            if str(actual_key).lower() in lower_keys and value is not None and value != "":
                return value
    return None


def _camel_to_snake(value: str) -> str:
    result: list[str] = []
    for char in value:
        if char.isupper() and result:
            result.append("_")
        result.append(char.lower())
    return "".join(result)


def _first_present(payload: Any, *keys: str) -> Any:
    expanded: list[str] = []
    for key in keys:
        expanded.append(key)
        expanded.append(_camel_to_snake(key))
    return _get_value(payload, *expanded)


def _collect_named_nodes(payload: Any, *names: str) -> list[Mapping[str, Any]]:
    matches: list[Mapping[str, Any]] = []
    target_names = {name.lower() for name in names}

    def visit(node: Any) -> None:
        if isinstance(node, Mapping):
            for key, value in node.items():
                if str(key).lower() in target_names:
                    for item in _as_list(value):
                        mapping = _as_mapping(item)
                        if mapping:
                            matches.append(mapping)
                visit(value)
            return
        if isinstance(node, (list, tuple)):
            for item in node:
                visit(item)

    visit(payload)
    return matches


def _to_float(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    text = str(value).strip().replace(",", "")
    if text.startswith("$"):
        text = text[1:]
    try:
        return float(Decimal(text))
    except (InvalidOperation, ValueError):
        return None


def _mask_account(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    if len(text) <= 4:
        return f"***{text}"
    return f"***{text[-4:]}"


def _slug(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", str(value or "").strip().lower()).strip("-")
    return normalized[:64] or "account"


def _hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def _summarize_error(exc: Exception) -> str:
    detail = str(exc).strip() or type(exc).__name__
    status_code = getattr(exc, "status_code", None)
    if status_code is not None:
        detail = f"status={status_code} {detail}"
    if len(detail) > 240:
        detail = f"{detail[:237]}..."
    return detail


class BrokerAccountOnboardingService:
    def __init__(
        self,
        trade_repo: TradeDeskRepository,
        configuration_repo: BrokerAccountConfigurationRepository,
        configuration_service: BrokerAccountConfigurationService,
        operations_service: BrokerAccountOperationsService,
        settings: TradeDeskSettings,
        *,
        refresh_service: BrokerAccountStatusRefreshService | None = None,
        alpaca_gateway: Any | None = None,
        etrade_gateway: Any | None = None,
        schwab_gateway: Any | None = None,
    ) -> None:
        self._trade_repo = trade_repo
        self._configuration_repo = configuration_repo
        self._configuration_service = configuration_service
        self._operations_service = operations_service
        self._settings = settings
        self._refresh_service = refresh_service
        self._alpaca_gateway = alpaca_gateway
        self._etrade_gateway = etrade_gateway
        self._schwab_gateway = schwab_gateway

    def list_candidates(
        self,
        *,
        provider: str,
        environment: str,
        actor: str | None,
        granted_roles: list[str],
    ) -> BrokerAccountOnboardingCandidateListResponse:
        normalized_provider = self._normalize_provider(provider)
        normalized_environment = self._normalize_environment(environment)
        try:
            discovered = self._discover_candidates(
                provider=normalized_provider,
                environment=normalized_environment,
                actor=actor,
                granted_roles=granted_roles,
            )
        except BrokerAccountOnboardingError as exc:
            if exc.status_code == 424:
                return BrokerAccountOnboardingCandidateListResponse(
                    candidates=[],
                    discoveryStatus="not_connected",
                    message=exc.detail,
                    generatedAt=utc_now(),
                )
            if exc.status_code == 503:
                return BrokerAccountOnboardingCandidateListResponse(
                    candidates=[],
                    discoveryStatus="provider_unavailable",
                    message=exc.detail,
                    generatedAt=utc_now(),
                )
            return BrokerAccountOnboardingCandidateListResponse(
                candidates=[],
                discoveryStatus="failed",
                message=exc.detail,
                generatedAt=utc_now(),
            )
        except Exception as exc:
            return BrokerAccountOnboardingCandidateListResponse(
                candidates=[],
                discoveryStatus="failed",
                message=_summarize_error(exc),
                generatedAt=utc_now(),
            )

        return BrokerAccountOnboardingCandidateListResponse(
            candidates=[candidate.contract for candidate in discovered],
            discoveryStatus="completed",
            generatedAt=utc_now(),
        )

    def onboard_account(
        self,
        payload: BrokerAccountOnboardingRequest,
        *,
        actor: str | None,
        request_id: str | None,
        granted_roles: list[str],
    ) -> BrokerAccountOnboardingResponse:
        discovered = self._discover_candidates(
            provider=self._normalize_provider(payload.provider),
            environment=self._normalize_environment(payload.environment),
            actor=actor,
            granted_roles=granted_roles,
        )
        candidate = next((item for item in discovered if item.contract.candidateId == payload.candidateId), None)
        if candidate is None:
            raise BrokerAccountOnboardingError(404, "Onboarding candidate was not found. Refresh discovery and retry.")

        contract = candidate.contract
        if contract.state == "already_configured":
            raise BrokerAccountOnboardingError(409, f"Account '{contract.suggestedAccountId}' is already enabled.")
        if not contract.canOnboard:
            raise BrokerAccountOnboardingError(409, contract.stateReason or "This broker account cannot be onboarded.")
        if payload.executionPosture not in contract.allowedExecutionPostures:
            reason = contract.blockedExecutionPostureReasons.get(payload.executionPosture)
            raise BrokerAccountOnboardingError(
                403,
                reason or f"Execution posture '{payload.executionPosture}' is not allowed for this account.",
            )

        existing = self._trade_repo.get_account_seed_state(contract.suggestedAccountId)
        if existing is not None and existing.enabled:
            raise BrokerAccountOnboardingError(409, f"Account '{contract.suggestedAccountId}' is already enabled.")

        account = self._seed_trade_account(
            candidate=contract,
            display_name=payload.displayName,
            readiness=payload.readiness,
            posture=payload.executionPosture,
        )
        detail = TradeAccountDetail(account=account)
        created, reenabled = self._trade_repo.upsert_account_seed(
            account=account,
            detail=detail,
            provider_account_key=candidate.providerAccountKey,
            live_trading_allowed=payload.executionPosture == "live",
            kill_switch_active=False,
        )
        audit = self._save_onboarding_audit(
            account_id=account.accountId,
            candidate=candidate,
            payload=payload,
            actor=actor,
            request_id=request_id,
            granted_roles=granted_roles,
            created=created,
            reenabled=reenabled,
        )

        refresh_action = None
        if payload.initialRefresh:
            refresh_action = self._require_refresh_service().action_response(
                account_id=account.accountId,
                action="refresh",
                trigger="manual",
            )

        try:
            detail_response = self._operations_service.get_account(account.accountId)
            broker_account = detail_response.account
            configuration = detail_response.configuration
        except BrokerAccountOperationsError as exc:
            raise BrokerAccountOnboardingError(exc.status_code, exc.detail) from exc

        message = "Broker account re-enabled." if reenabled else "Broker account onboarded."
        if refresh_action is not None:
            message = f"{message} {refresh_action.message}".strip()
        return BrokerAccountOnboardingResponse(
            account=broker_account,
            configuration=configuration,
            created=created,
            reenabled=reenabled,
            refreshAction=refresh_action,
            audit=audit,
            message=message,
            generatedAt=utc_now(),
        )

    def _discover_candidates(
        self,
        *,
        provider: Provider,
        environment: Environment,
        actor: str | None,
        granted_roles: list[str],
    ) -> list[DiscoveredOnboardingCandidate]:
        if provider == "alpaca":
            rows = self._discover_alpaca(environment=environment, actor=actor)
        elif provider == "etrade":
            rows = self._discover_etrade(environment=environment, actor=actor)
        else:
            rows = self._discover_schwab(environment=environment, actor=actor)
        return [
            self._decorate_candidate(
                provider=provider,
                environment=environment,
                provider_account_key=row["providerAccountKey"],
                suggested_account_id=row["suggestedAccountId"],
                display_name=row["displayName"],
                account_number_masked=row["accountNumberMasked"],
                base_currency=row["baseCurrency"],
                granted_roles=granted_roles,
                metadata=row["metadata"],
                provider_key_required=row["providerKeyRequired"],
            )
            for row in rows
        ]

    def _discover_alpaca(self, *, environment: Environment, actor: str | None) -> list[dict[str, Any]]:
        if environment == "sandbox":
            raise BrokerAccountOnboardingError(400, "Alpaca onboarding supports paper and live environments.")
        if self._alpaca_gateway is None:
            raise BrokerAccountOnboardingError(503, "Alpaca gateway is not initialized.")

        payload = self._alpaca_gateway.get_account(environment=environment, subject=actor or "account-onboarding")
        account_number = _first_present(payload, "accountNumber", "account_number", "accountId", "id")
        provider_key = str(account_number or f"alpaca:{environment}").strip()
        display_name = f"Alpaca {environment.title()}"
        return [
            {
                "providerAccountKey": provider_key,
                "suggestedAccountId": f"alpaca-{environment}",
                "displayName": display_name,
                "accountNumberMasked": _mask_account(account_number),
                "baseCurrency": str(_first_present(payload, "currency", "baseCurrency") or "USD").upper(),
                "providerKeyRequired": False,
                "metadata": {
                    "provider": "alpaca",
                    "environment": environment,
                    "accountNumberMasked": _mask_account(account_number),
                    "status": str(_first_present(payload, "status") or ""),
                },
            }
        ]

    def _discover_etrade(self, *, environment: Environment, actor: str | None) -> list[dict[str, Any]]:
        if environment == "paper":
            raise BrokerAccountOnboardingError(400, "E*TRADE onboarding supports sandbox and live environments.")
        if self._etrade_gateway is None:
            raise BrokerAccountOnboardingError(503, "E*TRADE gateway is not initialized.")

        session = self._etrade_gateway.get_session_state(environment=environment)
        if not session.get("configured"):
            raise BrokerAccountOnboardingError(503, f"E*TRADE {environment} credentials are not configured.")
        if not session.get("connected"):
            raise BrokerAccountOnboardingError(424, f"E*TRADE {environment} OAuth session is not connected.")

        payload = self._etrade_gateway.list_accounts(environment=environment, subject=actor or "account-onboarding")
        accounts = _collect_named_nodes(payload, "Account", "account")
        if not accounts and _as_mapping(payload):
            accounts = [_as_mapping(payload)]

        rows: list[dict[str, Any]] = []
        for index, account in enumerate(accounts, start=1):
            provider_key = str(_first_present(account, "accountIdKey", "accountKey") or "").strip() or None
            account_number = _first_present(account, "accountId", "accountNumber")
            display = str(
                _first_present(account, "accountName", "accountDesc", "accountType")
                or f"E*TRADE {_mask_account(account_number) or f'Account {index}'}"
            ).strip()
            key_for_id = provider_key or str(account_number or display or index)
            rows.append(
                {
                    "providerAccountKey": provider_key,
                    "suggestedAccountId": f"etrade-{environment}-{_hash(key_for_id)}",
                    "displayName": display[:128],
                    "accountNumberMasked": _mask_account(account_number),
                    "baseCurrency": "USD",
                    "providerKeyRequired": True,
                    "metadata": {
                        "provider": "etrade",
                        "environment": environment,
                        "accountNumberMasked": _mask_account(account_number),
                        "accountType": str(_first_present(account, "accountType") or ""),
                    },
                }
            )
        return rows

    def _discover_schwab(self, *, environment: Environment, actor: str | None) -> list[dict[str, Any]]:
        if environment != "live":
            raise BrokerAccountOnboardingError(400, "Schwab onboarding supports the live environment.")
        if self._schwab_gateway is None:
            raise BrokerAccountOnboardingError(503, "Schwab gateway is not initialized.")

        session = self._schwab_gateway.get_session_state()
        if not session.get("configured"):
            raise BrokerAccountOnboardingError(503, "Schwab credentials are not configured.")
        if not session.get("connected"):
            raise BrokerAccountOnboardingError(424, "Schwab OAuth session is not connected.")

        payload = self._schwab_gateway.get_account_numbers(subject=actor or "account-onboarding")
        accounts = _as_list(payload)
        rows: list[dict[str, Any]] = []
        for index, account in enumerate(accounts, start=1):
            mapping = _as_mapping(account)
            provider_key = str(_first_present(mapping, "hashValue") or "").strip() or None
            account_number = _first_present(mapping, "accountNumber")
            key_for_id = provider_key or str(account_number or index)
            rows.append(
                {
                    "providerAccountKey": provider_key,
                    "suggestedAccountId": f"schwab-live-{_hash(key_for_id)}",
                    "displayName": f"Schwab {_mask_account(account_number) or f'Account {index}'}",
                    "accountNumberMasked": _mask_account(account_number),
                    "baseCurrency": "USD",
                    "providerKeyRequired": True,
                    "metadata": {
                        "provider": "schwab",
                        "environment": environment,
                        "accountNumberMasked": _mask_account(account_number),
                    },
                }
            )
        return rows

    def _decorate_candidate(
        self,
        *,
        provider: Provider,
        environment: Environment,
        provider_account_key: str | None,
        suggested_account_id: str,
        display_name: str,
        account_number_masked: str | None,
        base_currency: str,
        granted_roles: list[str],
        metadata: dict[str, Any],
        provider_key_required: bool,
    ) -> DiscoveredOnboardingCandidate:
        allowed_postures, blocked_reasons = self._execution_postures(
            environment=environment,
            account_id=suggested_account_id,
            granted_roles=granted_roles,
        )
        state = "available"
        state_reason = None
        existing_account_id = None
        can_onboard = True

        existing = self._trade_repo.get_account_seed_state(suggested_account_id)
        if existing is not None and existing.enabled:
            state = "already_configured"
            state_reason = "Account is already configured."
            existing_account_id = existing.accountId
            can_onboard = False
        elif existing is not None:
            state = "disabled"
            state_reason = "Account is disabled and can be re-enabled."
            existing_account_id = existing.accountId

        account_allowlist = self._account_allowlist()
        if can_onboard and account_allowlist and suggested_account_id not in account_allowlist:
            state = "blocked"
            state_reason = "Account id is not in TRADE_DESK_ACCOUNT_ALLOWLIST."
            can_onboard = False

        if can_onboard and provider_key_required and not provider_account_key:
            state = "unavailable"
            state_reason = "Broker discovery did not return a provider account key."
            can_onboard = False

        contract = BrokerAccountOnboardingCandidate(
            candidateId=self._candidate_id(provider, environment, suggested_account_id),
            provider=provider,
            environment=environment,
            suggestedAccountId=suggested_account_id,
            displayName=display_name,
            accountNumberMasked=account_number_masked,
            baseCurrency=base_currency or "USD",
            state=state,
            stateReason=state_reason,
            existingAccountId=existing_account_id,
            allowedExecutionPostures=allowed_postures,
            blockedExecutionPostureReasons=blocked_reasons,
            canOnboard=can_onboard,
        )
        return DiscoveredOnboardingCandidate(
            contract=contract,
            providerAccountKey=provider_account_key,
            metadata=metadata,
        )

    def _execution_postures(
        self,
        *,
        environment: Environment,
        account_id: str,
        granted_roles: list[str],
    ) -> tuple[list[Posture], dict[str, str]]:
        allowed: list[Posture] = ["monitor_only"]
        blocked: dict[str, str] = {}

        if environment == "paper" and self._settings.paper_execution_enabled:
            allowed.append("paper")
        else:
            blocked["paper"] = "Paper posture requires environment=paper and paper execution enabled."

        if environment == "sandbox" and self._settings.sandbox_execution_enabled:
            allowed.append("sandbox")
        else:
            blocked["sandbox"] = "Sandbox posture requires environment=sandbox and sandbox execution enabled."

        live_reason = self._live_posture_block_reason(account_id=account_id, environment=environment, granted_roles=granted_roles)
        if live_reason:
            blocked["live"] = live_reason
        else:
            allowed.append("live")

        return allowed, {posture: reason for posture, reason in blocked.items() if posture not in allowed}

    def _live_posture_block_reason(
        self,
        *,
        account_id: str,
        environment: Environment,
        granted_roles: list[str],
    ) -> str | None:
        if environment != "live":
            return "Live posture requires environment=live."
        if self._settings.global_kill_switch:
            return "Global trade desk kill switch is active."
        if self._settings.live_kill_switch:
            return "Live trading kill switch is active."
        if not self._settings.live_execution_enabled:
            return "Live execution is disabled."
        if account_id not in set(self._settings.live_account_allowlist):
            return "Account is not allowlisted for live trading."
        missing_roles = sorted(
            role for role in self._settings.live_required_roles if role and role not in set(granted_roles)
        )
        if missing_roles:
            return f"Missing required live-trade roles: {', '.join(missing_roles)}."
        return None

    def _seed_trade_account(
        self,
        *,
        candidate: BrokerAccountOnboardingCandidate,
        display_name: str,
        readiness: str,
        posture: Posture,
    ) -> TradeAccountSummary:
        now = utc_now()
        can_trade = posture in {"paper", "sandbox", "live"}
        capabilities = TradeCapabilityFlags(
            canReadAccount=True,
            canReadPositions=True,
            canReadOrders=True,
            canReadHistory=True,
            canPreview=can_trade,
            canSubmitPaper=posture == "paper",
            canSubmitSandbox=posture == "sandbox",
            canSubmitLive=posture == "live",
            canCancel=can_trade,
            supportsMarketOrders=True,
            supportsLimitOrders=True,
            supportsEquities=True,
            supportsEtfs=True,
            readOnly=not can_trade,
            unsupportedReason="Account is monitor-only." if not can_trade else None,
        )
        return TradeAccountSummary(
            accountId=candidate.suggestedAccountId,
            name=display_name,
            provider=candidate.provider,
            environment=candidate.environment,
            accountNumberMasked=candidate.accountNumberMasked,
            baseCurrency=candidate.baseCurrency,
            readiness=readiness,
            readinessReason="Seeded through broker onboarding; refresh required.",
            capabilities=capabilities,
            cash=0.0,
            buyingPower=0.0,
            equity=0.0,
            openOrderCount=0,
            positionCount=0,
            unresolvedAlertCount=0,
            killSwitchActive=False,
            snapshotAsOf=now,
            freshness=TradeDataFreshness(
                balancesState="unknown",
                positionsState="unknown",
                ordersState="unknown",
                staleReason="Seeded through broker onboarding; initial refresh pending.",
            ),
        )

    def _save_onboarding_audit(
        self,
        *,
        account_id: str,
        candidate: DiscoveredOnboardingCandidate,
        payload: BrokerAccountOnboardingRequest,
        actor: str | None,
        request_id: str | None,
        granted_roles: list[str],
        created: bool,
        reenabled: bool,
    ) -> BrokerAccountConfigurationAuditRecord:
        return self._configuration_repo.save_audit(
            account_id=account_id,
            category="onboarding",
            outcome="saved",
            actor=actor,
            request_id=request_id,
            granted_roles=granted_roles,
            summary="Onboarded broker account.",
            before={},
            after={
                "candidate": candidate.contract.model_dump(mode="json"),
                "metadata": candidate.metadata,
                "readiness": payload.readiness,
                "executionPosture": payload.executionPosture,
                "initialRefresh": payload.initialRefresh,
                "operatorReason": payload.reason,
                "created": created,
                "reenabled": reenabled,
            },
            denial_reason=None,
        )

    def _configuration_or_none(self, account_id: str) -> BrokerAccountConfiguration | None:
        try:
            return self._configuration_service.get_configuration(account_id)
        except BrokerAccountConfigurationError as exc:
            if exc.status_code == 404:
                return None
            raise BrokerAccountOnboardingError(exc.status_code, exc.detail) from exc

    def _require_refresh_service(self) -> BrokerAccountStatusRefreshService:
        if self._refresh_service is None:
            raise BrokerAccountOnboardingError(503, "Broker account status refresh service is not initialized.")
        return self._refresh_service

    def _account_allowlist(self) -> set[str]:
        return {account_id.strip() for account_id in self._settings.account_allowlist if account_id.strip()}

    @staticmethod
    def _candidate_id(provider: Provider, environment: Environment, account_id: str) -> str:
        return f"{provider}:{environment}:{_hash(account_id)}"

    @staticmethod
    def _normalize_provider(provider: str) -> Provider:
        normalized = str(provider or "").strip().lower()
        if normalized not in _PROVIDERS:
            raise BrokerAccountOnboardingError(400, "provider must be one of alpaca, etrade, or schwab.")
        return normalized  # type: ignore[return-value]

    @staticmethod
    def _normalize_environment(environment: str) -> Environment:
        normalized = str(environment or "").strip().lower()
        if normalized not in _ENVIRONMENTS:
            raise BrokerAccountOnboardingError(400, "environment must be one of paper, sandbox, or live.")
        return normalized  # type: ignore[return-value]
