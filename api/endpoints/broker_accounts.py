from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from asset_allocation_contracts.broker_accounts import (
    BrokerAccountAllocationUpdateRequest,
    BrokerAccountConfiguration,
    BrokerTradingPolicyUpdateRequest,
)

from api.service.auth import AuthContext
from api.service.broker_account_configuration_service import (
    BrokerAccountConfigurationError,
    BrokerAccountConfigurationService,
)
from api.service.dependencies import require_account_policy_read_access, require_account_policy_write_access
from core.broker_account_configuration_repository import BrokerAccountConfigurationRepository
from core.portfolio_repository import PortfolioRepository
from core.trade_desk_repository import TradeDeskRepository

router = APIRouter()


def _set_no_store(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store"


def _service(request: Request) -> BrokerAccountConfigurationService:
    dsn = str(request.app.state.settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for broker account configuration endpoints.")
    return BrokerAccountConfigurationService(
        BrokerAccountConfigurationRepository(dsn),
        TradeDeskRepository(dsn),
        PortfolioRepository(dsn),
    )


def _actor(auth_context: AuthContext) -> str | None:
    return str(auth_context.subject or "").strip() or None


def _request_id(request: Request) -> str | None:
    return str(getattr(request.state, "request_id", "") or request.headers.get("x-request-id", "")).strip() or None


def _granted_roles(auth_context: AuthContext) -> list[str]:
    roles = auth_context.claims.get("roles") if isinstance(auth_context.claims, dict) else []
    if not isinstance(roles, list):
        return []
    return [str(role).strip() for role in roles if str(role).strip()]


def _handle_configuration_error(exc: BrokerAccountConfigurationError | ValueError) -> None:
    if isinstance(exc, BrokerAccountConfigurationError):
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    detail = str(exc)
    status_code = 409 if "Configuration version conflict" in detail else 400
    raise HTTPException(status_code=status_code, detail=detail) from exc


@router.get("/broker-accounts/{account_id}/configuration", response_model=BrokerAccountConfiguration)
async def get_broker_account_configuration(
    account_id: str,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_account_policy_read_access),
) -> BrokerAccountConfiguration:
    _set_no_store(response)
    try:
        return _service(request).get_configuration(account_id)
    except (BrokerAccountConfigurationError, ValueError) as exc:
        _handle_configuration_error(exc)
        raise


@router.put("/broker-accounts/{account_id}/trading-policy", response_model=BrokerAccountConfiguration)
async def save_broker_account_trading_policy(
    account_id: str,
    payload: BrokerTradingPolicyUpdateRequest,
    request: Request,
    response: Response,
    auth_context: AuthContext = Depends(require_account_policy_write_access),
) -> BrokerAccountConfiguration:
    _set_no_store(response)
    try:
        return _service(request).save_trading_policy(
            account_id,
            payload,
            actor=_actor(auth_context),
            request_id=_request_id(request),
            granted_roles=_granted_roles(auth_context),
        )
    except (BrokerAccountConfigurationError, ValueError) as exc:
        _handle_configuration_error(exc)
        raise


@router.put("/broker-accounts/{account_id}/allocation", response_model=BrokerAccountConfiguration)
async def save_broker_account_allocation(
    account_id: str,
    payload: BrokerAccountAllocationUpdateRequest,
    request: Request,
    response: Response,
    auth_context: AuthContext = Depends(require_account_policy_write_access),
) -> BrokerAccountConfiguration:
    _set_no_store(response)
    try:
        return _service(request).save_allocation(
            account_id,
            payload,
            actor=_actor(auth_context),
            request_id=_request_id(request),
            granted_roles=_granted_roles(auth_context),
        )
    except (BrokerAccountConfigurationError, ValueError) as exc:
        _handle_configuration_error(exc)
        raise
