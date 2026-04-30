from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response

from asset_allocation_contracts.broker_accounts import (
    AcknowledgeBrokerAlertRequest,
    BrokerAccountAllocationUpdateRequest,
    BrokerAccountActionResponse,
    BrokerAccountConfiguration,
    BrokerAccountDetail,
    BrokerAccountListResponse,
    BrokerTradingPolicyUpdateRequest,
    PauseBrokerSyncRequest,
    ReconnectBrokerAccountRequest,
    RefreshBrokerAccountRequest,
)

from api.service.auth import AuthContext
from api.service.broker_account_configuration_service import (
    BrokerAccountConfigurationError,
    BrokerAccountConfigurationService,
)
from api.service.broker_account_operations_service import (
    BrokerAccountOperationsError,
    BrokerAccountOperationsService,
)
from api.service.dependencies import (
    require_account_policy_read_access,
    require_account_policy_write_access,
    require_trade_desk_read_access,
)
from core.broker_account_configuration_repository import BrokerAccountConfigurationRepository
from core.portfolio_repository import PortfolioRepository
from core.trade_desk_repository import TradeDeskRepository

router = APIRouter()


def _set_no_store(response: Response) -> None:
    response.headers["Cache-Control"] = "no-store"


def _service(request: Request) -> BrokerAccountConfigurationService:
    dsn = str(request.app.state.settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(
            status_code=503,
            detail="Postgres is required for broker account configuration endpoints.",
            headers={"Cache-Control": "no-store"},
        )
    return BrokerAccountConfigurationService(
        BrokerAccountConfigurationRepository(dsn),
        TradeDeskRepository(dsn),
        PortfolioRepository(dsn),
    )


def _operations_service(request: Request) -> BrokerAccountOperationsService:
    dsn = str(request.app.state.settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(
            status_code=503,
            detail="Postgres is required for broker account operations endpoints.",
            headers={"Cache-Control": "no-store"},
        )
    trade_repo = TradeDeskRepository(dsn)
    return BrokerAccountOperationsService(
        trade_repo,
        request.app.state.settings.trade_desk,
        BrokerAccountConfigurationService(
            BrokerAccountConfigurationRepository(dsn),
            trade_repo,
            PortfolioRepository(dsn),
        ),
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
        raise HTTPException(
            status_code=exc.status_code,
            detail=exc.detail,
            headers={"Cache-Control": "no-store"},
        ) from exc
    detail = str(exc)
    status_code = 409 if "Configuration version conflict" in detail else 400
    raise HTTPException(status_code=status_code, detail=detail, headers={"Cache-Control": "no-store"}) from exc


def _handle_operations_error(exc: BrokerAccountOperationsError) -> None:
    raise HTTPException(
        status_code=exc.status_code,
        detail=exc.detail,
        headers={"Cache-Control": "no-store"},
    ) from exc


@router.get("/broker-accounts", response_model=BrokerAccountListResponse)
async def list_broker_accounts(
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_trade_desk_read_access),
) -> BrokerAccountListResponse:
    _set_no_store(response)
    try:
        return _operations_service(request).list_accounts()
    except BrokerAccountOperationsError as exc:
        _handle_operations_error(exc)
        raise


@router.get("/broker-accounts/{account_id}", response_model=BrokerAccountDetail)
async def get_broker_account(
    account_id: str,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_trade_desk_read_access),
) -> BrokerAccountDetail:
    _set_no_store(response)
    try:
        return _operations_service(request).get_account(account_id)
    except BrokerAccountOperationsError as exc:
        _handle_operations_error(exc)
        raise


@router.post("/broker-accounts/{account_id}/reconnect", response_model=BrokerAccountActionResponse)
async def reconnect_broker_account(
    account_id: str,
    _payload: ReconnectBrokerAccountRequest,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_account_policy_write_access),
) -> BrokerAccountActionResponse:
    _set_no_store(response)
    try:
        _operations_service(request).reconnect_account(account_id)
    except BrokerAccountOperationsError as exc:
        _handle_operations_error(exc)
        raise
    raise HTTPException(status_code=501, detail="Broker account reconnect is not implemented in Account Operations v1.")


@router.post("/broker-accounts/{account_id}/sync/pause", response_model=BrokerAccountActionResponse)
async def pause_broker_account_sync(
    account_id: str,
    _payload: PauseBrokerSyncRequest,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_account_policy_write_access),
) -> BrokerAccountActionResponse:
    _set_no_store(response)
    try:
        _operations_service(request).set_sync_paused(account_id, paused=True)
    except BrokerAccountOperationsError as exc:
        _handle_operations_error(exc)
        raise
    raise HTTPException(status_code=501, detail="Broker account sync pause is not implemented in Account Operations v1.")


@router.post("/broker-accounts/{account_id}/sync/resume", response_model=BrokerAccountActionResponse)
async def resume_broker_account_sync(
    account_id: str,
    _payload: PauseBrokerSyncRequest,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_account_policy_write_access),
) -> BrokerAccountActionResponse:
    _set_no_store(response)
    try:
        _operations_service(request).set_sync_paused(account_id, paused=False)
    except BrokerAccountOperationsError as exc:
        _handle_operations_error(exc)
        raise
    raise HTTPException(status_code=501, detail="Broker account sync resume is not implemented in Account Operations v1.")


@router.post("/broker-accounts/{account_id}/refresh", response_model=BrokerAccountActionResponse)
async def refresh_broker_account(
    account_id: str,
    _payload: RefreshBrokerAccountRequest,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_account_policy_write_access),
) -> BrokerAccountActionResponse:
    _set_no_store(response)
    try:
        _operations_service(request).refresh_account(account_id)
    except BrokerAccountOperationsError as exc:
        _handle_operations_error(exc)
        raise
    raise HTTPException(status_code=501, detail="Broker account refresh is not implemented in Account Operations v1.")


@router.post(
    "/broker-accounts/{account_id}/alerts/{alert_id}/acknowledge",
    response_model=BrokerAccountActionResponse,
)
async def acknowledge_broker_account_alert(
    account_id: str,
    alert_id: str,
    _payload: AcknowledgeBrokerAlertRequest,
    request: Request,
    response: Response,
    _auth_context: AuthContext = Depends(require_account_policy_write_access),
) -> BrokerAccountActionResponse:
    _set_no_store(response)
    try:
        _operations_service(request).acknowledge_alert(account_id, alert_id)
    except BrokerAccountOperationsError as exc:
        _handle_operations_error(exc)
        raise
    raise HTTPException(
        status_code=501,
        detail="Broker account alert acknowledgement is not implemented in Account Operations v1.",
    )


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
