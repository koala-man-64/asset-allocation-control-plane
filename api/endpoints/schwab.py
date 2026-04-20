from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from api.openapi_models import ProviderCallbackUrlResponse, SchwabCallbackReceiptResponse
from api.service.auth import AuthContext
from api.service.dependencies import get_settings, validate_auth


logger = logging.getLogger("asset-allocation.api.schwab")
router = APIRouter()


def _require_provider_callback_url(request: Request) -> str:
    callback_url = get_settings(request).get_provider_callback_url("schwab")
    if callback_url:
        return callback_url
    raise HTTPException(
        status_code=503,
        detail="Schwab callback URL is not configured. Set SCHWAB_APP_CALLBACK_URL or API_PUBLIC_BASE_URL.",
    )


@router.get("/connect/callback", response_model=SchwabCallbackReceiptResponse)
def schwab_connect_callback(
    code: str = Query(..., min_length=1),
    state: str | None = Query(default=None),
    session: str | None = Query(default=None),
) -> JSONResponse:
    del code
    logger.info(
        "Schwab authorization callback received: has_state=%s has_session=%s",
        bool(state),
        bool(session),
    )
    payload = SchwabCallbackReceiptResponse()
    return JSONResponse(payload.model_dump(mode="json"), headers={"Cache-Control": "no-store"})


@router.get("/connect/callback-url", response_model=ProviderCallbackUrlResponse)
def schwab_connect_callback_url(
    request: Request,
    _auth_context: AuthContext = Depends(validate_auth),
) -> JSONResponse:
    payload = ProviderCallbackUrlResponse(callback_url=_require_provider_callback_url(request))
    return JSONResponse(payload.model_dump(mode="json"), headers={"Cache-Control": "no-store"})
