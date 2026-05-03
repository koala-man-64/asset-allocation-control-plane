from __future__ import annotations

import logging
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field, ValidationError, model_validator

from api.service.dependencies import validate_auth
from core.config_library_repository import ConfigLibraryRepository
from core.regime_repository import RegimeRepository
from core.strategy_engine.contracts import ExitRule, IntrabarConflictPolicy, RebalancePolicy, StrategyRiskPolicy

logger = logging.getLogger(__name__)

regime_policy_router = APIRouter()
risk_policy_router = APIRouter()
exit_rule_set_router = APIRouter()
rebalance_policy_router = APIRouter()


class ConfigUpsertRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    description: str = Field(default="", max_length=2048)
    status: Literal["draft", "active", "deprecated"] = "active"
    intendedUse: Literal["research", "validation", "production_candidate"] = "research"
    thesis: str = Field(default="", max_length=4096)
    whatToMonitor: list[str] = Field(default_factory=list)
    config: dict[str, Any]


class RegimePolicyConfigModel(BaseModel):
    modelName: str = Field(default="default-regime", min_length=1, max_length=128)
    modelVersion: int = Field(..., ge=1)
    mode: Literal["observe_only"] = "observe_only"

    @model_validator(mode="after")
    def normalize_model_name(self) -> "RegimePolicyConfigModel":
        self.modelName = str(self.modelName or "").strip() or "default-regime"
        return self


class RiskPolicyConfigModel(BaseModel):
    policy: StrategyRiskPolicy


class RebalancePolicyConfigModel(RebalancePolicy):
    @model_validator(mode="after")
    def validate_reusable_policy(self) -> "RebalancePolicyConfigModel":
        if self.cadence is None or self.dayRule is None or self.anchor is None:
            raise ValueError("Reusable rebalance policies require cadence, dayRule, and anchor.")
        return self


class ExitRuleSetConfigModel(BaseModel):
    intrabarConflictPolicy: IntrabarConflictPolicy = "stop_first"
    exits: list[ExitRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def normalize_exits(self) -> "ExitRuleSetConfigModel":
        seen_ids: set[str] = set()
        for idx, rule in enumerate(self.exits):
            if rule.id in seen_ids:
                raise ValueError(f"Duplicate exit rule id '{rule.id}'.")
            seen_ids.add(rule.id)
            if rule.priority is None:
                rule.priority = idx
        return self


def _require_postgres_dsn(request: Request) -> str:
    dsn = str(request.app.state.settings.postgres_dsn or "").strip()
    if not dsn:
        raise HTTPException(status_code=503, detail="Postgres is required for strategy configuration libraries.")
    return dsn


def _shape_detail(repo: ConfigLibraryRepository, family_key: str, name: str, summary_key: str) -> dict[str, Any]:
    record = repo.get_config(family_key, name)
    if not record:
        raise HTTPException(status_code=404, detail=f"Configuration '{name}' not found.")
    return {
        summary_key: record,
        "activeRevision": repo.get_revision(family_key, name),
        "revisions": repo.list_revisions(family_key, name),
    }


def _save_config(
    repo: ConfigLibraryRepository,
    family_key: str,
    payload: ConfigUpsertRequest,
    config: dict[str, Any],
) -> dict[str, Any]:
    saved = repo.save_config(
        family_key,
        name=payload.name.strip(),
        description=payload.description.strip(),
        config=config,
        status=payload.status,
        intended_use=payload.intendedUse,
        thesis=payload.thesis.strip(),
        what_to_monitor=[str(item).strip() for item in payload.whatToMonitor if str(item).strip()],
    )
    return {
        "status": "success",
        "message": f"Configuration '{payload.name}' saved successfully",
        "version": int(saved["version"]),
    }


@regime_policy_router.get("/")
async def list_regime_policies(request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    return {"policies": repo.list_configs("regimePolicy")}


@regime_policy_router.get("/{name}/detail")
async def get_regime_policy_detail(name: str, request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    return _shape_detail(repo, "regimePolicy", name, "policy")


@regime_policy_router.get("/{name}/revisions/{version}")
async def get_regime_policy_revision(name: str, version: int, request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    revision = repo.get_revision("regimePolicy", name, version=version)
    if not revision:
        raise HTTPException(status_code=404, detail=f"Regime policy '{name}' version {version} not found.")
    return revision


@regime_policy_router.post("/")
async def save_regime_policy(payload: ConfigUpsertRequest, request: Request) -> dict[str, Any]:
    validate_auth(request)
    dsn = _require_postgres_dsn(request)
    try:
        config = RegimePolicyConfigModel.model_validate(payload.config).model_dump(mode="json")
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    if not RegimeRepository(dsn).get_regime_model_revision(config["modelName"], version=int(config["modelVersion"])):
        raise HTTPException(
            status_code=400,
            detail=f"Regime model '{config['modelName']}' version {config['modelVersion']} not found.",
        )
    repo = ConfigLibraryRepository(dsn)
    return _save_config(repo, "regimePolicy", payload, config)


@regime_policy_router.delete("/{name}")
async def archive_regime_policy(name: str, request: Request) -> dict[str, str]:
    validate_auth(request)
    archived = ConfigLibraryRepository(_require_postgres_dsn(request)).archive_config("regimePolicy", name)
    if not archived:
        raise HTTPException(status_code=404, detail=f"Regime policy '{name}' not found.")
    return {"status": "success", "message": f"Regime policy '{name}' archived successfully"}


@risk_policy_router.get("/")
async def list_risk_policies(request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    return {"policies": repo.list_configs("riskPolicy")}


@risk_policy_router.get("/{name}/detail")
async def get_risk_policy_detail(name: str, request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    return _shape_detail(repo, "riskPolicy", name, "policy")


@risk_policy_router.get("/{name}/revisions/{version}")
async def get_risk_policy_revision(name: str, version: int, request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    revision = repo.get_revision("riskPolicy", name, version=version)
    if not revision:
        raise HTTPException(status_code=404, detail=f"Risk policy '{name}' version {version} not found.")
    return revision


@risk_policy_router.post("/")
async def save_risk_policy(payload: ConfigUpsertRequest, request: Request) -> dict[str, Any]:
    validate_auth(request)
    try:
        config = RiskPolicyConfigModel.model_validate(payload.config).model_dump(mode="json")
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    return _save_config(repo, "riskPolicy", payload, config)


@risk_policy_router.delete("/{name}")
async def archive_risk_policy(name: str, request: Request) -> dict[str, str]:
    validate_auth(request)
    archived = ConfigLibraryRepository(_require_postgres_dsn(request)).archive_config("riskPolicy", name)
    if not archived:
        raise HTTPException(status_code=404, detail=f"Risk policy '{name}' not found.")
    return {"status": "success", "message": f"Risk policy '{name}' archived successfully"}


@rebalance_policy_router.get("/")
async def list_rebalance_policies(request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    return {"policies": repo.list_configs("rebalancePolicy")}


@rebalance_policy_router.get("/{name}/detail")
async def get_rebalance_policy_detail(name: str, request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    return _shape_detail(repo, "rebalancePolicy", name, "policy")


@rebalance_policy_router.get("/{name}/revisions/{version}")
async def get_rebalance_policy_revision(name: str, version: int, request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    revision = repo.get_revision("rebalancePolicy", name, version=version)
    if not revision:
        raise HTTPException(status_code=404, detail=f"Rebalance policy '{name}' version {version} not found.")
    return revision


@rebalance_policy_router.post("/")
async def save_rebalance_policy(payload: ConfigUpsertRequest, request: Request) -> dict[str, Any]:
    validate_auth(request)
    raw_config = payload.config.get("policy") if isinstance(payload.config.get("policy"), dict) else payload.config
    try:
        config = RebalancePolicyConfigModel.model_validate(raw_config).model_dump(mode="json")
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    return _save_config(repo, "rebalancePolicy", payload, config)


@rebalance_policy_router.delete("/{name}")
async def archive_rebalance_policy(name: str, request: Request) -> dict[str, str]:
    validate_auth(request)
    archived = ConfigLibraryRepository(_require_postgres_dsn(request)).archive_config("rebalancePolicy", name)
    if not archived:
        raise HTTPException(status_code=404, detail=f"Rebalance policy '{name}' not found.")
    return {"status": "success", "message": f"Rebalance policy '{name}' archived successfully"}


@exit_rule_set_router.get("/")
async def list_exit_rule_sets(request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    return {"ruleSets": repo.list_configs("exitRuleSet")}


@exit_rule_set_router.get("/{name}/detail")
async def get_exit_rule_set_detail(name: str, request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    return _shape_detail(repo, "exitRuleSet", name, "ruleSet")


@exit_rule_set_router.get("/{name}/revisions/{version}")
async def get_exit_rule_set_revision(name: str, version: int, request: Request) -> dict[str, Any]:
    validate_auth(request)
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    revision = repo.get_revision("exitRuleSet", name, version=version)
    if not revision:
        raise HTTPException(status_code=404, detail=f"Exit rule set '{name}' version {version} not found.")
    return revision


@exit_rule_set_router.post("/")
async def save_exit_rule_set(payload: ConfigUpsertRequest, request: Request) -> dict[str, Any]:
    validate_auth(request)
    try:
        config = ExitRuleSetConfigModel.model_validate(payload.config).model_dump(mode="json")
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    repo = ConfigLibraryRepository(_require_postgres_dsn(request))
    return _save_config(repo, "exitRuleSet", payload, config)


@exit_rule_set_router.delete("/{name}")
async def archive_exit_rule_set(name: str, request: Request) -> dict[str, str]:
    validate_auth(request)
    archived = ConfigLibraryRepository(_require_postgres_dsn(request)).archive_config("exitRuleSet", name)
    if not archived:
        raise HTTPException(status_code=404, detail=f"Exit rule set '{name}' not found.")
    return {"status": "success", "message": f"Exit rule set '{name}' archived successfully"}
