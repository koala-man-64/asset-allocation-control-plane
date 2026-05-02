from __future__ import annotations

from asset_allocation_contracts.strategy import *  # noqa: F401,F403
from asset_allocation_contracts.regime import RegimePolicy as _SharedRegimePolicy
from asset_allocation_contracts.strategy import StrategyConfig as _SharedStrategyConfig
from pydantic import Field, model_validator


class RegimePolicy(_SharedRegimePolicy):
    modelVersion: int | None = Field(default=None, ge=1)


class StrategyConfig(_SharedStrategyConfig):
    universeConfigVersion: int | None = Field(default=None, ge=1)
    rankingSchemaVersion: int | None = Field(default=None, ge=1)
    regimePolicyConfigName: str | None = Field(default=None, min_length=1, max_length=128)
    regimePolicyConfigVersion: int | None = Field(default=None, ge=1)
    regimePolicy: RegimePolicy | None = None
    riskPolicyName: str | None = Field(default=None, min_length=1, max_length=128)
    riskPolicyVersion: int | None = Field(default=None, ge=1)
    exitRuleSetName: str | None = Field(default=None, min_length=1, max_length=128)
    exitRuleSetVersion: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def validate_config_pins(self) -> "StrategyConfig":
        self._validate_pin_pair("universeConfigVersion", self.universeConfigName, self.universeConfigVersion)
        self._validate_pin_pair("rankingSchemaVersion", self.rankingSchemaName, self.rankingSchemaVersion)
        self._validate_pin_pair(
            "regimePolicyConfigVersion",
            self.regimePolicyConfigName,
            self.regimePolicyConfigVersion,
        )
        self._validate_pin_pair("riskPolicyVersion", self.riskPolicyName, self.riskPolicyVersion)
        self._validate_pin_pair("exitRuleSetVersion", self.exitRuleSetName, self.exitRuleSetVersion)
        return self

    @staticmethod
    def _validate_pin_pair(field_name: str, name: str | None, version: int | None) -> None:
        if version is not None and not name:
            raise ValueError(f"{field_name} requires the matching config name.")
