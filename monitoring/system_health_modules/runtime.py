from __future__ import annotations

import importlib
import sys
from types import ModuleType
from typing import Any


def module() -> ModuleType:
    runtime = sys.modules.get("monitoring.system_health")
    if runtime is None:
        runtime = importlib.import_module("monitoring.system_health")
    return runtime


def attr(runtime: ModuleType, name: str) -> Any:
    return getattr(runtime, name)
