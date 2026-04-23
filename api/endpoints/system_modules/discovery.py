from __future__ import annotations

from types import ModuleType
from typing import Any

from fastapi import APIRouter, Query, Request

from api.service.data_discovery import (
    DataDiscoveryCatalogResponse,
    DataDiscoveryDatasetDetailResponse,
    DataDiscoverySampleResponse,
    build_data_discovery_catalog,
    build_data_discovery_dataset_detail,
    build_data_discovery_sample,
)


def _runtime_attr(runtime: ModuleType, name: str) -> Any:
    return getattr(runtime, name)


def build_router(*, runtime: ModuleType) -> tuple[APIRouter, dict[str, Any]]:
    router = APIRouter()

    @router.get("/discovery/catalog", response_model=DataDiscoveryCatalogResponse)
    def get_discovery_catalog(request: Request) -> DataDiscoveryCatalogResponse:
        require_data_discovery_read_access = _runtime_attr(runtime, "require_data_discovery_read_access")
        require_data_discovery_read_access(request)
        return build_data_discovery_catalog(request)

    @router.get(
        "/discovery/datasets/{schema_name}/{table_name}",
        response_model=DataDiscoveryDatasetDetailResponse,
    )
    def get_discovery_dataset_detail(
        schema_name: str,
        table_name: str,
        request: Request,
    ) -> DataDiscoveryDatasetDetailResponse:
        require_data_discovery_read_access = _runtime_attr(runtime, "require_data_discovery_read_access")
        require_data_discovery_read_access(request)
        return build_data_discovery_dataset_detail(
            request,
            schema_name=schema_name,
            table_name=table_name,
        )

    @router.get(
        "/discovery/datasets/{schema_name}/{table_name}/sample",
        response_model=DataDiscoverySampleResponse,
    )
    def get_discovery_dataset_sample(
        schema_name: str,
        table_name: str,
        request: Request,
        limit: int = Query(default=10, ge=1),
    ) -> DataDiscoverySampleResponse:
        require_data_discovery_read_access = _runtime_attr(runtime, "require_data_discovery_read_access")
        require_data_discovery_read_access(request)
        return build_data_discovery_sample(
            request,
            schema_name=schema_name,
            table_name=table_name,
            limit=limit,
        )

    return router, {
        "get_discovery_catalog": get_discovery_catalog,
        "get_discovery_dataset_detail": get_discovery_dataset_detail,
        "get_discovery_dataset_sample": get_discovery_dataset_sample,
    }
