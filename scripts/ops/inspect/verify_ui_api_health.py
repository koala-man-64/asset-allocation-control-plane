#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass

import requests


DEFAULT_LOCAL_API_BASE_URL = "http://localhost:9000"
DEFAULT_LOCAL_UI_BASE_URL = "http://localhost:5174"
DEFAULT_CLOUD_BASE_URL = os.environ.get("ASSET_ALLOCATION_PUBLIC_BASE_URL", "").strip()


def _join_url(base_url: str, path: str) -> str:
    base = (base_url or "").strip().rstrip("/")
    clean_path = "/" + (path or "").strip().lstrip("/")
    return f"{base}{clean_path}"


def _normalize_url(url: str) -> str:
    return (url or "").strip().rstrip("/")


@dataclass(frozen=True)
class ProbeResult:
    name: str
    url: str
    ok: bool
    elapsed_ms: float
    status_code: int | None = None
    detail: str | None = None
    error: str | None = None


def _probe_json_status(
    *,
    name: str,
    url: str,
    timeout_seconds: float,
    expected_status: str,
) -> ProbeResult:
    started = time.perf_counter()
    try:
        response = requests.get(url, timeout=timeout_seconds)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        response.raise_for_status()
    except requests.RequestException as exc:
        return ProbeResult(
            name=name,
            url=url,
            ok=False,
            elapsed_ms=(time.perf_counter() - started) * 1000.0,
            error=str(exc),
        )

    try:
        payload = response.json()
    except ValueError:
        return ProbeResult(
            name=name,
            url=url,
            ok=False,
            elapsed_ms=elapsed_ms,
            status_code=response.status_code,
            error="Expected JSON response.",
        )

    if not isinstance(payload, dict):
        return ProbeResult(
            name=name,
            url=url,
            ok=False,
            elapsed_ms=elapsed_ms,
            status_code=response.status_code,
            error=f"Expected JSON object, got {type(payload).__name__}.",
        )

    actual_status = payload.get("status")
    if str(actual_status).lower() != expected_status.lower():
        return ProbeResult(
            name=name,
            url=url,
            ok=False,
            elapsed_ms=elapsed_ms,
            status_code=response.status_code,
            error=f"Unexpected status value: {actual_status!r} (expected {expected_status!r}).",
            detail=str(payload),
        )

    return ProbeResult(
        name=name,
        url=url,
        ok=True,
        elapsed_ms=elapsed_ms,
        status_code=response.status_code,
        detail=f"status={actual_status}",
    )


def _probe_ui_root(*, url: str, timeout_seconds: float) -> ProbeResult:
    started = time.perf_counter()
    try:
        response = requests.get(url, timeout=timeout_seconds)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        response.raise_for_status()
    except requests.RequestException as exc:
        return ProbeResult(
            name="UI root",
            url=url,
            ok=False,
            elapsed_ms=(time.perf_counter() - started) * 1000.0,
            error=str(exc),
        )

    body = response.text or ""
    content_type = response.headers.get("Content-Type", "")
    looks_like_html = (
        "<html" in body.lower()
        or "text/html" in content_type.lower()
        or "<!doctype html" in body.lower()
    )
    if not looks_like_html:
        return ProbeResult(
            name="UI root",
            url=url,
            ok=False,
            elapsed_ms=elapsed_ms,
            status_code=response.status_code,
            error="Response does not look like HTML.",
            detail=f"content_type={content_type!r}",
        )

    return ProbeResult(
        name="UI root",
        url=url,
        ok=True,
        elapsed_ms=elapsed_ms,
        status_code=response.status_code,
        detail=f"content_type={content_type!r}",
    )


def _probe_ui_config(*, url: str, timeout_seconds: float) -> ProbeResult:
    started = time.perf_counter()
    try:
        response = requests.get(url, timeout=timeout_seconds)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        response.raise_for_status()
    except requests.RequestException as exc:
        return ProbeResult(
            name="UI config.js",
            url=url,
            ok=False,
            elapsed_ms=(time.perf_counter() - started) * 1000.0,
            error=str(exc),
        )

    body = response.text or ""
    if "__API_UI_CONFIG__" not in body:
        return ProbeResult(
            name="UI config.js",
            url=url,
            ok=False,
            elapsed_ms=elapsed_ms,
            status_code=response.status_code,
            error="config.js did not contain the canonical UI runtime config key.",
        )

    return ProbeResult(
        name="UI config.js",
        url=url,
        ok=True,
        elapsed_ms=elapsed_ms,
        status_code=response.status_code,
    )


def _print_result(result: ProbeResult, *, verbose: bool) -> None:
    state = "PASS" if result.ok else "FAIL"
    base = f"[{state}] {result.name:<12} {result.url} ({result.elapsed_ms:.1f} ms)"
    if result.status_code is not None:
        base += f" status={result.status_code}"
    print(base)
    if result.detail:
        print(f"       detail: {result.detail}")
    if result.error:
        print(f"       error: {result.error}")
    if verbose:
        print()


def _run_probe_set(
    *,
    api_base_url: str,
    ui_base_url: str,
    timeout_seconds: float,
    skip_ui_config: bool,
) -> list[ProbeResult]:
    api_healthz_url = _join_url(api_base_url, "/healthz")
    api_readyz_url = _join_url(api_base_url, "/readyz")
    ui_root_url = _join_url(ui_base_url, "/")
    ui_config_url = _join_url(ui_base_url, "/config.js")

    results: list[ProbeResult] = [
        _probe_json_status(
            name="API healthz",
            url=api_healthz_url,
            timeout_seconds=timeout_seconds,
            expected_status="ok",
        ),
        _probe_json_status(
            name="API readyz",
            url=api_readyz_url,
            timeout_seconds=timeout_seconds,
            expected_status="ready",
        ),
        _probe_ui_root(url=ui_root_url, timeout_seconds=timeout_seconds),
    ]
    if not skip_ui_config:
        results.append(_probe_ui_config(url=ui_config_url, timeout_seconds=timeout_seconds))
    return results


def _print_environment_header(*, environment: str, api_base_url: str, ui_base_url: str, timeout_seconds: float) -> None:
    print(f"=== {environment.upper()} ===")
    print(f"API base URL: {api_base_url}")
    print(f"UI base URL:  {ui_base_url}")
    print(f"Timeout:      {timeout_seconds}s")
    print()


def _require_cloud_urls(args: argparse.Namespace, cloud_api_base_url: str, cloud_ui_base_url: str) -> None:
    if args.mode not in {"both", "cloud"}:
        return
    if cloud_api_base_url and cloud_ui_base_url:
        return
    raise SystemExit(
        "Cloud mode requires --cloud-base-url or explicit --cloud-api-base-url and --cloud-ui-base-url values."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the control-plane UI and API are healthy.")
    parser.add_argument(
        "--mode",
        choices=("both", "local", "cloud"),
        default="both",
        help="Which endpoint group(s) to test (default: both).",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="HTTP request timeout in seconds (default: 10).",
    )
    parser.add_argument(
        "--skip-ui-config",
        action="store_true",
        help="Skip the /config.js probe.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print expanded probe output.",
    )
    parser.add_argument(
        "--local-api-base-url",
        default=DEFAULT_LOCAL_API_BASE_URL,
        help=f"Local API base URL (default: {DEFAULT_LOCAL_API_BASE_URL}).",
    )
    parser.add_argument(
        "--local-ui-base-url",
        default=DEFAULT_LOCAL_UI_BASE_URL,
        help=f"Local UI base URL (default: {DEFAULT_LOCAL_UI_BASE_URL}).",
    )
    parser.add_argument(
        "--cloud-base-url",
        default=DEFAULT_CLOUD_BASE_URL,
        help="Shared cloud base URL for both UI and API. Defaults to ASSET_ALLOCATION_PUBLIC_BASE_URL when set.",
    )
    parser.add_argument(
        "--cloud-api-base-url",
        default="",
        help="Cloud API base URL. Overrides --cloud-base-url for API probes.",
    )
    parser.add_argument(
        "--cloud-ui-base-url",
        default="",
        help="Cloud UI base URL. Overrides --cloud-base-url for UI probes.",
    )

    args = parser.parse_args()

    local_api_base_url = _normalize_url(args.local_api_base_url)
    local_ui_base_url = _normalize_url(args.local_ui_base_url)
    cloud_base_url = _normalize_url(args.cloud_base_url)
    cloud_api_base_url = _normalize_url(args.cloud_api_base_url or cloud_base_url)
    cloud_ui_base_url = _normalize_url(args.cloud_ui_base_url or cloud_base_url)
    _require_cloud_urls(args, cloud_api_base_url, cloud_ui_base_url)

    print("UI/API health check")
    print(f"Mode:         {args.mode}")
    print(f"Timeout:      {args.timeout_seconds}s")
    print()

    checked_environments = 0
    unhealthy_environments = 0
    global_passed = 0
    global_total = 0

    if args.mode in {"both", "local"}:
        _print_environment_header(
            environment="local",
            api_base_url=local_api_base_url,
            ui_base_url=local_ui_base_url,
            timeout_seconds=args.timeout_seconds,
        )
        local_results = _run_probe_set(
            api_base_url=local_api_base_url,
            ui_base_url=local_ui_base_url,
            timeout_seconds=args.timeout_seconds,
            skip_ui_config=bool(args.skip_ui_config),
        )
        for result in local_results:
            _print_result(result, verbose=bool(args.verbose))
        local_passed = sum(1 for result in local_results if result.ok)
        local_failed = len(local_results) - local_passed
        print(f"Local summary: {local_passed}/{len(local_results)} passed, {local_failed} failed.")
        print()
        checked_environments += 1
        if local_failed > 0:
            unhealthy_environments += 1
        global_passed += local_passed
        global_total += len(local_results)

    if args.mode in {"both", "cloud"}:
        _print_environment_header(
            environment="cloud",
            api_base_url=cloud_api_base_url,
            ui_base_url=cloud_ui_base_url,
            timeout_seconds=args.timeout_seconds,
        )
        cloud_results = _run_probe_set(
            api_base_url=cloud_api_base_url,
            ui_base_url=cloud_ui_base_url,
            timeout_seconds=args.timeout_seconds,
            skip_ui_config=bool(args.skip_ui_config),
        )
        for result in cloud_results:
            _print_result(result, verbose=bool(args.verbose))
        cloud_passed = sum(1 for result in cloud_results if result.ok)
        cloud_failed = len(cloud_results) - cloud_passed
        print(f"Cloud summary: {cloud_passed}/{len(cloud_results)} passed, {cloud_failed} failed.")
        print()
        checked_environments += 1
        if cloud_failed > 0:
            unhealthy_environments += 1
        global_passed += cloud_passed
        global_total += len(cloud_results)

    if checked_environments == 0:
        print("No endpoint groups were checked.")
        return 2

    global_failed = global_total - global_passed
    print(f"Overall summary: {global_passed}/{global_total} passed, {global_failed} failed.")
    print(f"Environments checked: {checked_environments}, unhealthy: {unhealthy_environments}.")
    if unhealthy_environments == 0:
        print("Overall: HEALTHY")
        return 0

    print("Overall: UNHEALTHY")
    return 1


if __name__ == "__main__":
    sys.exit(main())
