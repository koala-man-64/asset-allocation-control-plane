"""Diagnose Schwab authorize-endpoint `invalid_client` failures.

The script mirrors the env-file loading order used by broker_balances_smoke.py,
prints the effective Schwab OAuth settings without exposing the client secret,
generates the current repo authorization URL beside the PDF-minimal URL, and
can optionally probe Schwab without following redirects so operators can keep
correlation IDs from failed responses.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import httpx
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from schwab import SchwabClient, SchwabConfig

DEFAULT_OPERATOR_PROJECT = Path.home() / "Projects" / "asset-allocation-control-plane"
DEFAULT_ENV_PATHS = (
    ROOT / ".env",
    ROOT / ".env.web",
    DEFAULT_OPERATOR_PROJECT / ".env",
    DEFAULT_OPERATOR_PROJECT / ".env.web",
)

SCHWAB_CLIENT_CORRELATION_HEADER = "schwab-client-correlid"
SCHWAB_REQUEST_ID_HEADER = "x-request-id"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,application/json;q=0.8,*/*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass(frozen=True)
class EnvSnapshot:
    loaded_paths: tuple[Path, ...]
    values: dict[str, str]
    callback_sources: tuple[tuple[Path, str], ...]


@dataclass(frozen=True)
class AuthorizationVariant:
    label: str
    request_shape: str
    callback_url: str
    url: str
    diagnostic_purpose: str


@dataclass(frozen=True)
class CallbackInspection:
    has_code: bool
    code_length: int
    code_suffix: str
    returned_state: str
    state_matches: bool | None


@dataclass(frozen=True)
class ProbeResult:
    label: str
    status_code: int | None
    location: str
    content_type: str
    request_body: str
    response_body: str
    error: str
    error_description: str
    schwab_client_correlid: str
    request_id: str
    classification: str
    network_error: str = ""


def _strip_or_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _existing_unique_paths(paths: Iterable[Path]) -> list[Path]:
    seen: set[Path] = set()
    result: list[Path] = []
    for path in paths:
        resolved = path.expanduser().resolve()
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        result.append(resolved)
    return result


def _unique_nonblank(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _mask_suffix(value: object, *, suffix_length: int = 4) -> str:
    text = str(value or "").strip()
    if not text:
        return "<missing>"
    suffix = text[-suffix_length:] if len(text) >= suffix_length else text
    return f"present len={len(text)} suffix={suffix}"


def _presence_only(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return "<missing>"
    return f"present len={len(text)}"


def _truncate(value: object, *, limit: int = 240) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return f"{text[: limit - 3]}..."


def _body_text(value: object, *, empty: str = "<empty>") -> str:
    if value is None:
        return empty
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
    else:
        text = str(value)
    return text if text else empty


def load_effective_env(paths: Sequence[Path], *, base_env: Mapping[str, str] | None = None) -> EnvSnapshot:
    """Load env files in the same order as broker_balances_smoke.py.

    File values intentionally override earlier values, including process env,
    because broker_balances_smoke.py uses that operator-facing behavior today.
    """

    merged = dict(base_env if base_env is not None else os.environ)
    loaded_paths: list[Path] = []
    callback_sources: list[tuple[Path, str]] = []

    for path in _existing_unique_paths(paths):
        loaded_paths.append(path)
        values = dotenv_values(path)
        callback = _strip_or_none(values.get("SCHWAB_APP_CALLBACK_URL"))
        if callback:
            callback_sources.append((path, callback))
        for key, value in values.items():
            merged[str(key)] = "" if value is None else str(value)

    return EnvSnapshot(
        loaded_paths=tuple(loaded_paths),
        values=merged,
        callback_sources=tuple(callback_sources),
    )


def build_repo_authorization_url(client_id: str, callback_url: str, *, state: str | None) -> str:
    config = SchwabConfig(
        client_id=client_id,
        client_secret="",
        app_callback_url=callback_url,
    )
    with SchwabClient(config) as client:
        return client.build_authorization_url(state=state)


def build_pdf_minimal_authorization_url(client_id: str, callback_url: str) -> str:
    config = SchwabConfig(
        client_id=client_id,
        client_secret="",
        app_callback_url=callback_url,
    )
    params = {
        "client_id": client_id,
        "redirect_uri": callback_url,
    }
    return f"{config.get_authorization_url()}?{urlencode(params)}"


def build_authorization_variants(
    *,
    client_id: str,
    effective_callback_url: str,
    callback_candidates: Sequence[str],
    state: str | None,
) -> list[AuthorizationVariant]:
    resolved_client_id = _strip_or_none(client_id)
    resolved_callback = _strip_or_none(effective_callback_url)
    if not resolved_client_id:
        raise ValueError("SCHWAB_CLIENT_ID is required to build authorization diagnostics.")
    if not resolved_callback:
        raise ValueError("SCHWAB_APP_CALLBACK_URL is required to build authorization diagnostics.")

    callbacks = _unique_nonblank([resolved_callback, *callback_candidates])
    variants: list[AuthorizationVariant] = []

    for index, callback in enumerate(callbacks):
        suffix = "effective" if index == 0 else f"alternate-{index}"
        variants.append(
            AuthorizationVariant(
                label=f"current-{suffix}",
                request_shape="repo-current",
                callback_url=callback,
                url=build_repo_authorization_url(resolved_client_id, callback, state=state),
                diagnostic_purpose=(
                    "Matches SchwabClient.build_authorization_url and broker_balances_smoke.py."
                    if index == 0
                    else "Tests whether a different registered callback fixes the current repo request."
                ),
            )
        )
        variants.append(
            AuthorizationVariant(
                label=f"minimal-{suffix}",
                request_shape="pdf-minimal",
                callback_url=callback,
                url=build_pdf_minimal_authorization_url(resolved_client_id, callback),
                diagnostic_purpose=(
                    "Uses only the PDF-documented client_id and redirect_uri parameters."
                    if index == 0
                    else "Separates callback registration from extra authorize parameters."
                ),
            )
        )

    return variants


def inspect_callback_url(callback_url: str, *, expected_state: str | None) -> CallbackInspection:
    parsed = urlparse(str(callback_url or "").strip())
    query = parse_qs(parsed.query)
    codes = query.get("code") or []
    states = query.get("state") or []
    code = codes[0] if codes else ""
    returned_state = states[0] if states else ""
    expected = _strip_or_none(expected_state)
    state_matches = None if not expected else returned_state == expected
    return CallbackInspection(
        has_code=bool(code),
        code_length=len(code),
        code_suffix=code[-4:] if len(code) >= 4 else code,
        returned_state=returned_state,
        state_matches=state_matches,
    )


def _payload_from_response(response: httpx.Response) -> Mapping[str, Any]:
    try:
        payload = response.json()
    except Exception:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _classify_probe(
    *,
    status_code: int | None,
    location: str,
    content_type: str,
    error: str,
    network_error: str,
) -> str:
    if network_error:
        return "network failure before Schwab returned an OAuth decision"
    if status_code is None:
        return "no HTTP status captured"
    if 300 <= status_code <= 399 and location:
        return "authorize accepted; Schwab redirected toward login, consent, or callback"
    if status_code == 200 and "html" in content_type.lower():
        return "authorize accepted or served an interactive login/consent page"
    if status_code in {401, 403} and error == "invalid_client":
        return "client ID, app approval, product subscription, or exact redirect_uri rejected before login"
    if status_code == 400 and error in {"invalid_request", "unsupported_response_type"}:
        return "authorize request shape rejected; compare repo-current with pdf-minimal"
    if status_code in {401, 403}:
        return "authorization server rejected the app before login"
    return "unexpected authorize response; preserve correlation IDs and compare variants"


def probe_authorization_variant(
    variant: AuthorizationVariant,
    *,
    timeout_seconds: float = 20.0,
    http_client: httpx.Client | None = None,
) -> ProbeResult:
    owns_client = http_client is None
    client = http_client or httpx.Client(
        timeout=httpx.Timeout(timeout_seconds),
        follow_redirects=False,
        headers=BROWSER_HEADERS,
    )
    try:
        response = client.get(variant.url)
    except httpx.HTTPError as exc:
        network_error = f"{type(exc).__name__}: {exc}"
        return ProbeResult(
            label=variant.label,
            status_code=None,
            location="",
            content_type="",
            request_body="<empty>",
            response_body="<empty>",
            error="",
            error_description="",
            schwab_client_correlid="",
            request_id="",
            classification=_classify_probe(
                status_code=None,
                location="",
                content_type="",
                error="",
                network_error=network_error,
            ),
            network_error=_truncate(network_error),
        )
    finally:
        if owns_client:
            client.close()

    payload = _payload_from_response(response)
    error = str(payload.get("error") or "").strip()
    error_description = str(payload.get("error_description") or payload.get("message") or "").strip()
    content_type = response.headers.get("content-type", "")
    location = response.headers.get("location", "")
    return ProbeResult(
        label=variant.label,
        status_code=response.status_code,
        location=location,
        content_type=content_type,
        request_body=_body_text(response.request.content if response.request is not None else b""),
        response_body=_body_text(response.text),
        error=error,
        error_description=_truncate(error_description),
        schwab_client_correlid=response.headers.get(SCHWAB_CLIENT_CORRELATION_HEADER, ""),
        request_id=response.headers.get(SCHWAB_REQUEST_ID_HEADER, ""),
        classification=_classify_probe(
            status_code=response.status_code,
            location=location,
            content_type=content_type,
            error=error,
            network_error="",
        ),
    )


def summarize_probe_results(results: Sequence[ProbeResult]) -> str:
    accepted = [result.label for result in results if "accepted" in result.classification]
    invalid_client = [
        result.label
        for result in results
        if result.status_code in {401, 403} and result.error == "invalid_client"
    ]
    if accepted:
        return f"At least one variant reached Schwab login/redirect path: {', '.join(accepted)}."
    if invalid_client and len(invalid_client) == len(results):
        return "Every probed variant returned invalid_client; prioritize Schwab app/client/product approval checks."
    if invalid_client:
        return "Only some variants returned invalid_client; compare accepted variants by callback URL and request shape."
    return "No decisive OAuth classification; preserve request IDs and inspect Schwab response details."


def _selected_env_paths(args: argparse.Namespace) -> tuple[Path, ...]:
    if args.env_file is None:
        return DEFAULT_ENV_PATHS
    return tuple(Path(path) for path in args.env_file)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate and optionally probe Schwab OAuth authorize URLs for invalid_client RCA.",
    )
    parser.add_argument(
        "--env-file",
        action="append",
        default=None,
        help="Env file to load. Repeat to load multiple files in order. Defaults match broker_balances_smoke.py.",
    )
    parser.add_argument(
        "--alternate-callback",
        action="append",
        default=[],
        help="Extra callback URL to test against the same Schwab client ID. Repeat for multiple callbacks.",
    )
    parser.add_argument(
        "--state",
        default="schwab-oauth-diagnostic",
        help="State value used by the repo-current authorization URL variants.",
    )
    parser.add_argument(
        "--probe",
        action="store_true",
        help="Send GET requests without following redirects and print status, error, and Schwab correlation IDs.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="HTTP timeout for --probe requests.",
    )
    parser.add_argument(
        "--callback-url",
        default=None,
        help="Optional redirected Schwab callback URL to inspect for code/state without exchanging tokens.",
    )
    return parser


def _print_snapshot(snapshot: EnvSnapshot) -> None:
    print("Loaded env files:")
    for path in snapshot.loaded_paths:
        print(f"  {path}")
    print()
    print("Effective Schwab config:")
    print(f"  SCHWAB_CLIENT_ID: {_mask_suffix(snapshot.values.get('SCHWAB_CLIENT_ID'))}")
    print(f"  SCHWAB_CLIENT_SECRET: {_presence_only(snapshot.values.get('SCHWAB_CLIENT_SECRET'))}")
    print(f"  SCHWAB_APP_CALLBACK_URL: {snapshot.values.get('SCHWAB_APP_CALLBACK_URL') or '<missing>'}")
    print()
    if snapshot.callback_sources:
        print("Callback URLs discovered in loaded env files:")
        for path, callback in snapshot.callback_sources:
            print(f"  {path}: {callback}")
        print()


def _print_variants(variants: Sequence[AuthorizationVariant]) -> None:
    print("Authorization URL variants:")
    for variant in variants:
        print(f"[{variant.label}] shape={variant.request_shape}")
        print(f"  callback_url: {variant.callback_url}")
        print(f"  purpose: {variant.diagnostic_purpose}")
        print(f"  url: {variant.url}")
    print()


def _print_callback_inspection(inspection: CallbackInspection) -> None:
    print("Callback URL inspection:")
    print(f"  code: {'present' if inspection.has_code else 'missing'}")
    if inspection.has_code:
        print(f"  code_length: {inspection.code_length}")
        print(f"  code_suffix: {inspection.code_suffix}")
    print(f"  returned_state: {inspection.returned_state or '<missing>'}")
    if inspection.state_matches is not None:
        print(f"  state_matches_expected: {inspection.state_matches}")
    print()


def _print_probe_results(results: Sequence[ProbeResult]) -> None:
    print("Probe results:")
    for result in results:
        print(f"[{result.label}]")
        if result.network_error:
            print(f"  network_error: {result.network_error}")
        else:
            print(f"  status: {result.status_code}")
            print(f"  content_type: {result.content_type or '<missing>'}")
            print(f"  location: {result.location or '<missing>'}")
            print(f"  request_body: {result.request_body}")
            print(f"  response_body: {result.response_body}")
            print(f"  error: {result.error or '<missing>'}")
            print(f"  error_description: {result.error_description or '<missing>'}")
            print(f"  {SCHWAB_CLIENT_CORRELATION_HEADER}: {result.schwab_client_correlid or '<missing>'}")
            print(f"  {SCHWAB_REQUEST_ID_HEADER}: {result.request_id or '<missing>'}")
        print(f"  classification: {result.classification}")
    print()
    print(f"Summary: {summarize_probe_results(results)}")
    print()
    print("Raw probe JSON:")
    print(json.dumps([result.__dict__ for result in results], indent=2, sort_keys=True))


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    snapshot = load_effective_env(_selected_env_paths(args))
    if not snapshot.loaded_paths:
        print("No env files were found. Pass --env-file explicitly.", file=sys.stderr)
        return 1

    _print_snapshot(snapshot)

    client_id = _strip_or_none(snapshot.values.get("SCHWAB_CLIENT_ID"))
    callback_url = _strip_or_none(snapshot.values.get("SCHWAB_APP_CALLBACK_URL"))
    if not client_id or not callback_url:
        print("SCHWAB_CLIENT_ID and SCHWAB_APP_CALLBACK_URL are required for authorize diagnostics.", file=sys.stderr)
        return 1

    callback_candidates = [
        callback for _path, callback in snapshot.callback_sources if callback != callback_url
    ]
    callback_candidates.extend(args.alternate_callback or [])
    variants = build_authorization_variants(
        client_id=client_id,
        effective_callback_url=callback_url,
        callback_candidates=callback_candidates,
        state=args.state,
    )
    _print_variants(variants)

    if args.callback_url:
        _print_callback_inspection(inspect_callback_url(args.callback_url, expected_state=args.state))

    if not args.probe:
        print("No live Schwab requests sent. Re-run with --probe to capture HTTP status and correlation IDs.")
        return 0

    results = [
        probe_authorization_variant(variant, timeout_seconds=args.timeout_seconds)
        for variant in variants
    ]
    _print_probe_results(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
