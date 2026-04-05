#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import socket
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv


def _repo_root() -> Path:
    current = Path(__file__).resolve().parent
    for candidate in (current, *current.parents):
        if (candidate / "pyproject.toml").exists() or (candidate / ".codex").exists():
            return candidate
    raise RuntimeError(f"Unable to locate repo root from {__file__}")


def _parse_port(raw: str | None, default: int = 9000) -> int:
    text = (raw or "").strip()
    if not text:
        return default
    try:
        value = int(text)
    except ValueError as exc:
        raise ValueError(f"API_PORT must be an integer (received: {raw!r})") from exc
    if value <= 0 or value > 65535:
        raise ValueError(f"API_PORT must be between 1 and 65535 (received: {value})")
    return value


def _can_bind_local_port(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
            return True
        except OSError:
            return False


def _proxy_target_port() -> int | None:
    raw = (os.environ.get("VITE_API_PROXY_TARGET") or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if parsed.scheme and parsed.netloc and parsed.port is not None:
        return parsed.port
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Run local API dev server with env-aware port checks.")
    parser.add_argument("--host", default="0.0.0.0", help="Host interface for uvicorn (default: 0.0.0.0).")
    parser.add_argument(
        "--no-reload",
        action="store_true",
        help="Disable uvicorn reload mode (default: reload enabled).",
    )
    args = parser.parse_args()

    repo_root = _repo_root()
    env_path = repo_root / ".env"
    load_dotenv(env_path, override=False)

    try:
        api_port = _parse_port(os.environ.get("API_PORT"), default=9000)
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    if not _can_bind_local_port(api_port):
        proxy_target = (os.environ.get("VITE_API_PROXY_TARGET") or "").strip() or "<unset>"
        print(
            f"ERROR: Port {api_port} is already in use. "
            "The API server could not start, so UI calls will not reach this project.",
            file=sys.stderr,
        )
        print(f"       API_PORT={api_port}", file=sys.stderr)
        print(f"       VITE_API_PROXY_TARGET={proxy_target}", file=sys.stderr)
        print(
            "       Stop the conflicting process, or change API_PORT and "
            "VITE_API_PROXY_TARGET to a matching free port.",
            file=sys.stderr,
        )
        return 1

    proxy_port = _proxy_target_port()
    if proxy_port is not None and proxy_port != api_port:
        print(
            "WARNING: VITE_API_PROXY_TARGET port does not match API_PORT. "
            f"UI proxy port={proxy_port}, API port={api_port}.",
            file=sys.stderr,
        )

    command = [
        sys.executable,
        "-m",
        "uvicorn",
        "api.service.app:app",
        "--host",
        args.host,
        "--port",
        str(api_port),
        "--env-file",
        str(env_path),
    ]
    if not args.no_reload:
        command.append("--reload")

    print(f"Starting API dev server on http://{args.host}:{api_port}")
    return subprocess.call(command, cwd=str(repo_root))


if __name__ == "__main__":
    raise SystemExit(main())
