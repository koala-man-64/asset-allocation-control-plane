#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
from typing import Iterable


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _address_matches_port(address: str, port: int) -> bool:
    text = str(address or "").strip()
    return bool(text) and re.search(rf":{port}$", text) is not None


def _collect_windows_listening_pids(port: int) -> set[int]:
    result = _run(["netstat", "-ano", "-p", "TCP"])
    pids: set[int] = set()
    for raw_line in result.stdout.splitlines():
        line = raw_line.strip()
        if not line or line.lower().startswith("proto"):
            continue
        parts = line.split()
        if len(parts) < 5:
            continue
        local_address = parts[1]
        state = parts[-2].upper()
        pid_text = parts[-1]
        if state != "LISTENING" or not _address_matches_port(local_address, port):
            continue
        try:
            pid = int(pid_text)
        except ValueError:
            continue
        if pid > 0 and pid != os.getpid():
            pids.add(pid)
    return pids


def _collect_unix_listening_pids(port: int) -> set[int]:
    pids: set[int] = set()

    if shutil.which("lsof"):
        result = _run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"])
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                pid = int(line)
            except ValueError:
                continue
            if pid > 0 and pid != os.getpid():
                pids.add(pid)
        if pids:
            return pids

    if shutil.which("fuser"):
        result = _run(["fuser", "-n", "tcp", str(port)])
        for token in re.findall(r"\d+", f"{result.stdout} {result.stderr}"):
            pid = int(token)
            if pid > 0 and pid != os.getpid():
                pids.add(pid)
        if pids:
            return pids

    if shutil.which("ss"):
        result = _run(["ss", "-ltnp"])
        for line in result.stdout.splitlines():
            if f":{port}" not in line:
                continue
            for pid_text in re.findall(r"pid=(\d+)", line):
                pid = int(pid_text)
                if pid > 0 and pid != os.getpid():
                    pids.add(pid)
    return pids


def _collect_listening_pids(port: int) -> set[int]:
    if platform.system().lower().startswith("win"):
        return _collect_windows_listening_pids(port)
    return _collect_unix_listening_pids(port)


def _is_process_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _terminate_unix_pid(pid: int) -> bool:
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return False

    deadline = time.time() + 2.0
    while time.time() < deadline:
        if not _is_process_alive(pid):
            return True
        time.sleep(0.1)

    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        return not _is_process_alive(pid)

    deadline = time.time() + 1.0
    while time.time() < deadline:
        if not _is_process_alive(pid):
            return True
        time.sleep(0.1)
    return not _is_process_alive(pid)


def _terminate_pid(pid: int) -> bool:
    if platform.system().lower().startswith("win"):
        result = _run(["taskkill", "/PID", str(pid), "/T", "/F"])
        return result.returncode == 0
    return _terminate_unix_pid(pid)


def _kill_ports(ports: Iterable[int], *, label: str) -> int:
    exit_code = 0
    for port in ports:
        pids = sorted(_collect_listening_pids(port))
        if not pids:
            print(f"[stop-dev] no listener on port {port} ({label})")
            continue

        print(f"[stop-dev] stopping port {port} ({label}) pids={','.join(str(pid) for pid in pids)}")
        for pid in pids:
            if _terminate_pid(pid):
                print(f"[stop-dev] stopped pid={pid} port={port}")
            else:
                print(f"[stop-dev] failed to stop pid={pid} port={port}", file=sys.stderr)
                exit_code = 1
    return exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="Stop local dev processes listening on one or more TCP ports.")
    parser.add_argument("--port", dest="ports", type=int, action="append", required=True, help="TCP port to stop.")
    parser.add_argument("--label", default="dev", help="Short label for log output.")
    args = parser.parse_args()

    normalized_ports: list[int] = []
    for port in args.ports:
        if port <= 0 or port > 65535:
            parser.error(f"invalid port: {port}")
        normalized_ports.append(port)

    return _kill_ports(normalized_ports, label=str(args.label or "dev"))


if __name__ == "__main__":
    raise SystemExit(main())
