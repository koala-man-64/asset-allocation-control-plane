#!/usr/bin/env bash
set -euo pipefail

# Refresh the compose stack so service name changes take effect.
docker compose down --remove-orphans
docker compose up --build -d
docker compose ps
