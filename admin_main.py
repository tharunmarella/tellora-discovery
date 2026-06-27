"""Uvicorn entrypoint for the discovery admin API."""

from __future__ import annotations

import os

import uvicorn

from infra.lifespan import SERVICE_ADMIN, bootstrap

bootstrap(server_name=SERVICE_ADMIN)

if __name__ == "__main__":
    port = int(os.getenv("PORT", os.getenv("DISCOVERY_ADMIN_PORT", "8080")))
    uvicorn.run(
        "api.admin:app",
        host="0.0.0.0",
        port=port,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )
