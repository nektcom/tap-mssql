"""Opt-in fallback host: pick the database host once, at startup.

Only used when ``fallback_host`` is configured. The primary host is probed first
and the fallback only if the primary cannot be reached; whichever succeeds is
pinned for the whole run (the connector's engine is built from that single URL),
so there is never any switching between hosts mid-extraction.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy.pool import NullPool

if TYPE_CHECKING:
    from collections.abc import Mapping

    from sqlalchemy.engine import URL

DEFAULT_PROBE_TIMEOUT_SECONDS = 15

# DB-Lib / SQL Server error codes, grouped by the stage of the connection they come from.
_NETWORK_ERROR_CODES = frozenset({20002, 20003, 20004, 20006, 20009})
_LOGIN_ERROR_CODES = frozenset({18452, 18456, 18486, 18487, 18488})
_DATABASE_ERROR_CODES = frozenset({4060})

STAGE_SSH_TUNNEL = "ssh_tunnel"
STAGE_NETWORK = "network"
STAGE_LOGIN = "login"
STAGE_DATABASE = "database"
STAGE_UNKNOWN = "connection"

_STAGE_DESCRIPTIONS = {
    STAGE_SSH_TUNNEL: "the SSH tunnel could not be opened",
    STAGE_NETWORK: "the server could not be reached (network, DNS or firewall)",
    STAGE_LOGIN: "the server rejected the user or password",
    STAGE_DATABASE: "the database could not be opened",
    STAGE_UNKNOWN: "the connection failed",
}


@dataclass(frozen=True)
class HostCandidate:
    role: str  # "primary" or "fallback"
    host: str
    port: int

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"


@dataclass(frozen=True)
class ConnectionAttempt:
    candidate: HostCandidate
    stage: str
    code: int | None
    detail: str

    @property
    def stage_description(self) -> str:
        return _STAGE_DESCRIPTIONS[self.stage]


def host_candidates(config: Mapping[str, Any]) -> list[HostCandidate]:
    """Primary host first, then the fallback; the fallback port defaults to the primary one."""
    port = int(config.get("port") or 1433)
    candidates = [HostCandidate("primary", config["host"], port)]
    if config.get("fallback_host"):
        fallback_port = int(config.get("fallback_port") or port)
        candidates.append(HostCandidate("fallback", config["fallback_host"], fallback_port))
    return candidates


def probe(url: URL, timeout_seconds: int) -> None:
    """Open one connection with a short login timeout and run ``SELECT 1``.

    ``login_timeout`` goes through ``connect_args`` because the pymssql dialect
    forwards URL query values as strings, and pymssql needs an int here.
    """
    engine = sa.create_engine(url, poolclass=NullPool, connect_args={"login_timeout": timeout_seconds})
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
    finally:
        engine.dispose()


def classify_error(exc: BaseException) -> tuple[str, int | None, str]:
    """Return (stage, error code, driver message) for a failed probe."""
    orig = getattr(exc, "orig", None) or exc
    args = getattr(orig, "args", ())
    # pymssql wraps (code, message) in a tuple: OperationalError((20009, b"..."),)
    if len(args) == 1 and isinstance(args[0], tuple):
        args = args[0]

    code = args[0] if args and isinstance(args[0], int) else None
    raw = args[1] if len(args) > 1 else str(orig)
    message = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
    message = " ".join(message.split())

    lowered = message.lower()
    if code in _LOGIN_ERROR_CODES or "login failed" in lowered:
        stage = STAGE_LOGIN
    elif code in _DATABASE_ERROR_CODES or "cannot open database" in lowered:
        stage = STAGE_DATABASE
    elif code in _NETWORK_ERROR_CODES or "unable to connect" in lowered:
        stage = STAGE_NETWORK
    else:
        stage = STAGE_UNKNOWN
    return stage, code, message
