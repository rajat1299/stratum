"""HTTP transport errors."""

from __future__ import annotations


class StratumError(Exception):
    """Base error for SDK failures."""


class StratumHttpError(StratumError):
    """Non-success HTTP status from Stratum."""

    def __init__(self, status_code: int, body: str) -> None:
        self.status_code = status_code
        self.body = body
        super().__init__(f"HTTP {status_code}: {body}")


class UnsupportedFeatureError(StratumError):
    """Feature not available from the current server/API."""

    pass
