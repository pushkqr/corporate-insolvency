"""Shared data models."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SeedCompany:
    ticker: str
    hint_name: str
