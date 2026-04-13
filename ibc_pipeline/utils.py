"""Numeric and label helpers."""

from __future__ import annotations

from typing import Any


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        if cleaned in {"", "NA", "N/A", "null", "None", "-"}:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def prefer_numeric(*values: Any) -> float | None:
    for value in values:
        converted = to_float(value)
        if converted is not None:
            return converted
    return None


def to_plain_number(value: Any) -> float:
    parsed = to_float(value)
    if parsed is None:
        return 0.0
    return parsed


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def norm_label(text: str) -> str:
    return "".join(ch.lower() for ch in text if ch.isalnum())
