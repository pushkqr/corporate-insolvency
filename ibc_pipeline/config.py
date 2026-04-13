"""Environment and runtime configuration."""

from __future__ import annotations

import logging
import os

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


def parse_debug_flag(raw: str | None) -> bool:
    if raw is None:
        return False
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def configure_logging() -> logging.Logger:
    debug_enabled = parse_debug_flag(os.getenv("DEBUG"))
    level = logging.DEBUG if debug_enabled else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s | %(levelname)s | %(message)s")
    logger = logging.getLogger("dataset_builder")
    logger.info("Logging initialized. mode=%s", "dev" if debug_enabled else "prod")
    return logger


def load_environment() -> None:
    if load_dotenv is not None:
        load_dotenv()


def get_required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def resolve_llm_api_key(base_url: str | None) -> str:
    if base_url and "generativelanguage.googleapis.com" in base_url:
        return get_required_env("GOOGLE_API_KEY")
    return get_required_env("OPENAI_API_KEY")


def resolve_enrich_model(base_url: str | None, requested_model: str) -> str:
    model = requested_model.strip()
    if base_url and "generativelanguage.googleapis.com" in base_url:
        if not model or model.startswith("gpt-"):
            return os.getenv("MODEL_NAME", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
    return model or "gpt-4o-mini"
