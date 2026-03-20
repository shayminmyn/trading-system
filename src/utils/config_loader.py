"""
Loads and validates config.yaml. Supports env-variable overrides via .env.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class ConfigLoader:
    """Singleton-style config loader. Call `ConfigLoader.load()` once at startup."""

    _instance: "ConfigLoader | None" = None
    _config: dict[str, Any] = {}

    def __init__(self, config_path: str = "config.yaml") -> None:
        self._path = Path(config_path)
        self._config = self._load()

    @classmethod
    def load(cls, config_path: str = "config.yaml") -> "ConfigLoader":
        if cls._instance is None:
            cls._instance = cls(config_path)
        return cls._instance

    def _load(self) -> dict[str, Any]:
        if not self._path.exists():
            raise FileNotFoundError(
                f"Config file not found: {self._path}. "
                "Copy config.example.yaml to config.yaml and fill in your credentials."
            )
        with open(self._path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

        cfg = self._apply_env_overrides(cfg)
        self._validate(cfg)
        return cfg

    def _apply_env_overrides(self, cfg: dict) -> dict:
        """Allow overriding sensitive values via environment variables."""
        overrides = {
            "TELEGRAM_BOT_TOKEN": ("telegram", "bot_token"),
            "TELEGRAM_CHAT_ID": ("telegram", "chat_id"),
            "MT5_LOGIN": ("mt5", "login"),
            "MT5_PASSWORD": ("mt5", "password"),
            "MT5_SERVER": ("mt5", "server"),
        }
        for env_key, (section, field) in overrides.items():
            value = os.getenv(env_key)
            if value:
                cfg.setdefault(section, {})[field] = value
        return cfg

    def _validate(self, cfg: dict) -> None:
        required_sections = ["trading_pairs", "risk_management"]
        for section in required_sections:
            if section not in cfg:
                raise ValueError(f"Missing required config section: '{section}'")

        rm = cfg["risk_management"]
        assert 0 < rm.get("risk_per_trade_percent", 0) <= 10, (
            "risk_per_trade_percent must be between 0 and 10"
        )

    def get(self, key: str, default: Any = None) -> Any:
        """Single-key access with default, same signature as dict.get()."""
        return self._config.get(key, default)

    def getpath(self, *keys: str, default: Any = None) -> Any:
        """Nested key access: cfg.getpath('risk_management', 'account_balance')"""
        node = self._config
        for k in keys:
            if not isinstance(node, dict):
                return default
            node = node.get(k, default)
            if node is default:
                return default
        return node

    def __getitem__(self, key: str) -> Any:
        return self._config[key]

    def __contains__(self, key: str) -> bool:
        return key in self._config

    @property
    def raw(self) -> dict[str, Any]:
        return self._config
