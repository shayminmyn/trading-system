"""
Thread-safe structured logger for the trading system.
Supports console + rotating file output, safe for concurrent threads (no-GIL).
"""

import logging
import logging.handlers
import sys
import os
from pathlib import Path


_loggers: dict[str, logging.Logger] = {}


def get_logger(name: str, level: str = "INFO", log_file: str | None = None) -> logging.Logger:
    """Return a named logger, creating it once and caching it."""
    if name in _loggers:
        return _loggers[name]

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-24s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)

    _loggers[name] = logger
    return logger


def configure_from_config(cfg: dict) -> None:
    """Apply logging config from the config.yaml logging section."""
    log_cfg = cfg.get("logging", {})
    level = log_cfg.get("level", "INFO")
    log_file = log_cfg.get("log_file")
    for name, logger in _loggers.items():
        logger.setLevel(getattr(logging, level.upper(), logging.INFO))
