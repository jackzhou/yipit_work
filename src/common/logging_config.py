# src/common/logging_config.py

from __future__ import annotations

import logging

_DEFAULT_FORMAT = "%(levelname)s [%(filename)s:%(lineno)d] %(message)s"
_configured = False


def configure_logging(
    level: int = logging.INFO,
    fmt: str = _DEFAULT_FORMAT,
) -> None:
    """Initialize the root logger. Later calls are no-ops."""
    global _configured
    if _configured:
        return
    logging.basicConfig(level=level, format=fmt)
    _configured = True


# Importing this module applies the default configuration once.
configure_logging()
