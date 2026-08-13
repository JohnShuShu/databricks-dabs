"""Logger factory (mirrors af_aif-loch-ness/src/nessie/common/logging.py).

Usage: ``logger = logging_setup(__name__)`` at module top.
"""

import logging
import sys

log_format = "%(levelname)8s:%(filename)18.18s:%(funcName)22.22s:%(lineno)4d:    %(message)s"
log_level = logging.INFO


def set_log_level(level: str) -> None:
    global log_level
    log_level = getattr(logging, level)


def logging_setup(name: str) -> logging.Logger:
    """Configure the root handler once and return the named child logger.

    Named loggers inherit the root config, so calling this from every module is
    cheap and idempotent.
    """
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(stream=sys.stdout, level=log_level, format=log_format)
    else:
        handler = root_logger.handlers[0]
        handler.setFormatter(logging.Formatter(fmt=log_format))
        handler.setLevel(log_level)
        root_logger.setLevel(log_level)

    return logging.getLogger(name)
