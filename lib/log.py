"""
Module for configuring and managing application-wide logger.
"""

import logging
import traceback
import warnings

import lib.environment as env


def format_traceback():
    """
    Fetches traceback from sys and formats it.
    Formatting such that it can be packaged as a single-line string literal
        with escapes on double-quotes (as to be a valid str in a json k-v)
    """
    tb_fmt_str = traceback.format_exc().replace("\n", "\\n").replace('"', '\\"')
    return tb_fmt_str


log_fmt = '{"timestamp":"%(asctime)s", "severity": "%(levelname)s", "textPayload": "%(message)s", "labels":{"pid":"%(process)d", "thread":"%(thread)d"}}'
log_level_map = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
}

try:
    log_level = log_level_map[env.LOG_LEVEL]
except KeyError:
    raise Exception(
        f"Log level must be specified as an env var, one of: {log_level_map.keys()}. "
        f"Got: {env.LOG_LEVEL}"
    )


logging.basicConfig(encoding="utf-8", level=log_level, format=log_fmt)

logger = logging.getLogger("api-preprocessor")


# capture and redirect warnings from the `warn` pkg to our logger
def log_warn(message, category, filename, lineno, file=None, line=None):
    logger.warning(message)


warnings.showwarning = log_warn
