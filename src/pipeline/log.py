# from typing import Any, Self

import sys
from collections import Counter
from functools import partial

from loguru import logger

from . import runner
from .outcome import Outcome

# Remove the default logger configuration
logger.remove(0)
# Configure the logger to output logs to stderr with a custom format
logger.add(sys.stderr, format="{time} | {level} | {message}")
# Add cumtom logging levels
logger.level("SKIPPED", no=900, color="<yellow>")
logger.level("FAILED", no=3901, color="<red>")
# Register the levels
log_levels = {
    Outcome.SUCCESS: logger.success,
    Outcome.SKIPPED: partial(logger.log, "SKIPPED"),
    Outcome.FAILED: partial(logger.log, "FAILED"),
}


# Custom formatting


def log_starting_staging():
    logger.info("Initializing staging.")


def log_no_staging_pipelines_registered():
    logger.warning("No staging pipelines registered — exiting early.")


def log_outcome(outcome: Outcome, message: str) -> None:
    log_levels[outcome](message)


def log_staging_completed(outcomes: list[Outcome]) -> None:
    counter = Counter(outcome.value for outcome in outcomes)
    fmt_count = f"{counter[Outcome.SUCCESS.value]} succeeded, {counter[Outcome.FAILED.value]} failed, {counter[Outcome.SKIPPED.value]} skipped"
    logger.info(f"Pipeline finished: {fmt_count}")
    pass


def format_count(counter: Counter):
    return (
        f"{counter[Outcome.SUCCESS.value]} succeeded,"
        f"{counter[Outcome.FAILED.value]} failed,"
        f"{counter[Outcome.SKIPPED.value]} skipped"
    )
