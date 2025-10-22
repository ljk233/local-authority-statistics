from enum import StrEnum


class Outcome(StrEnum):
    SUCCESS = "Success"
    SKIPPED = "Skipped"
    FAILED = "Failed"
