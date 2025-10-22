from enum import Enum, auto


class StageStatus(Enum):
    DATA_DIRS_NOT_FOUND = auto()
    FILE_PATH_IS_DIR = auto()
    FILE_PATH_NOT_FOUND = auto()
    FAILED_TO_HASH_FILE = auto()
    FILE_ALREADY_STAGED = auto()
    STAGING_PIPELINE_NOT_IMPLEMENTED = auto()
    STAGING_PIPELINE_FAILED = auto()
    STAGED_DATA_FAILED_SCHEMA = auto()
    FAILED_TO_WRITE_STAGED_DATA = auto()
    SUCCESS = auto()
