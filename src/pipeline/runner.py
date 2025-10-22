"""pipeline.runner.py"""

from pathlib import Path

import polars as pl
from pandera import errors as pae

from . import registry
from . import log
from .outcome import Outcome
from utils import file_handler as fh


class StagePipelineError(Exception):
    """Staging pipeline failed."""


def stage_files(raw_dir: Path | str, staged_dir: Path | str) -> list[Outcome]:
    log.log_starting_staging()

    if not registry.staging_pipelines:
        log.log_no_staging_pipelines_registered()
        return []

    outcomes = []
    for file_path in sorted(Path(raw_dir).iterdir()):
        outcome, message = stage(file_path, Path(staged_dir))
        log.log_outcome(outcome, message)
        outcomes.append(outcome)

    log.log_staging_completed(outcomes)

    return outcomes


def stage(file_path: Path, stage_dir_path: Path) -> tuple[Outcome, str]:
    file_hash = "NO FILE HASH"
    try:
        file_hash = fh.hash_file(file_path)
        staged_file_path = get_stage_file_path(file_hash, stage_dir_path)
        staging_pipeline = registry.staging_pipelines[file_hash]
        staged = run_staging_pipeline_func(file_path, staging_pipeline.pipeline_fn)
        validated = staging_pipeline.data_schema.validate(staged)
        validated.write_parquet(staged_file_path)
        return Outcome.SUCCESS, f"'{file_path.name}' -> '{staged_file_path.name}'"
    except IsADirectoryError as err:
        return (Outcome.SKIPPED, f"File '{file_path.name}' is a directory")
    except FileNotFoundError as err:
        return (Outcome.FAILED, f"File '{file_path.name}' does not exist")
    except PermissionError as err:
        return (Outcome.FAILED, f"Denied access to file '{file_path.name}': {str(err)}")
    except FileExistsError as err:
        return Outcome.SKIPPED, f"File '{file_path.name}' already staged"
    except KeyError as err:
        msg = f"File '{file_path.name}' has no registered staging pipeline."
        return (Outcome.SKIPPED, msg)
    except StagePipelineError as err:
        return Outcome.FAILED, f"Staging pipeline function failed: {str(err)}"
    except pae.SchemaError as err:
        return Outcome.FAILED, f"Staged data failed schema check: {str(err)}"
    except Exception as err:
        return Outcome.FAILED, f"Unexpected {type(err).__name__}: {err}"


def get_stage_file_path(file_hash: str, stage_dir_path: Path) -> Path:
    staged_file_path = stage_dir_path / f"{file_hash}.parquet"
    if staged_file_path.exists():
        raise FileExistsError(staged_file_path)

    return staged_file_path


def run_staging_pipeline_func(
    file_path: Path, pipeline_func: registry.PipelineFn
) -> pl.DataFrame:
    try:
        return pipeline_func(file_path)
    except Exception as err:
        raise StagePipelineError(str(err))
