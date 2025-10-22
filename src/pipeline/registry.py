"""pipeline.registry.py"""

import importlib
import pkgutil
from pathlib import Path
from typing import Callable, NamedTuple

import polars as pl
from pandera import polars as pa
from loguru import logger


PipelineFn = Callable[[Path | str], pl.DataFrame]


class StagingPipeline(NamedTuple):
    pipeline_fn: PipelineFn
    stage_schema: pa.DataFrameSchema


staging_pipelines: dict[str, StagingPipeline] = {}


def register_staging_pipeline(file_hash: str, schema: pa.DataFrameSchema):
    def decorator(func):
        staging_pipelines[file_hash] = StagingPipeline(func, schema)
        return func

    return decorator


def load_staging_pipelines(staging):
    """
    Dynamically import modules inside data_pipeline.staging so that their
    register_staging_pipeline decorators populate staging_pipelines.
    """
    succ, err = 0, 0
    for _, module_name, _ in pkgutil.iter_modules(staging.__path__):
        try:
            importlib.import_module(f"{staging.__name__}.{module_name}")
            succ += 1
            logger.debug(f"Imported staging pipeline module: {module_name}")
        except Exception as e:
            err += 1
            logger.error(
                f"Failed to import staging pipeline module '{module_name}': {e}"
            )

    return succ, err
