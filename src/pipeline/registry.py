"""pipeline.registry.py"""

import importlib
import pkgutil
from typing import NamedTuple
from loguru import logger

from utils.types import DataFrameSchema, PipelineFn


class StagingPipeline(NamedTuple):
    pipeline_fn: PipelineFn
    data_schema: DataFrameSchema


staging_pipelines: dict[str, StagingPipeline] = {}


def register_staging_pipeline(file_hash: str, schema: DataFrameSchema):
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
