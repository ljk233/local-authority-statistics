from pathlib import Path

import polars as pl
from loguru import logger

from . import schema
from pipeline.registry import register_staging_pipeline


hash = "d91d0f3f36a6fedbaaf44d7a482f60a91a25ff698701d6ac44728359a9378428"
schema = schema.LocalAuthorityHierarchy


@register_staging_pipeline(hash, schema)
def stage(source: Path | str) -> pl.DataFrame:
    return pl.read_csv(
        source,
        new_columns=[
            "local_authority_code",
            "local_authority_name",
            "region_code",
            "region_name",
            "country_code",
            "country_name",
        ],
    )
