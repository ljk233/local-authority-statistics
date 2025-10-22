from pathlib import Path

import polars as pl
from polars import selectors as cs

from . import schema
from pipeline.registry import register_staging_pipeline


hash = "792e38dd5031964f64f7d763a5300bed824d47b600c07cf8b67ad7d86a5fbba9"
schema = schema.Fact
metric_group = "House affordability ratio"
sheet_metric = {
    "5c": {
        "metric": "Median-to-Median",
        "code": "house_affordability_ratio_med",
        "unit": "Ratio of median house price to median gross annual residence-based earnings.",
    },
    "6c": {
        "metric": "Lower-to-lower",
        "code": "house_affordability_ratio_q25",
        "unit": "Ratio of lower quartile house price to lower quartile gross annual residence-based earnings.",
    },
}


@register_staging_pipeline(hash, schema)
def stage(source: Path | str) -> pl.DataFrame:
    source_path = Path(source)
    staged = []
    for sheet_name, metric_info in sheet_metric.items():
        data = load(source, sheet_name)
        cleansed = clean(data)
        transformed = transform(cleansed)
        annotated = annotate(
            transformed,
            metric_group=metric_group,
            **metric_info,
            source=source_path.name,
        )
        staged.append(annotated)

    return pl.concat(staged)


def load(source: Path | str, sheet_name: str) -> pl.DataFrame:
    return pl.read_excel(source, sheet_name=sheet_name, read_options={"header_row": 1})


def clean(data: pl.DataFrame) -> pl.DataFrame:
    return data.filter(
        pl.col("Local authority code").str.extract(r"^(\w)").is_in(["E", "W"])
    ).select(
        pl.col("Local authority code").alias("local_authority_code"),
        cs.matches(r"^(\d{4})$").cast(pl.Float64, strict=False),
    )


def transform(data: pl.DataFrame) -> pl.DataFrame:
    return data.unpivot(
        on=cs.matches(r"^(\d{4})$"),
        index="local_authority_code",
        variable_name="period",
    ).drop_nulls("value")


def annotate(data, **cols: str):
    return data.select(
        "local_authority_code",
        *[pl.lit(v).alias(k) for k, v in cols.items()],
        "period",
        "value",
    )
