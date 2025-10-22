from pathlib import Path

import polars as pl
from polars import selectors as cs

from . import schema
from pipeline.registry import register_staging_pipeline


hash = "e331a601e8fb0f1e53395deedd30e273b8b060553a9da7ac1cd910f58edb9fd0"
schema = schema.Fact
metric_group = "Gross domestic household spending per head"
sheet_metric = {
    "GDHI per head (£)": {
        "metric": "GDHI per head",
        "code": "gdhi",
        "unit": "GBP (£)",
    },
    "GDHI per head index (UK = 100)": {
        "metric": "GDHI per head index",
        "code": "gdhi_index",
        "unit": "None (UK = 100)",
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
    return pl.read_excel(
        source, sheet_name=sheet_name, read_options={"header_row": 6, "n_rows": 361}
    )


def clean(data: pl.DataFrame) -> pl.DataFrame:
    local_auth_col = "local authority: district / unitary (as of April 2023)"
    return data.filter(
        pl.col(local_auth_col).str.extract(r"^(\w)").is_in(["E", "W"])
    ).select(
        pl.col(local_auth_col).alias("local_authority_code"),
        cs.matches(r"^(\d{4})$").cast(pl.Float64, strict=False),
    )


def transform(data: pl.DataFrame) -> pl.DataFrame:
    return data.unpivot(
        on=cs.matches(r"^(\d{4})$"),
        index="local_authority_code",
        variable_name="period",
    )


def annotate(data, **cols: str):
    return data.select(
        "local_authority_code",
        *[pl.lit(v).alias(k) for k, v in cols.items()],
        "period",
        "value",
    )
