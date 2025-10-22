from pathlib import Path

import polars as pl
from polars import selectors as cs

from . import schema
from pipeline.registry import register_staging_pipeline


hash = "576dc4b91bd4894399ee024f7642b33c4a37b6216b03c622af51e7949a96111e"
schema = schema.Fact
metric_group = "Annual FT gross pay"
sheet_metric = {
    "Median": {
        "metric": "Median",
        "code": "annual_ft_gross_pay_med",
        "unit": "GBP (£)",
    },
    "25 percentile": {
        "metric": "Lower quartile",
        "code": "annual_ft_gross_pay_q25",
        "unit": "GBP (£)",
    },
    "75 percentile": {
        "metric": "Upper quartile",
        "code": "annual_ft_gross_pay_q75",
        "unit": "GBP (£)",
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
        source, sheet_name=sheet_name, read_options={"header_row": 8, "n_rows": 351}
    )


def clean(data: pl.DataFrame) -> pl.DataFrame:
    return data.filter(
        pl.col("__UNNAMED__1").str.extract(r"^(\w)").is_in(["E", "W"])
    ).select(
        pl.col("__UNNAMED__1").alias("local_authority_code"),
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
