from pathlib import Path

import polars as pl
from polars import selectors as cs

from . import schema
from pipeline.registry import register_staging_pipeline


hash = "ff64beb6b1e9ce44be43d04c656f2f2514a91d6c45dda2b7ab8bad77785ff120"
schema = schema.Fact
metric_group = "Subnational indicators"
sheet_metric = {
    "1": {
        "metric": "Mid-year estimate of the total number of people.",
        "code": "population",
        "unit": "people",
    },
    "2": {
        "metric": "Number of people resident per square kilometre.",
        "code": "population_density",
        "unit": "people_km2",
    },
    "3": {
        "metric": "Median age of people.",
        "code": "median_age",
        "unit": "years",
    },
    "4": {
        "metric": "Percentage of the population aged 0 to 15.",
        "code": "young_person_pct",
        "unit": "%",
    },
    "5": {
        "metric": "Percentage of the population aged 16 to 64.",
        "code": "adult_person_pct",
        "unit": "%",
    },
    "6": {
        "metric": "Percentage of the population aged 65 and over.",
        "code": "elderly_person_pct",
        "unit": "%",
    },
    "7": {
        "metric": "Percentage of people aged 16 to 64 who are not in employment and are not actively seeking work.",
        "code": "economic_inactivity_pct",
        "unit": "%",
    },
    "9": {
        "metric": "Percentage of people aged 16 to 64 in employment.",
        "code": "employment_pct",
        "unit": "%",
    },
    "11": {
        "metric": "Modelled percentage of economically active people aged 16 years and over without a job and actively seeking work.",
        "code": "unemployment_pct",
        "unit": "%",
    },
    "12": {
        "metric": "Percentage of people aged 16 to 64 who are claiming unemployment-related benefits.",
        "code": "claimant_pct",
        "unit": "%",
    },
    "15": {
        "metric": "Percentage of children aged 0 to 15 living in relative low income families Before Housing Costs (BHC).",
        "code": "child_relative_poverty_pct",
        "unit": "%",
    },
    "31": {
        "metric": "First-time buyer mortgage sales per 1,000 dwellings.",
        "code": "first_time_buyer_rate",
        "unit": "rate (per thousand dwellings)",
    },
    "34": {
        "metric": "Percentage of people achieving GCSEs in both subjects by age 19.",
        "code": "gcse_attainment_pct",
        "unit": "%",
    },
    "35": {
        "metric": "Percentage of people aged 16 to 64 with Level 3 or above qualifications.",
        "code": "l3_plus_quals_pct",
        "unit": "%",
    },
    "37": {
        "metric": "Percentage of pupils in state-funded schools meeting the expected standard in reading, writing and maths by the end of Key Stage 2.",
        "code": "ks2_standard_skills_pct",
        "unit": "%",
    },
    "38": {
        "metric": "Percentage of people aged 16 to 64 with no qualifications.",
        "code": "no_quals_pct",
        "unit": "%",
    },
    "64": {
        "metric": "Average years expected to be lived in good health for females born during year.",
        "code": "female_halthy_life_exp",
        "unit": "years",
    },
    "65": {
        "metric": "Average years expected to be lived in good health for males born during year.",
        "code": "male_halthy_life_exp",
        "unit": "years",
    },
}


@register_staging_pipeline(hash, schema)
def stage(source: Path | str) -> pl.DataFrame:
    source_path = Path(source)
    staged = []
    for sheet_name, metric_info in sheet_metric.items():
        data = load(source, sheet_name)
        cleansed = clean(data)
        annotated = annotate(
            cleansed,
            metric_group=metric_group,
            **metric_info,
            source=source_path.name,
        )
        staged.append(annotated)

    return pl.concat(staged)


def load(source: Path | str, sheet_name: str) -> pl.DataFrame:
    return pl.read_excel(source, sheet_name=sheet_name, read_options={"header_row": 5})


def clean(data: pl.DataFrame) -> pl.DataFrame:
    local_auth_col = "Area code"
    return (
        data.filter(pl.col(local_auth_col).str.extract(r"^(\w)").is_in(["E", "W"]))
        .select(
            pl.col(local_auth_col).alias("local_authority_code"),
            cs.matches("Period").str.extract(r"^(\d{4})").alias("period"),
            cs.starts_with("Value").alias("value").cast(pl.Float64, strict=False),
        )
        .drop_nulls()
    )


def annotate(data, **cols: str):
    return data.select(
        "local_authority_code",
        *[pl.lit(v).alias(k) for k, v in cols.items()],
        "period",
        "value",
    )
