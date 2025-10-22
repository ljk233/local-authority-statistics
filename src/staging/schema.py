"""src/staging/schema.py"""

from pandera import polars as pa


Fact = pa.DataFrameSchema(
    {
        "local_authority_code": pa.Column(str),
        "metric_group": pa.Column(str),
        "metric": pa.Column(str),
        "code": pa.Column(str),
        "unit": pa.Column(str),
        "source": pa.Column(str),
        "period": pa.Column(str),
        "value": pa.Column(float),
    },
    strict=True,
    coerce=True,
    unique=["local_authority_code", "metric", "period"],
)


LocalAuthorityHierarchy = pa.DataFrameSchema(
    {
        "local_authority_code": pa.Column(str, unique=True),
        "local_authority_name": pa.Column(str),
        "region_name": pa.Column(str),
        "region_code": pa.Column(str),
        "country_code": pa.Column(str),
        "country_name": pa.Column(str),
    },
    strict=True,
    coerce=True,
)
