"""core/types.py"""

from pathlib import Path
from typing import Any, Callable

from pandera.errors import SchemaError
from pandera.polars import DataFrameSchema
from polars import DataFrame


DataDict = dict[str, DataFrame]
JsonLike = dict[str, Any]
PipelineFn = Callable[[Path | str], DataFrame]
