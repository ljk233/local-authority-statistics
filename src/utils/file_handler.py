"""file_handler.py"""

import hashlib
import tomllib
from pathlib import Path

from .types import JsonLike


def hash_file(path: Path | str, block_size: int = 8192) -> str:
    """Return the `sha256` hex digest of the file at `path`."""
    h = hashlib.new("sha256")
    with Path(path).open("rb") as f:
        for chunk in iter(lambda: f.read(block_size), b""):
            h.update(chunk)

    return h.hexdigest()


def load_toml(file_path: Path | str) -> JsonLike:
    path = Path(file_path)
    if not path.parts[-1].endswith("toml"):
        raise ValueError(f"File '{file_path}' is not a TOML file.")

    with open(file_path, "rb") as f:
        return tomllib.load(f)
