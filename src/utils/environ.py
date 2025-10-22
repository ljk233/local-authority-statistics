"""environ.py"""

from typing import NamedTuple

import dotenv


class Env(NamedTuple):
    raw_data: str
    staged_data: str


def create_env() -> Env:
    if not dotenv.load_dotenv():
        raise EnvironmentError("No .env file found.")

    print(dotenv.dotenv_values().items())

    parsed_env = {k.lower(): v for k, v in dotenv.dotenv_values().items() if v}

    return Env(**parsed_env)
