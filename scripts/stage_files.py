"""scripts/stage_files.py"""

import staging
from pipeline import runner
from pipeline import registry
from utils import environ


def stage_files():
    env = environ.create_env()
    registry.load_staging_pipelines(staging)
    outcomes = runner.stage_files(env.raw_data, env.staged_data)


if __name__ == "__main__":
    stage_files()
