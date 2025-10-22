from pathlib import Path

from utils import file_handler as fh
from utils.environ import create_env


env = create_env()


def hash_directory(source_dir: str) -> dict[str, str]:
    source_dir_path = Path(source_dir)
    if not source_dir_path.is_dir():
        raise ValueError(f"Source directory '{source_dir}' is not a directory.")

    file_hashes = {}
    for item in sorted(source_dir_path.iterdir()):
        file_hashes[item.name] = fh.hash_file(item)

    return file_hashes


if __name__ == "__main__":
    print(hash_directory(env.raw_data))
