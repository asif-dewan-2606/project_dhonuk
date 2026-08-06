from pathlib import Path

import yaml


BASE_DIR = Path(__file__).resolve().parent

APPLICATION_FILE = BASE_DIR / "application.yaml"
PIPELINES_FILE = BASE_DIR / "pipelines.yaml"


def load_yaml(path: Path):

    with open(path, "r") as f:
        return yaml.safe_load(f)


def application():

    return load_yaml(APPLICATION_FILE)


def pipelines():

    return load_yaml(PIPELINES_FILE)["jobs"]