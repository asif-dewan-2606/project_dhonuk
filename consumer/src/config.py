from pathlib import Path
from types import SimpleNamespace

import yaml


BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = BASE_DIR / "config"


def _namespace(obj):
    if isinstance(obj, dict):
        return SimpleNamespace(**{k: _namespace(v) for k, v in obj.items()})

    if isinstance(obj, list):
        return [_namespace(item) for item in obj]

    return obj


def load_yaml(filename: str):
    with open(CONFIG_DIR / filename, "r", encoding="utf-8") as file:
        return _namespace(yaml.safe_load(file))


config = load_yaml("application.yaml")
pipeline = load_yaml("pipelines.yaml")