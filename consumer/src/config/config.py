"""
Loads application.yaml and pipelines.yaml, substituting ${VAR} placeholders
with environment variables. Returns plain dicts - deliberately no config
dataclasses/schema layer, since the two files are small and reviewed by
hand; a schema class would just be code to keep in sync for no real benefit.
"""

import os
import re

import yaml

_ENV_VAR = re.compile(r"^\$\{([^}]+)\}$")


def _substitute_env(value):
    if isinstance(value, str):
        match = _ENV_VAR.match(value.strip())
        if match:
            return os.environ.get(match.group(1), "")
        return value

    if isinstance(value, dict):
        return {key: _substitute_env(val) for key, val in value.items()}

    if isinstance(value, list):
        return [_substitute_env(val) for val in value]

    return value


def _load_yaml(path):
    with open(path) as f:
        raw = yaml.safe_load(f)
    return _substitute_env(raw)


def load_config(config_dir):
    """Returns (application_config, pipelines_config) as dicts."""
    application = _load_yaml(os.path.join(config_dir, "application.yaml"))
    pipelines = _load_yaml(os.path.join(config_dir, "pipelines.yaml"))
    return application, pipelines
