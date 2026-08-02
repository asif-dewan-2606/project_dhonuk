import os
import re

import yaml

_VAR_PATTERN = re.compile(r"\$\{([A-Za-z0-9_]+)(:-([^}]*))?\}")


def _substitute_env(value):
    """Recursively replaces ${VAR} / ${VAR:-default} in strings, dicts, and
    lists loaded from YAML - the same convention docker-compose.yml and the
    consumer's application.yaml already use."""
    if isinstance(value, str):
        def replace(match):
            var_name, _, default = match.groups()
            return os.environ.get(var_name, default if default is not None else "")

        return _VAR_PATTERN.sub(replace, value)

    if isinstance(value, dict):
        return {k: _substitute_env(v) for k, v in value.items()}

    if isinstance(value, list):
        return [_substitute_env(v) for v in value]

    return value


def load_sources_config(path: str) -> dict:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)

    return _substitute_env(raw)
