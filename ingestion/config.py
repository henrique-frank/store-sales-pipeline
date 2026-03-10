import os
import re

import yaml


def _resolve_env_vars(value):
    """Replace ${ENV_VAR} placeholders with environment variable values."""
    if isinstance(value, str):
        def replacer(match):
            var_name = match.group(1)
            return os.environ.get(var_name, match.group(0))
        return re.sub(r"\$\{(\w+)\}", replacer, value)
    if isinstance(value, dict):
        return {k: _resolve_env_vars(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_env_vars(item) for item in value]
    return value


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        raw = yaml.safe_load(f)
    return _resolve_env_vars(raw)
