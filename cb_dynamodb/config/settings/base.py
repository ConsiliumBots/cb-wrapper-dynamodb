from pathlib import Path
import json
import os

# Secrets
BASE_DIR = Path(__file__).resolve().parent.parent

with open(os.path.join("config", "settings", "secrets.json")) as f:
    _secrets = json.loads(f.read())


def get_secret(setting, secrets=_secrets):
    """Get the secret variable or raises ImproperlyConfigured."""
    try:
        return secrets[setting]
    except KeyError:
        error_msg = "Set the {0} variable on secrets.json file.".format(setting)
        raise KeyError(error_msg)

CURRENT_ENVIRONMENT = get_secret("ENVIRONMENT")