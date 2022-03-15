import boto3
import json
import logging
from logdna import LogDNAHandler

try:
    from config.settings.base import CURRENT_ENVIRONMENT
except:
    pass


def load_credentials(secretid: str, region: str) -> dict:
    """
    Loads authentication credentials from AWS secrets manager and returns them
    """
    client = boto3.client("secretsmanager", region_name=region)
    credentials = json.loads(client.get_secret_value(SecretId=secretid)["SecretString"])
    return credentials


def set_logger(name: str, level=None):
    credentials = load_credentials(
        secretid="cb-explorer-cl-secrets", region="us-east-1"
    )

    key = credentials["LOGDNA_INGESTION_KEY"]

    if level is None:
        level = credentials["LOG_LEVEL"]

    if not "CURRENT_ENVIRONMENT" in locals():
        CURRENT_ENVIRONMENT = "LOCAL"

    log = logging.getLogger(name)

    options = {
        "index_meta": True,
        "hostname": "cb-messaging-api",
        "tags": ["cb-messaging-api"],
        "env": CURRENT_ENVIRONMENT,
        "level": level.capitalize() if level is None else level.capitalize(),
    }

    log.addHandler(hdlr=LogDNAHandler(key, options))

    return log
