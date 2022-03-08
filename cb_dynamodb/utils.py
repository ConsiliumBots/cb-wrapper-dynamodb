import boto3
import json
import os
import logging
from logdna import LogDNAHandler



def load_credentials(secretid: str, region: str) -> dict:
    """
    Loads authentication credentials from AWS secrets manager and returns them
    """
    client = boto3.client("secretsmanager", region_name=region)
    credentials = json.loads(client.get_secret_value(SecretId=secretid)["SecretString"])
    return credentials


def set_logger(name: str, level=None, ingestion_key=None, env=None):
    credentials = load_credentials(
        secretid="cb-squirrel-mailer-secrets", region="us-east-1"
    )
    if ingestion_key is None:
        key = os.environ["LOGDNA_INGESTION_KEY"]

    if level is None:
        level = os.environ["LOG_LEVEL"]

    if env is None:
        env = os.environ["ENVIRONMENT"]

    log = logging.getLogger(name)

    options = {
        "index_meta": True,
        "hostname": "cb-messaging-api",
        "tags": ["cb-messaging-api"],
        "env": env,
        "level": level.capitalize()
        if level is None
        else level.capitalize(),
    }
    if os.environ["ENVIRONMENT"] != "TESTING":
        log.addHandler(hdlr=LogDNAHandler(key, options))

    return log
