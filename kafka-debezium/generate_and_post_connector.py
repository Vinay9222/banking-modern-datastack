import json
import logging
import os
from pathlib import Path

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("debezium_connector")

# ---------------------------------------------------------------------
# Environment Configuration
# ---------------------------------------------------------------------
local_env = Path(__file__).parent / ".env"
root_env = Path(__file__).parent.parent / ".env"

if local_env.exists():
    load_dotenv(dotenv_path=local_env)
elif root_env.exists():
    load_dotenv(dotenv_path=root_env)
else:
    load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "banking")
DEBEZIUM_URL = os.getenv("DEBEZIUM_CONNECT_URL", "http://localhost:8083/connectors")

# ---------------------------------------------------------------------
# Connector Configuration
# ---------------------------------------------------------------------
connector_config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": POSTGRES_HOST,
        "database.port": POSTGRES_PORT,
        "database.user": POSTGRES_USER,
        "database.password": POSTGRES_PASSWORD,
        "database.dbname": POSTGRES_DB,
        "topic.prefix": "banking_server",
        "table.include.list": "public.customers,public.accounts,public.transactions",
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
    },
}


def register_connector():
    logger.info("Submitting Debezium Postgres CDC connector configuration to %s...", DEBEZIUM_URL)
    headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(DEBEZIUM_URL, headers=headers, data=json.dumps(connector_config), timeout=10)
        if response.status_code == 201:
            logger.info("Connector '%s' registered successfully!", connector_config["name"])
        elif response.status_code == 409:
            logger.warning("Connector '%s' already exists on Debezium Connect.", connector_config["name"])
        else:
            logger.error("Failed to create connector (Status %s): %s", response.status_code, response.text)
    except requests.exceptions.RequestException as err:
        logger.error("Could not reach Debezium Connect at %s: %s", DEBEZIUM_URL, err)


if __name__ == "__main__":
    register_connector()