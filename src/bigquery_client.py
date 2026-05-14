"""
bigquery_client.py
------------------
Handles BigQuery connection using a service account JSON file.
"""

import os
import yaml
from google.cloud import bigquery
from google.oauth2 import service_account


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load settings from the YAML config file."""
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Config file not found: {config_path}\n"
            "Make sure you run the app from the project root folder."
        )

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config


def get_bigquery_client(config: dict) -> bigquery.Client:
    """
    Create and return a BigQuery client using the service account JSON.

    Args:
        config: Configuration dictionary from load_config().

    Returns:
        A configured BigQuery client.
    """
    project_id = config["bigquery"]["project_id"]
    credentials_path = config["auth"]["credentials_path"]

    if not os.path.exists(credentials_path):
        raise FileNotFoundError(
            f"Service account JSON not found at: {credentials_path}\n"
            "Make sure the JSON file is in the config/ folder."
        )

    print(f"Authenticating with: {credentials_path}")

    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    client = bigquery.Client(
        project=project_id,
        credentials=credentials
    )

    print(f"Connected to BigQuery project: {project_id}")
    return client


def get_table_id(config: dict) -> str:
    """Build the fully qualified table ID."""
    project = config["bigquery"]["project_id"]
    dataset = config["bigquery"]["dataset"]
    table = config["bigquery"]["table"]
    return f"{project}.{dataset}.{table}"