"""
bigquery_client.py
------------------
Handles BigQuery connection and configuration loading.

Authentication flow:
1. If credentials_path is set in config and the file exists,
   uses that service account JSON file (recommended for automation).
2. Otherwise, falls back to gcloud auth application-default credentials.
3. If neither works, shows a clear error with instructions.
"""

import os
import yaml
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError


def load_config(config_path: str = "config/config.yaml") -> dict:
    """
    Load settings from the YAML config file.

    Args:
        config_path: Path to the config.yaml file.

    Returns:
        Dictionary with all config values.

    Raises:
        FileNotFoundError: If config file is missing.
    """
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
    Create and return a BigQuery client.

    Tries to use the service account JSON file specified in config.
    If not available, falls back to default gcloud credentials.

    Args:
        config: Configuration dictionary from load_config().

    Returns:
        A configured BigQuery client.

    Raises:
        FileNotFoundError: If credentials file specified but not found.
        DefaultCredentialsError: If no valid credentials are available.
    """
    project_id = config["bigquery"]["project_id"]
    credentials_path = config.get("auth", {}).get("credentials_path", "")

    # Option 1: Use service account JSON file (preferred for automation)
    if credentials_path and os.path.exists(credentials_path):
        print(f"Using service account credentials from: {credentials_path}")
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(
            project=project_id,
            credentials=credentials
        )
        return client

    # Option 2: Use default credentials (gcloud auth)
    if credentials_path:
        print(
            f"Note: credentials file '{credentials_path}' not found. "
            "Falling back to gcloud auth credentials."
        )
    else:
        print("Using gcloud auth credentials (no JSON specified).")

    try:
        client = bigquery.Client(project=project_id)
        return client
    except DefaultCredentialsError:
        raise DefaultCredentialsError(
            "No valid credentials found!\n\n"
            "To fix this, do ONE of the following:\n"
            "  1. Place your service account JSON file at: "
            f"{credentials_path or 'config/service-account.json'}\n"
            "  2. OR run this command in terminal:\n"
            "     gcloud auth application-default login\n"
        )


def get_table_id(config: dict) -> str:
    """
    Build the fully qualified table ID from config values.
    Example: "project-x.my_dataset.doctor"

    Args:
        config: Configuration dictionary from load_config().

    Returns:
        Fully qualified BigQuery table ID.
    """
    project = config["bigquery"]["project_id"]
    dataset = config["bigquery"]["dataset"]
    table = config["bigquery"]["table"]
    return f"{project}.{dataset}.{table}"
