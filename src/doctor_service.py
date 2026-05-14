"""
doctor_service.py
-----------------
Core business logic for the Doctor Management System.
Contains functions for: view, search, add, and update doctors.

Note on UPDATE: BigQuery Sandbox blocks DML (UPDATE/DELETE).
So we use a "read all, modify in memory, rewrite table" pattern.
This works in Sandbox but is only suitable for small tables.
"""

import logging
import random
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery

from src.validator import validate_doctor, validate_doctor_id, ValidationError


logger = logging.getLogger("doctor_management")


# -----------------------------------------------------------
# VIEW Operations
# -----------------------------------------------------------

def view_all_doctors(client: bigquery.Client, table_id: str) -> pd.DataFrame:
    """
    Fetch all doctors from BigQuery and return as a DataFrame.

    Args:
        client: BigQuery client instance.
        table_id: Fully qualified table ID.

    Returns:
        DataFrame with all doctor records.
    """
    logger.info(f"Fetching all doctors from {table_id}")

    query = f"SELECT * FROM `{table_id}` ORDER BY doctor_id"

    try:
        df = client.query(query).to_dataframe()
        logger.info(f"Retrieved {len(df)} doctor record(s)")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch doctors: {e}")
        raise


def view_doctor_by_id(client: bigquery.Client, table_id: str, doctor_id: str) -> pd.DataFrame:
    """
    Fetch a single doctor by ID.

    Args:
        client: BigQuery client instance.
        table_id: Fully qualified table ID.
        doctor_id: ID of the doctor to find (e.g., 'D012').

    Returns:
        DataFrame with the matching doctor (or empty if not found).
    """
    validate_doctor_id(doctor_id)
    logger.info(f"Searching for doctor with ID: {doctor_id}")

    query = f"""
        SELECT * FROM `{table_id}`
        WHERE doctor_id = @doctor_id
    """

    # Use parameterized query to prevent SQL injection
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("doctor_id", "STRING", doctor_id)
        ]
    )

    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        if df.empty:
            logger.warning(f"No doctor found with ID: {doctor_id}")
        else:
            logger.info(f"Found doctor: {doctor_id}")
        return df
    except Exception as e:
        logger.error(f"Failed to search doctor {doctor_id}: {e}")
        raise


# -----------------------------------------------------------
# ADD Operations
# -----------------------------------------------------------

def doctor_exists(client: bigquery.Client, table_id: str, doctor_id: str) -> bool:
    """Check if a doctor with the given ID already exists."""
    df = view_doctor_by_id(client, table_id, doctor_id)
    return not df.empty


def add_doctor(client: bigquery.Client, table_id: str, doctor: dict) -> int:
    """
    Add a new doctor to BigQuery using load_table_from_dataframe.
    This method works in Sandbox mode (unlike SQL INSERT).

    Args:
        client: BigQuery client instance.
        table_id: Fully qualified table ID.
        doctor: Dictionary with doctor fields.

    Returns:
        Number of rows inserted (should be 1).
    """
    # Step 1: Validate the data
    logger.info(f"Validating doctor data for ID: {doctor.get('doctor_id')}")
    validate_doctor(doctor)

    # Step 2: Check for duplicate
    if doctor_exists(client, table_id, doctor["doctor_id"]):
        raise ValidationError(
            f"Doctor with ID '{doctor['doctor_id']}' already exists. "
            "Use update instead."
        )

    # Step 3: Convert to DataFrame
    df = pd.DataFrame([doctor])

    # Ensure proper data types
    df["experience_years"] = df["experience_years"].astype(int)
    df["phone"] = df["phone"].astype(int)

    # Step 4: Load into BigQuery (WRITE_APPEND mode)
    logger.info(f"Inserting doctor {doctor['doctor_id']} into BigQuery")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion
        logger.info(f"Successfully inserted {job.output_rows} doctor record(s)")
        return job.output_rows
    except Exception as e:
        logger.error(f"Failed to insert doctor: {e}")
        raise


# -----------------------------------------------------------
# UPDATE Operations (Sandbox-safe workaround)
# -----------------------------------------------------------

def update_doctor(client: bigquery.Client, table_id: str,
                  doctor_id: str, updates: dict) -> int:
    """
    Update an existing doctor's fields.

    Strategy (Sandbox-safe):
    1. Read ALL rows from table
    2. Modify the target row in memory
    3. Rewrite the entire table (WRITE_TRUNCATE)

    Args:
        client: BigQuery client instance.
        table_id: Fully qualified table ID.
        doctor_id: ID of the doctor to update.
        updates: Dictionary of {field: new_value} to update.

    Returns:
        Number of rows updated (1 if successful, 0 if not found).
    """
    validate_doctor_id(doctor_id)
    logger.info(f"Updating doctor {doctor_id} with: {list(updates.keys())}")

    # Step 1: Read all data
    query = f"SELECT * FROM `{table_id}`"
    df = client.query(query).to_dataframe()

    # Step 2: Find the row to update
    mask = df["doctor_id"] == doctor_id
    if not mask.any():
        logger.warning(f"Doctor {doctor_id} not found for update")
        return 0

    # Step 3: Apply updates in memory
    for field, new_value in updates.items():
        if field == "doctor_id":
            logger.warning("Cannot change doctor_id - skipping")
            continue
        if field not in df.columns:
            logger.warning(f"Unknown field '{field}' - skipping")
            continue
        df.loc[mask, field] = new_value
        logger.info(f"  {field} -> {new_value}")

    # Step 4: Ensure correct types after modification
    df["experience_years"] = df["experience_years"].astype(int)
    df["phone"] = df["phone"].astype(int)

    # Step 5: Rewrite the entire table
    logger.info(f"Rewriting table with updated data ({len(df)} rows)")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info(f"Successfully updated doctor {doctor_id}")
        return 1
    except Exception as e:
        logger.error(f"Failed to update doctor {doctor_id}: {e}")
        raise


# -----------------------------------------------------------
# Statistics
# -----------------------------------------------------------

def get_doctor_count(client: bigquery.Client, table_id: str) -> int:
    """Return total number of doctors in the table."""
    query = f"SELECT COUNT(*) as cnt FROM `{table_id}`"
    result = client.query(query).result()
    for row in result:
        return row.cnt
    return 0


# -----------------------------------------------------------
# Pipeline Operations (for the automated cron job)
# -----------------------------------------------------------

# Sample data pools for generating random doctors
_FIRST_NAMES = ["Arjun", "Priya", "Karthik", "Deepa", "Suresh", "Lakshmi",
                "Vikram", "Anjali", "Rahul", "Sneha", "Ganesh", "Pooja"]
_SPECIALIZATIONS = ["Cardiology", "Neurology", "Orthopedics", "Pediatrics",
                    "Dermatology", "Oncology", "Radiology", "Psychiatry"]
_QUALIFICATIONS = ["MBBS, MD", "MBBS, MS", "MBBS, DM", "MBBS, DNB"]
_HOSPITALS = ["Apollo Hospital", "Fortis Hospital", "Manipal Hospital",
              "Max Hospital", "Columbia Asia", "Narayana Health"]
_CITIES = ["Chennai", "Bangalore", "Mumbai", "Delhi", "Hyderabad", "Pune"]


def generate_random_doctor(client: bigquery.Client, table_id: str) -> dict:
    """
    Generate a random doctor record with a unique doctor_id and
    a created_at timestamp set to the current UTC time.

    The doctor_id is built from the current count + a random suffix
    to reduce the chance of collisions.

    Args:
        client: BigQuery client instance.
        table_id: Fully qualified table ID.

    Returns:
        Dictionary representing a new doctor record.
    """
    # Build a reasonably unique ID
    count = get_doctor_count(client, table_id)
    unique_num = count + random.randint(1000, 9999)
    doctor_id = f"D{unique_num}"

    first = random.choice(_FIRST_NAMES)
    doctor = {
        "doctor_id": doctor_id,
        "name": f"Dr. {first}",
        "specialization": random.choice(_SPECIALIZATIONS),
        "qualification": random.choice(_QUALIFICATIONS),
        "hospital": random.choice(_HOSPITALS),
        "city": random.choice(_CITIES),
        "experience_years": random.randint(1, 35),
        "email": f"{first.lower()}{random.randint(1, 999)}@hospital.com",
        "phone": random.randint(9000000000, 9999999999),
        "created_at": datetime.now(timezone.utc),
    }

    logger.info(f"Generated random doctor: {doctor_id}")
    return doctor


def insert_doctor_with_timestamp(client: bigquery.Client, table_id: str,
                                 doctor: dict) -> int:
    """
    Insert a doctor record that already includes a 'created_at' field.

    Unlike add_doctor(), this does NOT validate or check duplicates -
    it is meant for the automated pipeline inserting generated data.

    Args:
        client: BigQuery client instance.
        table_id: Fully qualified table ID.
        doctor: Dictionary with doctor fields (including created_at).

    Returns:
        Number of rows inserted.
    """
    df = pd.DataFrame([doctor])

    # Ensure correct data types
    df["experience_years"] = df["experience_years"].astype("int64")
    df["phone"] = df["phone"].astype("int64")
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)

    logger.info(f"Inserting generated doctor {doctor['doctor_id']} into BigQuery")
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        logger.info(f"Inserted {job.output_rows} generated row(s)")
        return job.output_rows
    except Exception as e:
        logger.error(f"Failed to insert generated doctor: {e}")
        raise


def get_doctors_since(client: bigquery.Client, table_id: str,
                      since_timestamp: str) -> pd.DataFrame:
    """
    Fetch all doctor rows created AFTER the given timestamp.
    This is the core of incremental loading - only NEW rows are returned.

    Args:
        client: BigQuery client instance.
        table_id: Fully qualified table ID.
        since_timestamp: Timestamp string 'YYYY-MM-DD HH:MM:SS' (UTC).

    Returns:
        DataFrame with rows where created_at > since_timestamp.
    """
    logger.info(f"Fetching doctors created after: {since_timestamp}")

    query = f"""
        SELECT * FROM `{table_id}`
        WHERE created_at > TIMESTAMP(@since_ts)
        ORDER BY created_at
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "since_ts", "TIMESTAMP", since_timestamp
            )
        ]
    )

    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        logger.info(f"Found {len(df)} new row(s) since last run")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch incremental rows: {e}")
        raise
