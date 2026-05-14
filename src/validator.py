"""
validator.py
------------
Validates doctor data before saving to BigQuery.
Catches bad data early so we never insert corrupt records.
"""

import re


# Required fields for every doctor record
REQUIRED_FIELDS = [
    "doctor_id", "name", "specialization", "qualification",
    "hospital", "city", "experience_years", "email", "phone"
]


class ValidationError(Exception):
    """Raised when doctor data fails validation."""
    pass


def validate_doctor(doctor: dict) -> None:
    """
    Check that doctor data is valid.
    Raises ValidationError with a clear message if anything is wrong.

    Args:
        doctor: Dictionary with doctor fields.

    Raises:
        ValidationError: If any validation rule fails.
    """
    # Rule 1: All required fields must be present
    missing = [f for f in REQUIRED_FIELDS if f not in doctor or doctor[f] in (None, "")]
    if missing:
        raise ValidationError(f"Missing required fields: {missing}")

    # Rule 2: doctor_id must follow pattern: D + digits (e.g. D001, D012)
    if not re.match(r"^D\d{3,}$", str(doctor["doctor_id"])):
        raise ValidationError(
            f"Invalid doctor_id '{doctor['doctor_id']}'. "
            "Must be 'D' followed by at least 3 digits (e.g., D012)."
        )

    # Rule 3: experience_years must be a non-negative integer
    try:
        exp = int(doctor["experience_years"])
        if exp < 0 or exp > 80:
            raise ValueError
    except (ValueError, TypeError):
        raise ValidationError(
            f"experience_years must be a number between 0 and 80. "
            f"Got: {doctor['experience_years']}"
        )

    # Rule 4: email must look like an email
    email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if not re.match(email_pattern, str(doctor["email"])):
        raise ValidationError(f"Invalid email format: {doctor['email']}")

    # Rule 5: phone must be a 10-digit number
    phone_str = str(doctor["phone"])
    if not phone_str.isdigit() or len(phone_str) != 10:
        raise ValidationError(
            f"Phone must be exactly 10 digits. Got: {doctor['phone']}"
        )

    # Rule 6: name must be at least 3 characters
    if len(str(doctor["name"]).strip()) < 3:
        raise ValidationError(f"Name too short: '{doctor['name']}'")


def validate_doctor_id(doctor_id: str) -> None:
    """
    Validate just the doctor_id format (used for view/update operations).

    Args:
        doctor_id: The ID string to validate.

    Raises:
        ValidationError: If format is invalid.
    """
    if not re.match(r"^D\d{3,}$", str(doctor_id)):
        raise ValidationError(
            f"Invalid doctor_id format: '{doctor_id}'. "
            "Must be 'D' followed by at least 3 digits (e.g., D012)."
        )
