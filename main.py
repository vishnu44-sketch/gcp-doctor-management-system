"""
main.py
-------
Entry point for the Doctor Management System.
Run this file to start the interactive menu:
    python main.py
"""

import sys
from src.logger import setup_logger
from src.bigquery_client import load_config, get_bigquery_client, get_table_id
from src.doctor_service import (
    view_all_doctors,
    view_doctor_by_id,
    add_doctor,
    update_doctor,
    get_doctor_count,
)
from src.validator import ValidationError


def print_banner(app_name: str, version: str):
    """Print the application banner."""
    print("\n" + "=" * 50)
    print(f"   {app_name.upper()}")
    print(f"   Version: {version}")
    print("=" * 50)


def print_menu():
    """Display the main menu options."""
    print("\n--- MAIN MENU ---")
    print("1. View all doctors")
    print("2. Search doctor by ID")
    print("3. Add new doctor")
    print("4. Update existing doctor")
    print("5. Show total count")
    print("6. Exit")
    print()


def handle_view_all(client, table_id, logger):
    """Show all doctors in the table."""
    try:
        df = view_all_doctors(client, table_id)
        if df.empty:
            print("\nNo doctors found in the database.")
        else:
            print(f"\n{len(df)} doctor(s) found:\n")
            print(df.to_string(index=False))
    except Exception as e:
        print(f"\nError: {e}")


def handle_view_by_id(client, table_id, logger):
    """Search for a specific doctor by ID."""
    doctor_id = input("Enter Doctor ID (e.g., D001): ").strip().upper()
    try:
        df = view_doctor_by_id(client, table_id, doctor_id)
        if df.empty:
            print(f"\nNo doctor found with ID: {doctor_id}")
        else:
            print(f"\nDoctor details:\n")
            print(df.to_string(index=False))
    except ValidationError as e:
        print(f"\nValidation error: {e}")
    except Exception as e:
        print(f"\nError: {e}")


def handle_add(client, table_id, logger):
    """Prompt for doctor details and add to BigQuery."""
    print("\n--- Add New Doctor ---")
    doctor = {
        "doctor_id": input("Doctor ID (e.g., D012): ").strip().upper(),
        "name": input("Full name (e.g., Dr. Ramya): ").strip(),
        "specialization": input("Specialization: ").strip(),
        "qualification": input("Qualification (e.g., MBBS, MD): ").strip(),
        "hospital": input("Hospital: ").strip(),
        "city": input("City: ").strip(),
        "experience_years": input("Experience (years): ").strip(),
        "email": input("Email: ").strip(),
        "phone": input("Phone (10 digits): ").strip(),
    }

    try:
        rows = add_doctor(client, table_id, doctor)
        print(f"\nDoctor {doctor['doctor_id']} added successfully! ({rows} row)")
    except ValidationError as e:
        print(f"\nValidation error: {e}")
    except Exception as e:
        print(f"\nError: {e}")


def handle_update(client, table_id, logger):
    """Update fields of an existing doctor."""
    print("\n--- Update Doctor ---")
    doctor_id = input("Enter Doctor ID to update: ").strip().upper()

    # Show current data first
    try:
        df = view_doctor_by_id(client, table_id, doctor_id)
        if df.empty:
            print(f"\nNo doctor found with ID: {doctor_id}")
            return
        print(f"\nCurrent details:\n")
        print(df.to_string(index=False))
    except ValidationError as e:
        print(f"\nValidation error: {e}")
        return

    print("\nWhich field do you want to update?")
    print("Options: name, specialization, qualification, hospital, city,")
    print("         experience_years, email, phone")
    print("(Type the field name exactly, or 'cancel' to abort)")

    field = input("Field name: ").strip().lower()
    if field == "cancel" or not field:
        print("Update cancelled.")
        return

    new_value = input(f"New value for {field}: ").strip()

    # Convert types for numeric fields
    if field == "experience_years":
        try:
            new_value = int(new_value)
        except ValueError:
            print(f"\nError: experience_years must be a number.")
            return
    elif field == "phone":
        try:
            new_value = int(new_value)
        except ValueError:
            print(f"\nError: phone must be a number.")
            return

    try:
        rows = update_doctor(client, table_id, doctor_id, {field: new_value})
        if rows > 0:
            print(f"\nDoctor {doctor_id} updated successfully!")
        else:
            print(f"\nNo doctor found with ID: {doctor_id}")
    except Exception as e:
        print(f"\nError: {e}")


def handle_count(client, table_id, logger):
    """Show total number of doctors."""
    try:
        count = get_doctor_count(client, table_id)
        print(f"\nTotal doctors in database: {count}")
    except Exception as e:
        print(f"\nError: {e}")


def main():
    """Application entry point."""
    # Step 1: Load configuration
    try:
        config = load_config("config/config.yaml")
    except FileNotFoundError as e:
        print(f"FATAL: {e}")
        sys.exit(1)

    # Step 2: Setup logger
    logger = setup_logger(
        log_file=config["logging"]["log_file"],
        log_level=config["logging"]["log_level"],
    )
    logger.info("=" * 50)
    logger.info("Application starting")

    # Step 3: Connect to BigQuery
    try:
        client = get_bigquery_client(config)
        table_id = get_table_id(config)
        logger.info(f"Connected to BigQuery. Table: {table_id}")
    except Exception as e:
        logger.error(f"Failed to connect to BigQuery: {e}")
        print(f"FATAL: Could not connect to BigQuery: {e}")
        sys.exit(1)

    # Step 4: Show banner
    print_banner(config["app"]["name"], config["app"]["version"])

    # Step 5: Menu loop
    handlers = {
        "1": handle_view_all,
        "2": handle_view_by_id,
        "3": handle_add,
        "4": handle_update,
        "5": handle_count,
    }

    while True:
        print_menu()
        choice = input("Enter your choice (1-6): ").strip()

        if choice == "6":
            print("\nGoodbye!")
            logger.info("Application exiting normally")
            break

        handler = handlers.get(choice)
        if handler:
            handler(client, table_id, logger)
        else:
            print("\nInvalid choice. Please enter 1-6.")

        input("\nPress Enter to continue...")


if __name__ == "__main__":
    main()
