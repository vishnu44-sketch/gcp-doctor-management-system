# Doctor Management System

A Python application to manage doctor records in Google BigQuery.
This is a real-world style project with proper folder structure,
configuration files, logging, and data validation.

---

## Features

- **View** all doctors or search by ID
- **Add** new doctor records with validation
- **Update** existing doctor fields (Sandbox-safe)
- **Count** total doctors in the database
- Centralized **configuration** (no hardcoded values)
- Proper **logging** to file and console
- Input **validation** with clear error messages

---

## Project Structure

```
doctor_management_system/
│
├── config/
│   └── config.yaml              # All project settings
│
├── src/
│   ├── __init__.py              # Package marker
│   ├── bigquery_client.py       # BigQuery connection
│   ├── doctor_service.py        # Add, update, view operations
│   ├── validator.py             # Data validation rules
│   └── logger.py                # Logging setup
│
├── logs/                        # Auto-generated log files
│
├── main.py                      # Entry point - run this
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## Setup Instructions

### 1. Install Python 3.8 or higher

Check your version:
```bash
python --version
```

### 2. Install Required Libraries

Open a terminal in the project folder and run:
```bash
pip install -r requirements.txt
```

### 3. Authenticate with Google Cloud

Run this once (opens browser for login):
```bash
gcloud auth application-default login
```

### 4. Update Configuration

Open `config/config.yaml` and update with your settings:
```yaml
bigquery:
  project_id: "your-project-id"
  dataset: "your_dataset"
  table: "doctor"
```

### 5. Ensure the BigQuery Table Exists

The `doctor` table must exist with this schema:

| Column | Type |
|--------|------|
| doctor_id | STRING |
| name | STRING |
| specialization | STRING |
| qualification | STRING |
| hospital | STRING |
| city | STRING |
| experience_years | INTEGER |
| email | STRING |
| phone | INTEGER |

---

## How to Run

From the project folder:
```bash
python main.py
```

You will see an interactive menu:
```
==================================================
   DOCTOR MANAGEMENT SYSTEM
   Version: 1.0.0
==================================================

--- MAIN MENU ---
1. View all doctors
2. Search doctor by ID
3. Add new doctor
4. Update existing doctor
5. Show total count
6. Exit
```

---

## Important Notes on BigQuery Sandbox

This project is built to work in **BigQuery Sandbox mode** (free tier).

| Operation | Method Used | Sandbox? |
|-----------|-------------|----------|
| View | `SELECT` query | OK |
| Add | `load_table_from_dataframe` with WRITE_APPEND | OK |
| Update | Read all -> modify in memory -> rewrite table | OK |

**Note:** The update strategy works in Sandbox but rewrites the entire table.
For production with large tables, enable billing and use `UPDATE` SQL instead.

---

## Validation Rules

When adding a new doctor, the following rules are enforced:

- **doctor_id**: Must match format `D + digits` (e.g., D001, D012)
- **name**: At least 3 characters
- **email**: Must be a valid email format
- **phone**: Exactly 10 digits
- **experience_years**: Between 0 and 80
- **All fields**: Cannot be empty

Validation errors show a clear message explaining what to fix.

---

## Logs

All operations are logged to `logs/app.log` with timestamps.
Example log entry:
```
2026-05-13 14:32:10 | INFO    | Inserting doctor D012 into BigQuery
2026-05-13 14:32:12 | INFO    | Successfully inserted 1 doctor record(s)
```

You can review this file to troubleshoot issues or track activity.

---

## Troubleshooting

### "FileNotFoundError: config/config.yaml"
Run the app from the project root folder, not from inside `src/`.

### "google.auth.exceptions.DefaultCredentialsError"
Run `gcloud auth application-default login` to authenticate.

### "Permission denied" errors
Make sure your Google account has BigQuery Data Editor role on the dataset.

### "DML queries are not allowed in the free tier"
This should not happen with this project (we use load jobs, not DML).
If you see it, double-check you have the latest version of `doctor_service.py`.

---

## Future Improvements

Items skipped for now but useful for a true production project:

- Git/GitHub integration
- Automated scheduling with Airflow
- Service account credentials (instead of user login)
- Secret Manager for sensitive values
- Multiple environments (dev/staging/prod)
- Unit tests with pytest
- Docker containerization
- CI/CD pipeline

---

## Author

Created as a learning project to demonstrate real-world Python + BigQuery practices.
