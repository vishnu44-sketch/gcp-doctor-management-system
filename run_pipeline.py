"""
run_pipeline.py
---------------
Project-root entry point for the automated pipeline.
This is the file cron should call every 5 minutes.

It simply imports and runs the pipeline from src/.

Cron usage (every 5 minutes):
    */5 * * * * cd /home/azureuser/gcp-doctor-management-system && \
                /home/azureuser/gcp-doctor-management-system/venv/bin/python \
                run_pipeline.py >> logs/cron.log 2>&1
"""

from src.pipeline import run_pipeline

if __name__ == "__main__":
    run_pipeline()
