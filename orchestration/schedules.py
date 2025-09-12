# orchestration/schedules.py
from dagster import ScheduleDefinition
from orchestration.jobs import london_bicycles_job

london_bicycles_schedule = ScheduleDefinition(
    job=london_bicycles_job,
    cron_schedule="0 * * * *",  # runs every hour
)