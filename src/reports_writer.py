# Import Libraries
from pathlib import Path
import logging
import polars as pl

# Initialization
logger = logging.getLogger(__name__)

def generate_event_reports(event_records: list[dict], output_dir: Path, run_timestamp: str):
    """
    Generates a log of significant events and a summary report.

    Args:
        event_records (list[dict]): The event-only log from the engine.
        output_dir (Path): The directory path for the report files.
        run_timestamp (str): A timestamp string for unique filenames.
    """
    if not event_records:
        logger.warning("No event records were generated. Skipping event report creation.")
        return
    schema = {
        "timestamp": pl.Datetime,
        "sensor_id": pl.String,
        "event": pl.String,
        "details": pl.String,
        "reasons": pl.String,
        "dilution_cycle": pl.Int64
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Generating event log report...")
    events_df = pl.DataFrame(event_records, schema_overrides=schema)
    event_log_path = output_dir / f"event_log_{run_timestamp}.csv"
    events_df.write_csv(event_log_path)
    logger.info(f"Event log saved to: {event_log_path}")
    logger.info("Generating event summary report...")
    summary_df = events_df.group_by(["sensor_id", "event"]).len()
    summary_report_path = output_dir / f"summary_report_{run_timestamp}.csv"
    summary_df.write_csv(summary_report_path)
    logger.info(f"Summary report saved to: {summary_report_path}")

def generate_detailed_simulation_log(detailed_records: list[dict], output_dir: Path, run_timestamp: str):
    """
    Generates a detailed, timestamp-by-timestamp log of the entire simulation.

    Args:
        detailed_records (list[dict]): The detailed simulation log from the engine.
        output_dir (Path): The directory path for the report file.
        run_timestamp (str): A timestamp string for unique filenames.
    """
    if not detailed_records:
        logger.warning("No detailed simulation records were generated. Skipping detailed log creation.")
        return
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Generating detailed per-timestamp simulation log...")
    detailed_df = pl.DataFrame(detailed_records)
    detailed_log_path = output_dir / f"detailed_simulation_log_{run_timestamp}.csv"
    detailed_df.write_csv(detailed_log_path)
    logger.info(f"Detailed simulation log saved to: {detailed_log_path}")