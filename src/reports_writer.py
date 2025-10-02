# Import Libraries
from datetime import datetime
from pathlib import Path
import logging
import polars as pl

# Initialization
logger = logging.getLogger(__name__)

def generate_reports(log_records: list[dict], output_dir: Path, run_timestamp: str):
    """
    Generates and saves a detailed log and a summary report from simulation records.

    Args:
        log_records (list[dict]): A list of structured log dictionaries from the engine.
        output_dir (Path): The directory path where the report files will be saved.
        run_timestamp (str): A timestamp string to ensure unique filenames for each run.
    """
    if not log_records:
        logger.warning("No log records were generated. Skipping report creation.")
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
    logger.info("Generating detailed per-timestamp log...")
    detailed_df = pl.DataFrame(log_records, schema=schema)
    detailed_log_path = output_dir / f"detailed_simulation_log_{run_timestamp}.csv"
    detailed_df.write_csv(detailed_log_path)
    logger.info(f"Detailed log saved to: {detailed_log_path}")
    logger.info("Generating summary report...")
    summary_df = detailed_df.group_by(["sensor_id", "event"]).len()
    summary_report_path = output_dir / f"summary_report_{run_timestamp}.csv"
    summary_df.write_csv(summary_report_path)
    logger.info(f"Summary report saved to: {summary_report_path}")