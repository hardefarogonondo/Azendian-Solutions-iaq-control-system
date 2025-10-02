# Import Libraries
from datetime import datetime
from src.reports_writer import generate_reports
import polars as pl

def test_generate_reports_creates_files(tmp_path):
    """
    Tests the "happy path" where the function receives records and successfully
    creates the detailed and summary CSV files with the correct content.
    """
    output_dir = tmp_path / "reports"
    run_timestamp = "2025-10-01_12-00-00"
    log_records = [
        {"timestamp": datetime(2025, 10, 1, 10, 10), "sensor_id": "047", "event": "Dilution Cycle Started", "details": "...", "reasons": "['tvoc']", "dilution_cycle": 1},
        {"timestamp": datetime(2025, 10, 1, 10, 10), "sensor_id": "048", "event": "Cooling Cycle Started", "details": "...", "reasons": "['temp']", "dilution_cycle": 1},
        {"timestamp": datetime(2025, 10, 1, 10, 11), "sensor_id": "047", "event": "Normalization", "details": "...", "reasons": "[]", "dilution_cycle": 0},
    ]
    generate_reports(log_records, output_dir, run_timestamp)
    detailed_path = output_dir / f"detailed_simulation_log_{run_timestamp}.csv"
    summary_path = output_dir / f"summary_report_{run_timestamp}.csv"
    assert detailed_path.exists()
    assert summary_path.exists()
    detailed_df = pl.read_csv(detailed_path)
    assert detailed_df.shape == (3, 6)
    summary_df = pl.read_csv(summary_path)
    assert summary_df.shape == (3, 3) # 3 unique sensor_id/event pairs
    # Find the count for the 'Dilution Cycle Started' event for sensor 047
    count_val = summary_df.filter(
        (pl.col("sensor_id") == "047") & (pl.col("event") == "Dilution Cycle Started")
    ).select("count").item()
    assert count_val == 1

def test_generate_reports_skips_on_empty_logs(tmp_path, caplog):
    """
    Tests the edge case where the function receives no log records.
    It asserts that no files are created and a warning is logged.
    """
    output_dir = tmp_path / "reports"
    run_timestamp = "2025-10-01_12-00-00"
    generate_reports([], output_dir, run_timestamp)
    assert not output_dir.exists()
    assert "No log records were generated. Skipping report creation." in caplog.text