# Import Libraries
from datetime import datetime
from src.reports_writer import generate_event_reports, generate_detailed_simulation_log
import polars as pl

def test_generate_event_reports_creates_files(tmp_path):
    """
    Tests that generate_event_reports successfully creates the event log
    and summary CSV files with the correct content.
    """
    output_dir = tmp_path / "reports"
    run_timestamp = "2025-10-01_12-00-00"
    event_records = [
        {"timestamp": datetime(2025, 10, 1, 10, 10), "sensor_id": "047", "event": "Dilution Cycle Started", "details": "...", "reasons": "['tvoc']", "dilution_cycle": 1},
        {"timestamp": datetime(2025, 10, 1, 10, 10), "sensor_id": "048", "event": "Cooling Cycle Started", "details": "...", "reasons": "['temp']", "dilution_cycle": 1},
        {"timestamp": datetime(2025, 10, 1, 10, 11), "sensor_id": "047", "event": "Normalization", "details": "...", "reasons": "[]", "dilution_cycle": 0},
    ]
    generate_event_reports(event_records, output_dir, run_timestamp)
    event_log_path = output_dir / f"event_log_{run_timestamp}.csv"
    summary_path = output_dir / f"summary_report_{run_timestamp}.csv"
    assert event_log_path.exists()
    assert summary_path.exists()
    events_df = pl.read_csv(event_log_path, schema_overrides={"sensor_id": pl.String})
    assert events_df.shape == (3, 6)
    summary_df = pl.read_csv(summary_path, schema_overrides={"sensor_id": pl.String})
    assert summary_df.shape == (3, 3) # 3 unique sensor_id/event pairs
    count_val = summary_df.filter(
        (pl.col("sensor_id") == "047") & (pl.col("event") == "Normalization")
    ).select("len").item()
    assert count_val == 1

def test_generate_event_reports_skips_on_empty(tmp_path, caplog):
    """
    Tests that generate_event_reports does not create files and logs a
    warning when it receives an empty list.
    """
    output_dir = tmp_path / "reports"
    run_timestamp = "2025-10-01_12-00-00"
    generate_event_reports([], output_dir, run_timestamp)
    assert not output_dir.exists()
    assert "No event records were generated. Skipping event report creation." in caplog.text

def test_generate_detailed_log_creates_file(tmp_path):
    """
    Tests that generate_detailed_simulation_log successfully creates the
    detailed timestamp-by-timestamp CSV file.
    """
    output_dir = tmp_path / "reports"
    run_timestamp = "2025-10-01_12-00-00"
    detailed_records = [
        {"timestamp": datetime(2025, 10, 1, 10, 10), "sensor_id": "047", "is_triggered": True, "has_fired": False, "alert_type": "pollutant", "dilution_cycle": 0, "temperature": 24.5, "co2": 850},
        {"timestamp": datetime(2025, 10, 1, 10, 11), "sensor_id": "047", "is_triggered": True, "has_fired": True, "alert_type": "pollutant", "dilution_cycle": 1, "temperature": 24.4, "co2": 800},
        {"timestamp": datetime(2025, 10, 1, 10, 12), "sensor_id": "047", "is_triggered": False, "has_fired": False, "alert_type": None, "dilution_cycle": 0, "temperature": 24.1, "co2": 650},
    ]
    generate_detailed_simulation_log(detailed_records, output_dir, run_timestamp)
    detailed_log_path = output_dir / f"detailed_simulation_log_{run_timestamp}.csv"
    assert detailed_log_path.exists()
    detailed_df = pl.read_csv(detailed_log_path)
    assert detailed_df.shape == (3, 8)
    assert detailed_df.filter(pl.col("dilution_cycle") == 1).select("co2").item() == 800

def test_generate_detailed_log_skips_on_empty(tmp_path, caplog):
    """
    Tests that generate_detailed_simulation_log does not create a file and
    logs a warning when it receives an empty list.
    """
    output_dir = tmp_path / "reports"
    run_timestamp = "2025-10-01_12-00-00"
    generate_detailed_simulation_log([], output_dir, run_timestamp)
    assert not output_dir.exists()
    assert "No detailed simulation records were generated. Skipping detailed log creation." in caplog.text