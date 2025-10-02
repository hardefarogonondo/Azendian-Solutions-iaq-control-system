# Import Libraries
from datetime import datetime
from src.logic_engine import IAQLogicEngine
import logging
import polars as pl
import pytest

def test_engine_initialization_success(base_config):
    """Tests that the engine initializes correctly with a valid config."""
    engine = IAQLogicEngine(base_config)
    assert engine.outdoor_co2 == 415
    assert "triggering" in engine.thresholds

@pytest.mark.parametrize("missing_key", [
    "data_files", "thresholds", "actions", "sensor_to_vav_map"
])
def test_engine_init_fails_on_missing_section(base_config, missing_key):
    """
    Uses @parametrize to test that the engine fails to initialize if any
    major section is missing from the config file.
    """
    del base_config[missing_key]
    with pytest.raises(ValueError, match=f"Section '{missing_key}' is missing"):
        IAQLogicEngine(base_config)

def test_engine_init_fails_on_missing_trigger_key(base_config):
    """Tests that the engine fails if a specific required key is missing."""
    del base_config["thresholds"]["triggering"]["persistence_minutes"]
    with pytest.raises(ValueError, match="Trigger threshold 'persistence_minutes' is missing"):
        IAQLogicEngine(base_config)

@pytest.mark.parametrize("sensor_data, expected_reasons", [
    ({"co2": 1000, "tvoc": 100}, ["co2"]),
    ({"tvoc": 600}, ["tvoc"]),
    ({"pm2_5": 30}, ["pm2_5"]),
    ({"pm10": 60}, ["pm10"]),
    ({"hcho": 110}, ["hcho"]),
    ({"humidity": 75}, ["rh"]),
    ({"temperature": 22}, ["temp"]),
    ({"temperature": 26}, ["temp"]),
    ({"co2": 1000, "temperature": 22}, ["co2", "temp"]),
    ({"co2": 400, "tvoc": 100, "temperature": 24}, []),
])
def test_check_iaq_triggers(base_config, sensor_data, expected_reasons):
    """
    Uses @parametrize to run multiple small unit tests on the _check_iaq_triggers
    method, checking various combinations of sensor data.
    """
    engine = IAQLogicEngine(base_config)
    reasons = engine._check_iaq_triggers(sensor_data)
    assert sorted(reasons) == sorted(expected_reasons)

def test_check_for_normalization(base_config):
    """Unit tests the pollutant normalization check."""
    engine = IAQLogicEngine(base_config)
    normalized_data = {"co2": 800, "tvoc": 300, "pm2_5": 15, "pm10": 30, "hcho": 50}
    unnormalized_data = {"co2": 950, "tvoc": 300} # co2 is still too high
    assert engine._check_for_normalization(normalized_data) is True
    assert engine._check_for_normalization(unnormalized_data) is False

def test_check_bms_filter_alarms(base_config):
    """Unit tests the BMS filter alarm check."""
    engine = IAQLogicEngine(base_config)
    ts = datetime.now()
    alarm_df = pl.DataFrame({"sne22_1_ddc_19_1_ahu_19_1_pri_filt_sts": [1], "sne22_1_ddc_19_1_ahu_19_1_sec_fil_sts": [0]})
    no_alarm_df = pl.DataFrame({"sne22_1_ddc_19_1_ahu_19_1_pri_filt_sts": [0], "sne22_1_ddc_19_1_ahu_19_1_sec_fil_sts": [0]})
    assert engine._check_bms_filter_alarms(ts, alarm_df) is True
    assert "BMS Filter Alarm" in engine.log_records[-1]["event"]
    assert engine._check_bms_filter_alarms(ts, no_alarm_df) is False

def test_execute_branch_a_vav_not_at_max(base_config, mock_processed_data):
    """Tests the first action of Branch A (increasing VAV)."""
    engine = IAQLogicEngine(base_config)
    engine.sensor_states["047"] = {"dilution_cycle_count": 0}
    ts = mock_processed_data["iaq"]["datetime"][0]
    engine._execute_branch_a(ts, "047", mock_processed_data, ["tvoc"])
    log = engine.log_records[-1]
    assert log["event"] == "VAV Action"
    assert "airflow not at max" in log["details"]

def test_execute_branch_a_pad_not_at_max(base_config, mock_processed_data):
    """Tests the second action of Branch A (increasing PAD) if VAV is already at max."""
    engine = IAQLogicEngine(base_config)
    engine.sensor_states["047"] = {"dilution_cycle_count": 0}
    ts = mock_processed_data["iaq"]["datetime"][0]
    # Modify VAV data to be at max
    mock_processed_data["vav"] = mock_processed_data["vav"].with_columns(pl.col("supflosp").map_elements(lambda x: 1000))
    engine._execute_branch_a(ts, "047", mock_processed_data, ["tvoc"])
    log = engine.log_records[-1]
    assert log["event"] == "PAD Action"
    assert "Increasing opening by 5%" in log["details"]

def test_execute_branch_b_cooling(base_config, mock_processed_data):
    """Tests the action of Branch B (Cooling)."""
    engine = IAQLogicEngine(base_config)
    engine.sensor_states["047"] = {"dilution_cycle_count": 0}
    ts = mock_processed_data["iaq"]["datetime"][0]
    engine._execute_branch_b(ts, "047", mock_processed_data, ["temp"])
    log = engine.log_records[-1]
    assert log["event"] == "VAV Action (Cooling)"
    assert "Increasing flow setpoint by 10%" in log["details"]

def test_execute_branch_c_warming(base_config, mock_processed_data):
    """Tests the action of Branch C (Warming)."""
    engine = IAQLogicEngine(base_config)
    engine.sensor_states["047"] = {"dilution_cycle_count": 0}
    ts = mock_processed_data["iaq"]["datetime"][0]
    engine._execute_branch_c(ts, "047", mock_processed_data, ["temp"])
    log = engine.log_records[-1]
    assert log["event"] == "VAV Action (Warming)"
    assert "Decreasing flow setpoint by 10%" in log["details"]

def test_execute_branch_d_dehumid(base_config, mock_processed_data):
    """Tests the action of Branch D (Dehumid)."""
    engine = IAQLogicEngine(base_config)
    engine.sensor_states["047"] = {"dilution_cycle_count": 0}
    ts = mock_processed_data["iaq"]["datetime"][0]
    engine._execute_branch_d(ts, "047", mock_processed_data, ["rh"])
    log = engine.log_records[-1]
    assert log["event"] == "CHW Valve Action (Dehumidifying)"
    assert "Increasing Chilled Water Valve position by 10%" in log["details"]

@pytest.mark.parametrize("reasons, temp, rh, expected_branch_method", [
    (["co2", "tvoc"], 24, 60, "_execute_branch_a"), # Pollutant
    (["temp"], 28, 60, "_execute_branch_b"),        # Hot
    (["temp"], 22, 60, "_execute_branch_c"),        # Cold
    (["rh"], 24, 75, "_execute_branch_d"),          # Humid
])
def test_handle_persistent_alert_routing(base_config, mock_processed_data, mocker, reasons, temp, rh, expected_branch_method):
    """
    Tests the main router function (_handle_persistent_alert) to ensure it calls
    the correct branch method based on the trigger reasons. It uses `mocker` to "spy"
    on the branch methods and confirm they were called.
    """
    mocker.patch(f"src.logic_engine.IAQLogicEngine.{expected_branch_method}")
    engine = IAQLogicEngine(base_config)
    ts = datetime.now()
    sensor_id = "047"
    engine.sensor_states[sensor_id] = {}
    sensor_data = {"temperature": temp, "humidity": rh}
    engine._handle_persistent_alert(ts, sensor_id, sensor_data, reasons, mock_processed_data)
    spy = getattr(engine, expected_branch_method)
    spy.assert_called_once()
    assert f"Routing to Branch {expected_branch_method[-1].upper()}" in engine.log_records[-1]["details"]

def test_run_simulation_full_cycle(base_config, monkeypatch):
    """
    An integration test for a complete alert cycle: trigger, persistence,
    action (Branch A), and finally normalization.
    """
    monkeypatch.setattr("src.logic_engine.fetch_psi_data", lambda date=None: pl.DataFrame())
    engine = IAQLogicEngine(base_config)
    persistence_min = base_config["thresholds"]["triggering"]["persistence_minutes"]
    timestamps = [datetime(2025, 1, 1, 12, i) for i in range(persistence_min + 2)]
    tvoc_readings = [600] * (persistence_min + 1) + [300]
    mock_data = {
        "iaq": pl.DataFrame({"datetime": timestamps, "sensor_id": ["047"]*len(timestamps), "tvoc": tvoc_readings}),
        "vav": pl.DataFrame({"datetime": timestamps, "vav_id": ["vav_01"]*len(timestamps), "cmaxflo": [1000]*len(timestamps), "supflosp": [500]*len(timestamps)}),
        "ahu": pl.DataFrame({"datetime": timestamps})
    }
    event_logs, _ = engine.run_simulation(mock_data)
    log_events = [log["event"] for log in event_logs]
    assert "Branch Routing" in log_events
    assert "VAV Action" in log_events
    assert "Normalization" in log_events
    assert not engine.sensor_states["047"]["is_triggered"]

def test_run_simulation_generates_correct_detailed_log(base_config, monkeypatch):
    """
    Tests the detailed_log output from run_simulation to ensure it correctly
    captures the minute-by-minute state changes of a sensor.
    """
    monkeypatch.setattr("src.logic_engine.fetch_psi_data", lambda date=None: pl.DataFrame())
    engine = IAQLogicEngine(base_config)
    persistence_min = base_config["thresholds"]["triggering"]["persistence_minutes"] # This is 2 mins
    # We'll simulate 4 minutes: Trigger -> Persist -> Action -> Normalize
    timestamps = [datetime(2025, 1, 1, 12, i) for i in range(persistence_min + 2)]
    # TVOC is high for 3 mins, then normalizes in the 4th min
    tvoc_readings = [600] * (persistence_min + 1) + [300]
    mock_data = {
        "iaq": pl.DataFrame({"datetime": timestamps, "sensor_id": ["047"]*len(timestamps), "tvoc": tvoc_readings}),
        "vav": pl.DataFrame({"datetime": timestamps, "vav_id": ["vav_01"]*len(timestamps), "cmaxflo": [1000]*len(timestamps), "supflosp": [500]*len(timestamps)}),
        "ahu": pl.DataFrame({"datetime": timestamps})
    }
    _, detailed_log = engine.run_simulation(mock_data)
    detailed_df = pl.DataFrame(detailed_log)
    # Check that the log has an entry for every timestamp
    assert detailed_df.shape[0] == len(timestamps)
    # Check the state at the initial trigger (minute 0)
    # is_triggered should be True, but has_fired should be False
    first_state = detailed_df.row(0, named=True)
    assert first_state["is_triggered"] is True
    assert first_state["has_fired"] is False
    assert first_state["alert_type"] == "pollutant"
    # Check the state when the action fires (minute 2, index 2)
    # is_triggered and has_fired should both be True
    action_state = detailed_df.row(persistence_min, named=True)
    assert action_state["is_triggered"] is True
    assert action_state["has_fired"] is True
    assert action_state["dilution_cycle"] == 1
    # Check the state after normalization (final minute)
    # All flags should be reset to their default state
    final_state = detailed_df.row(-1, named=True)
    assert final_state["is_triggered"] is False
    assert final_state["has_fired"] is False
    assert final_state["alert_type"] is None
    assert final_state["dilution_cycle"] == 0

def test_psi_mapping_haze_mode(base_config, caplog, monkeypatch):
    """
    Tests the PSI logic by mocking the API call to return an "Unhealthy" reading.
    """
    caplog.set_level(logging.INFO)
    monkeypatch.setattr("src.logic_engine.fetch_psi_data",
        lambda date=None: pl.DataFrame({"metric": ["psi_twenty_four_hourly"], "central": [150]}))
    engine = IAQLogicEngine(base_config)
    mock_data = {
        "iaq": pl.DataFrame({"datetime": [datetime.now()], "sensor_id": ["psi_test"]}),
        "vav": pl.DataFrame(),
        "ahu": pl.DataFrame({"datetime": [datetime.now()]})
    }
    engine.run_simulation(mock_data)
    assert "PSI is Unhealthy. Haze Mode Protocol triggered" in caplog.text