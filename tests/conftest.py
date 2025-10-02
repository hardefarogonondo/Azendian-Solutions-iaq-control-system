# Import Libraries
from datetime import datetime
import polars as pl
import pytest
import yaml

@pytest.fixture(scope="session")
def project_root(tmp_path_factory):
    """Creates a temporary root directory for the entire test session."""
    return tmp_path_factory.mktemp("project")

@pytest.fixture(scope="session")
def mock_config_path(project_root):
    """
    Creates a fake config.yaml file in a temporary directory and returns its path.
    This allows tests to run with a known, consistent configuration.
    """
    config_data = {
        "data_files": {
            "sensor_1": "sensor_data_1",
            "sensor_2": "sensor_data_2",
            "ahu": "ahu_data",
            "vav": "vav_data"
        },
        "api_urls": {"psi": "http://fake-psi-api.com/psi"},
        "parameters": {
            "outdoor_co2_ppm": 415,
            "enable_bms_filter_check": True
        },
        "defaults": {"sensor_reading_default": 0},
        "thresholds": {
            "triggering": {
                "co2_ppm_above_outdoor": 500, "tvoc_ug_m3": 500,
                "pm2_5_ug_m3": 25, "pm10_ug_m3": 50, "hcho_ug_m3": 100,
                "rh_percent_max": 70, "temp_c_min": 23, "temp_c_max": 25,
                "persistence_minutes": 10, "pad_increase_percent": 5,
                "max_dilution_cycles": 3
            },
            "normalization": {
                "co2_ppm_above_outdoor": 400, "tvoc_ug_m3": 400,
                "pm2_5_ug_m3": 20, "pm10_ug_m3": 40, "hcho_ug_m3": 80,
                "rh_percent_max": 65
            },
            "psi": {
                "unhealthy_min": 101, "unhealthy_max": 200,
                "very_unhealthy_min": 201
            }
        },
        "sensor_to_vav_map": {"047": "vav_01", "048": "vav_02"},
        "actions": {
            "branch_b": {"vav_flow_increase_pct": 10, "chw_valve_increase_pct": 5},
            "branch_c": {"vav_flow_decrease_pct": 10, "chw_valve_decrease_pct": 5},
            "branch_d": {"chw_valve_increase_pct": 10}
        }
    }
    config_path = project_root / "config.yaml"
    with open(config_path, 'w') as file:
        yaml.dump(config_data, file)
    return config_path

@pytest.fixture
def base_config(mock_config_path):
    """
    A fixture that loads the mock config file and returns it as a Python dictionary.
    This is used by any test that needs to access configuration values.
    """
    from src.config import load_config
    return load_config(mock_config_path)

@pytest.fixture
def mock_processed_data():
    """
    Provides a small, clean, and processed sample of data.
    This is useful for testing the logic engine without needing to run the full
    data ingestion pipeline in every test.
    """
    ts = [datetime(2025, 10, 1, 10, 0), datetime(2025, 10, 1, 10, 1)]
    return {
        "iaq": pl.DataFrame({
            "datetime": ts,
            "sensor_id": ["047", "047"],
            "co2": [1000, 450], "tvoc": [600, 300], "pm2_5": [30, 15],
            "pm10": [60, 25], "hcho": [110, 50], "humidity": [75, 60],
            "temperature": [28, 24]
        }),
        "vav": pl.DataFrame({
            "datetime": ts,
            "vav_id": ["vav_01", "vav_01"],
            "cmaxflo": [1000, 1000], "supflosp": [500, 500],
            "ocmnc_sp": [200, 200]
        }),
        "ahu": pl.DataFrame({
            "datetime": ts,
            "sne22_1_ddc_19_1_ahu_19_1_fad_fb": [80, 80],
            "sne22_1_ddc_19_1_ahu_19_1_fad_max_stpt": [100, 100],
            "sne22_1_ddc_19_1_ahu_19_1_pri_filt_sts": [0, 0],
            "sne22_1_ddc_19_1_ahu_19_1_sec_fil_sts": [0, 0]
        })
    }