# Import Libraries
from datetime import datetime
from src.data_ingestion import _read_data_file, fetch_psi_data, load_and_process_data
import polars as pl
import pytest

def test_read_data_file_prefers_parquet(tmp_path):
    """Tests that the helper function correctly reads .parquet if both file types exist."""
    base_path = tmp_path / "my_data"
    parquet_path = base_path.with_suffix(".parquet")
    csv_path = base_path.with_suffix(".csv")
    df_parquet = pl.DataFrame({"a": [1], "b": [2]})
    df_csv = pl.DataFrame({"a": [3], "b": [4]})
    df_parquet.write_parquet(parquet_path)
    df_csv.write_csv(csv_path)
    result = _read_data_file(base_path)
    assert result.equals(df_parquet)

def test_read_data_file_falls_back_to_csv(tmp_path):
    """Tests that the helper falls back to .csv if .parquet does not exist."""
    base_path = tmp_path / "my_data"
    csv_path = base_path.with_suffix(".csv")
    df_csv = pl.DataFrame({"a": [3], "b": [4]})
    df_csv.write_csv(csv_path)
    result = _read_data_file(base_path)
    assert result.equals(df_csv)

def test_read_data_file_exits_if_no_file(tmp_path, monkeypatch):
    """Tests that the program exits if neither file type is found."""
    with pytest.raises(SystemExit) as error:
        _read_data_file(tmp_path / "non_existent")
    assert error.type == SystemExit
    assert error.value.code == 1

@pytest.fixture
def mock_requests_get(monkeypatch):
    """
    A fixture that mocks the `requests.get` function to simulate API calls.
    This prevents tests from making real network requests, making them fast and reliable.
    """
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code
        def json(self):
            return self.json_data

    def mock_get(url, params=None):
        if url == "http://fake-psi-api.com/psi":
            if params and params.get("date") == "2025-10-01":
                return MockResponse({
                    "data": {"items": [{"readings": {"psi_twenty_four_hourly": {"central": 150}}}]}
                }, 200)
            else: # Latest data
                return MockResponse({
                    "data": {"items": [{"readings": {"psi_twenty_four_hourly": {"central": 50}}}]}
                }, 200)
        return MockResponse({"errorMsg": "URL not found in mock"}, 404)
    monkeypatch.setattr("requests.get", mock_get)

def test_fetch_psi_data_latest_success(mock_requests_get, base_config, monkeypatch):
    """Tests successfully fetching the 'latest' PSI data."""
    monkeypatch.setattr("src.data_ingestion.load_config", lambda: base_config)
    df = fetch_psi_data()
    assert not df.is_empty()
    assert df.filter(pl.col("metric") == "psi_twenty_four_hourly").select("central").item() == 50

def test_fetch_psi_data_specific_date_success(mock_requests_get, base_config, monkeypatch):
    """Tests successfully fetching PSI data for a specific date."""
    monkeypatch.setattr("src.data_ingestion.load_config", lambda: base_config)
    df = fetch_psi_data(date=datetime(2025, 10, 1))
    assert not df.is_empty()
    assert df.filter(pl.col("metric") == "psi_twenty_four_hourly").select("central").item() == 150

def test_fetch_psi_data_api_error(monkeypatch, base_config):
    """Tests that the function handles API errors (e.g., 400 status) gracefully."""
    class MockResponse:
        status_code = 400
        def json(self):
            return {"errorMsg": "Invalid date format"}
    monkeypatch.setattr("requests.get", lambda url, params: MockResponse())
    monkeypatch.setattr("src.data_ingestion.load_config", lambda: base_config)
    df = fetch_psi_data()
    assert df.is_empty()

def test_fetch_psi_data_no_readings(monkeypatch, base_config):
    """Tests that the function handles a successful response with no data."""
    class MockResponse:
        status_code = 200
        def json(self):
            return {"data": {"items": [{"readings": {}}]}}
    monkeypatch.setattr("requests.get", lambda url, params: MockResponse())
    monkeypatch.setattr("src.data_ingestion.load_config", lambda: base_config)
    df = fetch_psi_data()
    assert df.is_empty()

def test_load_and_process_data_success(tmp_path, base_config):
    """
    Tests the entire data loading and transformation pipeline with mock CSV files.
    """
    data_dir = tmp_path / "data" / "raw"
    data_dir.mkdir(parents=True)
    common_cols = {"datetime": ["2025-10-01 10:00:00.000"], "epoch": [123], "year": [2025], "month": [10], "day": [1]}
    pl.DataFrame({**common_cols, "idp_iaq_l19_047_co2": [800]}).write_csv(data_dir / "sensor_data_1.csv")
    pl.DataFrame({**common_cols, "idp_iaq_l19_047_tvoc": [600]}).write_csv(data_dir / "sensor_data_2.csv")
    pl.DataFrame({**common_cols, "sne22_2_vav_l19_z1_sa_vav_01_supflosp": [500]}).write_csv(data_dir / "vav_data.csv")
    pl.DataFrame({**common_cols, "ahu_pressure": [1.5]}).write_csv(data_dir / "ahu_data.csv")
    processed_data = load_and_process_data(data_dir, base_config)
    assert "iaq" in processed_data
    assert "vav" in processed_data
    assert "ahu" in processed_data
    iaq_df = processed_data["iaq"]
    assert iaq_df.shape == (1, 4) # datetime, sensor_id, co2, tvoc
    assert iaq_df.select("sensor_id").item() == "047"
    assert iaq_df.select("co2").item() == 800
    assert iaq_df["datetime"].dtype == pl.Datetime
    assert processed_data["vav"]["datetime"].dtype == pl.Datetime
    assert processed_data["ahu"]["datetime"].dtype == pl.Datetime