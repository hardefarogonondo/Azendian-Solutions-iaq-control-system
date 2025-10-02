# Import Libraries
from pathlib import Path
from src.config import load_config
import pytest
import yaml

def test_load_config_success(mock_config_path):
    """Tests that the config file loads successfully and contains expected data."""
    config = load_config(mock_config_path)
    assert isinstance(config, dict)
    assert "api_urls" in config
    assert config["parameters"]["outdoor_co2_ppm"] == 415

def test_load_config_file_not_found():
    """Tests that a FileNotFoundError is raised if the config file doesn't exist."""
    with pytest.raises(FileNotFoundError):
        load_config(Path("non_existent_config.yaml"))

def test_load_config_malformed_yaml(tmp_path):
    """Tests that a YAMLError is raised if the config file is not valid YAML."""
    malformed_file = tmp_path / "malformed.yaml"
    malformed_file.write_text("key: value: another_value")
    with pytest.raises(yaml.YAMLError):
        load_config(malformed_file)