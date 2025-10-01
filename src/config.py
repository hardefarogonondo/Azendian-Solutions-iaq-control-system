# Import Libraries
from pathlib import Path
import yaml

# Define a constant path to the configuration file.
# `__file__` is the current file, `.parent.parent` goes up two directories to the project root.
CONFIG_PATH = Path(__file__).parent.parent / 'config.yaml'

def load_config(config_path: Path = CONFIG_PATH) -> dict:
    """
    Loads, parses, and returns the YAML configuration file.

    Args:
        config_path (Path): The path to the YAML configuration file.

    Returns:
        dict: The configuration loaded as a Python dictionary.
    """
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config