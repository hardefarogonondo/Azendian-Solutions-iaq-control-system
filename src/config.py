# Import Libraries
from pathlib import Path
import yaml

# Initialization
CONFIG_PATH = Path(__file__).parent.parent / 'config.yaml'

def load_config(config_path: Path = CONFIG_PATH) -> dict:
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    return config