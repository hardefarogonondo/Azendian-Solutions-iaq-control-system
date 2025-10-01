# Import Libraries
from datetime import datetime
from pathlib import Path
from src.config import load_config
from src.data_ingestion import load_and_process_data
from src.logic_engine import IAQLogicEngine
from src.reports_writer import generate_reports
import logging

# Initialization
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting IAQ Control System...")
    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    project_root = Path(__file__).parent
    config_path = project_root / 'config.yaml'
    data_path = project_root / 'data' / 'raw'
    reports_path = project_root / 'reports'
    config = load_config(config_path)
    processed_data = load_and_process_data(data_path)
    engine = IAQLogicEngine(config)
    log_records = engine.run_simulation(processed_data)
    if log_records:
        generate_reports(log_records, reports_path, run_timestamp)
    logger.info("Simulation script finished.")

if __name__ == "__main__":
    main()