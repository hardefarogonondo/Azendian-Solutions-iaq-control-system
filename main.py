# Import Libraries
from datetime import datetime
from pathlib import Path
from src.config import load_config
from src.data_ingestion import load_and_process_data
from src.logic_engine import IAQLogicEngine
from src.reports_writer import generate_reports
import logging

# --- 1. Configure Logging ---
# Set up global logging configuration for the entire application.
# Messages will be shown from the DEBUG level and up.
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    """Orchestrates the IAQ control logic simulation from start to finish."""
    logger.info("Starting IAQ Control System...")
    # --- 2. Setup Phase ---
    # Generate a unique timestamp for this simulation run to use in filenames.
    run_timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    # Define key file and directory paths relative to this script's location.
    project_root = Path(__file__).parent
    config_path = project_root / 'config.yaml'
    data_path = project_root / 'data' / 'raw'
    reports_path = project_root / 'reports'
    # --- 3. Data Loading and Processing Phase ---
    config = load_config(config_path)
    processed_data = load_and_process_data(data_path, config)
    # --- 4. Simulation Phase ---
    # Initialize the engine with the rules from the config file.
    engine = IAQLogicEngine(config)
    # Run the simulation using the clean data and get the results.
    log_records = engine.run_simulation(processed_data)
    # --- 5. Reporting Phase ---
    # If the simulation produced any results, write them to CSV files.
    if log_records:
        generate_reports(log_records, reports_path, run_timestamp)
    logger.info("Simulation script finished.")

if __name__ == "__main__":
    main()