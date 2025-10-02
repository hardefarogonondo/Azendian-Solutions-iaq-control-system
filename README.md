# Azendian Solutions IAQ Control System

A configuration-driven, rule-based engine that simulates an Indoor Air Quality (IAQ) control system based on a detailed business logic flowchart.

## Table of Contents

1. [Project Description](#project-description)
2. [Project Architecture](#project-architecture)
3. [Installation Guide](#installation-guide)
4. [How to Run](#how-to-run)
5. [Conclusions](#conclusions)
6. [Future Works](#future-works)

## Project Description

This project is a Python-based simulation of an Indoor Air Quality (IAQ) control system. The primary objective is to implement a complex, stateful set of business rules provided in a flowchart. The system processes time-series sensor data (e.g., CO2, TVOC, temperature, humidity) and HVAC system data to monitor air quality, detect anomalies, and simulate control actions.

The core of the application is a logic engine that operates on a per-timestamp basis. It identifies when IAQ parameters exceed predefined thresholds, manages a persistence check to ensure alerts are not transient, and executes multi-step "dilution cycles" or "comfort control" actions based on the nature of the alert. A key feature of this system is its configuration-driven design; all operational parameters, thresholds, and even data filenames are managed in an external `config.yaml` file, ensuring no hardcoded values exist in the application logic. The final output is a set of detailed, timestamped CSV reports that log every event and action taken during the simulation.

## Project Architecture

The project is organized into a modular structure to separate concerns, making the codebase clean, maintainable, and testable. The primary directories include `src` for the application code, `data` for input files, `tests` for the unit test suite, and `reports` for the generated output.

```bash
iaq-control-system/
├── data/
│   └── raw/
│       ├── n_pdata_2.csv
│       ├── n_pdata_3.csv
│       ├── n_pdata_6_ahu.csv
│       └── n_pdata_8_vav.csv
├── references/
│   ├── AI_Engineer_Technical_Test.pdf
│   ├── Flowchart.pdf
│   └── FlowChart_Scenario_Matrix.xlsb
├── reports/
│   ├── detailed_simulation_log_...csv
│   └── summary_report_...csv
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── data_ingestion.py
│   ├── logic_engine.py
│   └── reports_writer.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_config.py
│   ├── test_data_ingestion.py
│   ├── test_logic_engine.py
│   └── test_reports_writer.py
├── .gitignore
├── config.yaml
├── main.py
├── README.md
└── requirements.txt
```

## Installation Guide

Follow these steps to set up and run the project locally.

### Prerequisites

- Python 3.10+
- `pip` and `conda` installed

### Setup

#### 1. Clone the repository:

```bash
git clone https://github.com/hardefarogonondo/Azendian-Solutions-iaq-control-system
cd Azendian-Solutions-iaq-control-system
```

#### 2. Create and activate a `conda` virtual environment:

```bash
conda create -n azendian python=3.13 -y
conda activate azendian
```

#### 3. Install the required dependencies:

```bash
pip install -r requirements.txt
```

#### 4. Place data files

Ensure your input `.csv` or `.parquet` data files are located in the `data/raw/` directory. The filenames should match those specified in `config.yaml`.

## How to Run

There are two main ways to execute the code: running the full simulation and running the test suite.

### 1. Run the Full Simulation

To process the data in `data/raw/`, run the entire simulation, and generate the output reports, execute the `main.py` script from the project root:

```bash
python main.py
```

Upon completion, the script will create two new CSV files with timestamped names inside the `reports/` directory:

- `detailed_simulation_log_[timestamp].csv`: A per-event log of every action taken by the engine.
- `summary_report_[timestamp].csv`: A summary of events grouped by sensor.

### 2. Run the Unit Tests

To verify the correctness of all modules and the core logic against predefined scenarios, run the automated test suite using `pytest`:

```bash
pytest
```

A passing test suite (42 passed) confirms that all components are functioning as expected.

## Conclusions

- The complex, stateful logic from the provided flowchart was successfully implemented in a modular and object-oriented `IAQLogicEngine`.
- The system is 100% configuration-driven. All thresholds, parameters, filenames, and mappings are externalized in `config.yaml`, making the application flexible and easy to maintain.
- A robust data ingestion pipeline was built using Polars to efficiently handle and transform wide raw data into a clean, tidy format suitable for analysis. The pipeline is flexible and can read either `.csv` or `.parquet` files.
- The system correctly integrates with an external API to fetch historical PSI data relevant to the simulation's timeframe.
- A comprehensive unit test suite was developed using `pytest`, covering all modules (`config`, `data_ingestion`, `logic_engine`, `reports_writer`) and key logic paths with mocked data and network calls.
- The final deliverables include timestamped, detailed CSV logs and summary reports, meeting the project's output requirements.

## Future Works

- **Real-Time API**: The current batch processing script could be refactored into a real-time API using a framework like FastAPI. This would allow the engine to process single data points or small windows of data on demand.
- **ML Model Integration**: The flowchart references placeholder ML models (e.g., for occupancy detection). A future version could implement and integrate real machine learning models to provide dynamic inputs to the logic engine.
- **Database Integration**: For a production system, data ingestion could be modified to read from a time-series database (like InfluxDB or TimescaleDB) instead of local files.
- **Enhanced Actions**: The current control actions are simulated via logging. Future work could involve integrating with a real or mock Building Management System (BMS) API to send actual commands.
- **Containerization**: The application could be containerized using Docker to ensure a consistent runtime environment and simplify deployment.