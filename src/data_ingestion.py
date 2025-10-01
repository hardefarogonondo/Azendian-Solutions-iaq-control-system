# Import Libraries
from datetime import datetime
from pathlib import Path
from src.config import load_config
import logging
import polars as pl
import requests
import sys

# Initialization
logger = logging.getLogger(__name__)
DATETIME_COL = "datetime"
ID_VARS = ["epoch", DATETIME_COL, "year", "month", "day"]

def _read_data_file(base_path: Path) -> pl.DataFrame:
    parquet_path = base_path.with_suffix('.parquet')
    csv_path = base_path.with_suffix('.csv')
    if parquet_path.exists():
        logger.info(f"Reading Parquet file: {parquet_path}")
        return pl.read_parquet(parquet_path)
    elif csv_path.exists():
        logger.info(f"Reading CSV file: {csv_path}")
        cols = pl.read_csv(csv_path, n_rows=0, infer_schema_length=0).columns
        overrides = {col: pl.Float64 for col in cols if col not in ID_VARS}
        return pl.read_csv(csv_path, schema_overrides=overrides)
    else:
        logger.error(f"No .parquet or .csv file found for base path: {base_path}")
        sys.exit(1)

def load_and_process_data(data_dir: Path, config: dict) -> dict[str, pl.DataFrame]:
    logger.info(f"Searching for data files in: {data_dir.resolve()}")
    file_map = config.get("data_files")
    if not file_map:
        logger.error("'data_files' section not found in config.yaml. Cannot proceed.")
        sys.exit(1)
    sensor_df_1 = _read_data_file(data_dir / file_map["sensor_1"])
    sensor_df_2 = _read_data_file(data_dir / file_map["sensor_2"])
    ahu_df = _read_data_file(data_dir / file_map["ahu"])
    vav_df = _read_data_file(data_dir / file_map["vav"])
    logger.info("Reshaping sensor and VAV data into tidy format...")
    combined_sensor_df = sensor_df_1.join(sensor_df_2, on=ID_VARS, how="inner")
    sensor_long = combined_sensor_df.unpivot(index=ID_VARS, variable_name="variable", value_name="value")
    sensor_tidy = sensor_long.with_columns([
        pl.col("variable").str.extract(r"idp_iaq_l19_([^_]+)_(.*)", 1).alias("sensor_id"),
        pl.col("variable").str.extract(r"idp_iaq_l19_([^_]+)_(.*)", 2).alias("metric")
    ]).drop("variable").drop_nulls()
    iaq_df = sensor_tidy.pivot(index=[DATETIME_COL, "sensor_id"], on="metric", values="value")
    vav_long = vav_df.unpivot(index=ID_VARS, variable_name="variable", value_name="value")
    vav_tidy = vav_long.with_columns([
        pl.col("variable").str.extract(r"sne22_2_vav_l19_z1_sa_([^_]+)_(.*)", 1).alias("vav_id"),
        pl.col("variable").str.extract(r"sne22_2_vav_l19_z1_sa_[^_]+_(.*)", 1).alias("metric")
    ]).drop("variable").drop_nulls()
    vav_df_tidy = vav_tidy.pivot(index=[DATETIME_COL, "vav_id"], on="metric", values="value")
    logger.info("Finalizing and converting data types...")
    main_df = ahu_df.with_columns(pl.col(DATETIME_COL).str.to_datetime("%Y-%m-%d %H:%M:%S%.f"))
    iaq_df = iaq_df.with_columns(pl.col(DATETIME_COL).str.to_datetime("%Y-%m-%d %H:%M:%S%.f"))
    vav_df_tidy = vav_df_tidy.with_columns(pl.col(DATETIME_COL).str.to_datetime("%Y-%m-%d %H:%M:%S%.f"))
    logger.info("Data ingestion and processing complete.")
    return {
        "iaq": iaq_df.sort(DATETIME_COL),
        "vav": vav_df_tidy.sort(DATETIME_COL),
        "ahu": main_df.sort(DATETIME_COL)
    }

def fetch_psi_data(date: datetime | None = None) -> pl.DataFrame:
    config = load_config()
    api_config = config.get("api_urls", {})
    psi_url = api_config.get("psi")
    if not psi_url:
        logger.error("PSI API URL not found in configuration.")
        return pl.DataFrame()
    params = {}
    if date:
        params["date"] = date.strftime("%Y-%m-%d")
        logger.info(f"Fetching PSI data for date: {params['date']}")
    else:
        logger.info("Fetching latest PSI data.")
    try:
        response = requests.get(psi_url, params=params)
        if response.status_code != 200:
            error_data = response.json()
            error_msg = error_data.get("errorMsg", "Unknown API error")
            logger.error(f"API returned status {response.status_code}: {error_msg}")
            return pl.DataFrame()
        data = response.json()
        logger.debug(f"Full API Response JSON: {data}")
        items = data.get("data", {}).get("items", [{}])[0]
        readings = items.get("readings", {})
        flat_readings = [{"metric": metric, **values} for metric, values in readings.items()]
        if not flat_readings:
            logger.warning("PSI data could be parsed but contains no readings.")
            return pl.DataFrame()
        psi_df = pl.DataFrame(flat_readings)
        logger.info("PSI data fetched successfully.")
        return psi_df
    except requests.exceptions.RequestException as error:
        logger.error(f"Error during API request: {error}")
        return pl.DataFrame()