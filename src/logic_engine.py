# Import Libraries
from datetime import timedelta
from src.data_ingestion import fetch_psi_data
import logging
import polars as pl

# Initialization
logger = logging.getLogger(__name__)

class IAQLogicEngine:
    def __init__(self, config: dict):
        self.config = config
        self._validate_config()
        self.outdoor_co2 = self.config["parameters"]["outdoor_co2_ppm"]
        self.defaults = self.config["defaults"]
        self.sensor_default = self.defaults["sensor_reading_default"]
        self.thresholds = self.config["thresholds"]
        self.sensor_to_vav_map = self.config["sensor_to_vav_map"]
        self.actions_config = self.config["actions"]
        self.sensor_states = {}
        self.log_records = []
        logger.info("IAQ Logic Engine Initialized.")

    def _validate_config(self):
        required_sections = [
            "data_files", "api_urls", "parameters", "defaults", 
            "thresholds", "sensor_to_vav_map", "actions"
        ]
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Configuration Error: Section '{section}' is missing from config.yaml")
        if "psi" not in self.config["api_urls"]:
            raise ValueError("Configuration Error: 'psi' key is missing from 'api_urls'")
        if "outdoor_co2_ppm" not in self.config["parameters"]:
            raise ValueError("Configuration Error: 'outdoor_co2_ppm' is missing from 'parameters'")
        if "sensor_reading_default" not in self.config["defaults"]:
            raise ValueError("Configuration Error: 'sensor_reading_default' is missing from 'defaults'")
        if "triggering" not in self.config["thresholds"] or "normalization" not in self.config["thresholds"]:
            raise ValueError("Configuration Error: 'triggering' or 'normalization' subsection is missing from 'thresholds'")
        required_triggers = [
            "co2_ppm_above_outdoor", "tvoc_ug_m3", "pm2_5_ug_m3", "pm10_ug_m3",
            "hcho_ug_m3", "rh_percent_max", "temp_c_min", "temp_c_max",
            "persistence_minutes", "pad_increase_percent", "max_dilution_cycles"
        ]
        for key in required_triggers:
            if key not in self.config["thresholds"]["triggering"]:
                raise ValueError(f"Configuration Error: Trigger threshold '{key}' is missing from config.yaml")
        required_norms = [
            "co2_ppm_above_outdoor", "tvoc_ug_m3", "pm2_5_ug_m3", "pm10_ug_m3", "hcho_ug_m3"
        ]
        for key in required_norms:
             if key not in self.config["thresholds"]["normalization"]:
                raise ValueError(f"Configuration Error: Normalization threshold '{key}' is missing from config.yaml")
        if "psi" not in self.config["thresholds"]:
            raise ValueError("Configuration Error: 'psi' subsection is missing from 'thresholds'")
        required_psi = ["unhealthy_min", "unhealthy_max", "very_unhealthy_min"]
        for key in required_psi:
            if key not in self.config["thresholds"]["psi"]:
                raise ValueError(f"Configuration Error: PSI threshold '{key}' is missing from config.yaml")
        if "branch_b" not in self.config["actions"] or "branch_c" not in self.config["actions"]:
            raise ValueError("Configuration Error: 'branch_b' or 'branch_c' is missing from 'actions'")
        logger.info("All required configuration sections and keys are present.")

    def _log_action(self, ts, sensor_id, event, details, reasons="", cycle=0):
        log_message = f"[{ts}] Sensor {sensor_id}: {event}. Details: {details}"
        if event.endswith("Failed"):
            logger.warning(log_message)
        else:
            logger.info(log_message)
        self.log_records.append({
            "timestamp": ts,
            "sensor_id": sensor_id,
            "event": event,
            "details": details,
            "reasons": str(reasons),
            "dilution_cycle": cycle,
        })

    def _check_iaq_triggers(self, sensor_data: dict) -> list[str]:
        reasons = []
        trigger_thresholds = self.thresholds["triggering"]
        if sensor_data.get("co2", self.sensor_default) > self.outdoor_co2 + trigger_thresholds["co2_ppm_above_outdoor"]:
            reasons.append("co2")
        if sensor_data.get("tvoc", self.sensor_default) > trigger_thresholds["tvoc_ug_m3"]:
            reasons.append("tvoc")
        if sensor_data.get("pm2_5", self.sensor_default) > trigger_thresholds["pm2_5_ug_m3"]:
            reasons.append("pm2_5")
        if sensor_data.get("pm10", self.sensor_default) > trigger_thresholds["pm10_ug_m3"]:
            reasons.append("pm10")
        if sensor_data.get("hcho", self.sensor_default) > trigger_thresholds["hcho_ug_m3"]:
            reasons.append("hcho")
        if sensor_data.get("humidity", self.sensor_default) > trigger_thresholds["rh_percent_max"]:
            reasons.append("rh")
        temp = sensor_data.get("temperature")
        if temp is not None and (temp < trigger_thresholds["temp_c_min"] or temp > trigger_thresholds["temp_c_max"]):
            reasons.append("temp")
        return reasons

    def _check_for_normalization(self, sensor_data: dict) -> bool:
        norm_thresholds = self.thresholds["normalization"]
        return (
            sensor_data.get("co2", self.sensor_default) < self.outdoor_co2 + norm_thresholds["co2_ppm_above_outdoor"] and
            sensor_data.get("tvoc", self.sensor_default) < norm_thresholds["tvoc_ug_m3"] and
            sensor_data.get("pm2_5", self.sensor_default) < norm_thresholds["pm2_5_ug_m3"] and
            sensor_data.get("pm10", self.sensor_default) < norm_thresholds["pm10_ug_m3"] and
            sensor_data.get("hcho", self.sensor_default) < norm_thresholds["hcho_ug_m3"]
        )

    def _check_for_comfort_normalization(self, sensor_data: dict) -> bool:
        temp = sensor_data.get("temperature")
        trigger_thresholds = self.thresholds["triggering"]
        norm_min = trigger_thresholds["temp_c_min"]
        norm_max = trigger_thresholds["temp_c_max"]
        return temp is not None and norm_min <= temp <= norm_max

    def _execute_branch_a(self, ts: timedelta, sensor_id: str, all_data: dict, reasons: list[str]):
        current_state = self.sensor_states[sensor_id]
        max_cycles = self.thresholds["triggering"]["max_dilution_cycles"]
        if current_state["dilution_cycle_count"] >= max_cycles:
            self._log_action(ts, sensor_id, "Dilution Failed", f"Max cycles ({max_cycles}) reached", reasons)
            current_state["has_fired"] = True
            return
        current_state["dilution_cycle_count"] += 1
        cycle = current_state["dilution_cycle_count"]
        vav_id = self.sensor_to_vav_map.get(sensor_id)
        if not vav_id:
            self._log_action(ts, sensor_id, "Branch A Skipped", "No VAV mapping found", reasons, cycle)
            return
        self._log_action(ts, sensor_id, "Dilution Cycle Started", f"Cycle #{cycle} for VAV '{vav_id}'", reasons, cycle)
        vav_df = all_data["vav"]
        vav_data = vav_df.filter((pl.col("datetime") == ts) & (pl.col("vav_id") == vav_id))
        if vav_data.is_empty():
            self._log_action(ts, sensor_id, "Branch A Halted", f"VAV mapping exists for '{vav_id}', but no data found at this timestamp", reasons, cycle)
            return
        vav_max_setpoint = vav_data.select("cmaxflo").item()
        vav_current_setpoint = vav_data.select("supflosp").item()
        if vav_current_setpoint < vav_max_setpoint:
            self._log_action(ts, sensor_id, "VAV Action", f"VAV '{vav_id}' airflow not at max. Setting to maximum", reasons, cycle)
        else:
            ahu_df = all_data["ahu"]
            ahu_data = ahu_df.filter(pl.col("datetime") == ts)
            pad_current_fb = ahu_data.select("sne22_1_ddc_19_1_ahu_19_1_fad_fb").item()
            pad_max_stpt = ahu_data.select("sne22_1_ddc_19_1_ahu_19_1_fad_max_stpt").item()
            if pad_current_fb < pad_max_stpt:
                increase_pct = self.thresholds.get("triggering", {}).get("pad_increase_percent", 5)
                self._log_action(ts, sensor_id, "PAD Action", f"VAV at max. PAD/FAD not at max. Increasing opening by {increase_pct}%", reasons, cycle)
            else:
                self._log_action(ts, sensor_id, "Alert", "VAV and PAD/FAD are both at maximum. Sending alert to FM team", reasons, cycle)

    def _handle_persistent_alert(self, ts: timedelta, sensor_id: str, sensor_data: dict, reasons: list[str], all_data: dict):
        pollutant_triggers = {"co2", "tvoc", "pm2_5", "pm10", "hcho"}
        is_pollutant_alert = any(reason in pollutant_triggers for reason in reasons)
        if is_pollutant_alert:
            self.sensor_states[sensor_id]["alert_type"] = "pollutant"
            self._execute_branch_a(ts, sensor_id, all_data, reasons)
        else:
            self.sensor_states[sensor_id]["alert_type"] = "comfort"
            trigger_thresholds = self.thresholds["triggering"]
            rh_max = trigger_thresholds["rh_percent_max"]
            temp_max = trigger_thresholds["temp_c_max"]
            temp_min = trigger_thresholds["temp_c_min"]
            rh = sensor_data.get("humidity", self.sensor_default)
            temp = sensor_data.get("temperature")
            if rh < rh_max and temp > temp_max:
                self._log_action(ts, sensor_id, "Comfort Alert (Branch B)", "Too Hot: Increasing cooling", reasons)
            elif rh < rh_max and temp < temp_min:
                self._log_action(ts, sensor_id, "Comfort Alert (Branch C)", "Too Cold: Decreasing cooling", reasons)
            elif rh > rh_max and temp_min <= temp <= temp_max:
                self._log_action(ts, sensor_id, "Comfort Alert (Branch D)", "Too Humid: Increasing dehumidification", reasons)
            else:
                self._log_action(ts, sensor_id, "Conflict Alert", "High RH/Temp conflict. Sending alert to FM team", reasons)

    def run_simulation(self, data: dict[str, pl.DataFrame]) -> list[dict]:
        iaq_df = data["iaq"]
        timestamps = iaq_df["datetime"].unique().sort()
        persistence_delta = timedelta(minutes=self.thresholds["triggering"]["persistence_minutes"])
        simulation_date = timestamps[0].date() if not timestamps.is_empty() else None
        psi_data = fetch_psi_data(date=simulation_date)
        psi_value_24hr = None
        if not psi_data.is_empty():
            psi_value_24hr = psi_data.filter(
                pl.col("metric") == "psi_twenty_four_hourly"
            ).select("central").item()
        if psi_value_24hr:
            psi_thresholds = self.thresholds["psi"]
            if psi_thresholds["unhealthy_min"] <= psi_value_24hr <= psi_thresholds["unhealthy_max"]:
                 self._log_action(ts="N/A", sensor_id="SYSTEM", event="PSI Alert", details="PSI is Unhealthy. Haze Mode Protocol triggered. Recommending Carbon Filters.")
            elif psi_value_24hr >= psi_thresholds["very_unhealthy_min"]:
                 self._log_action(ts="N/A", sensor_id="SYSTEM", event="PSI Alert", details="PSI is Very Unhealthy/Hazardous. Recommending HEPA Filters.")
        for ts in timestamps:
            readings_for_ts = iaq_df.filter(pl.col("datetime") == ts)
            for sensor_row in readings_for_ts.to_dicts():
                sensor_id = sensor_row["sensor_id"]
                if sensor_id not in self.sensor_states:
                    self.sensor_states[sensor_id] = {"is_triggered": False, "alert_start_time": None, "has_fired": False, "dilution_cycle_count": 0, "alert_type": None}
                current_state = self.sensor_states[sensor_id]
                if current_state["is_triggered"]:
                    normalized = False
                    if current_state["alert_type"] == "pollutant":
                        if self._check_for_normalization(sensor_row):
                            self._log_action(ts, sensor_id, "Normalization", "Dilution Successful! Pollutant levels normalized.")
                            normalized = True
                    elif current_state["alert_type"] == "comfort":
                         if self._check_for_comfort_normalization(sensor_row):
                            self._log_action(ts, sensor_id, "Normalization", "Comfort Restored! Temperature is normal.")
                            normalized = True
                    if normalized:
                        current_state.update({"is_triggered": False, "alert_start_time": None, "has_fired": False, "dilution_cycle_count": 0, "alert_type": None})
                        continue
                trigger_reasons = self._check_iaq_triggers(sensor_row)
                is_currently_triggered = bool(trigger_reasons)
                if is_currently_triggered and not current_state["is_triggered"]:
                    alert_type = "pollutant" if any(r in {"co2", "tvoc", "pm2_5", "pm10", "hcho"} for r in trigger_reasons) else "comfort"
                    current_state.update({"is_triggered": True, "alert_start_time": ts, "has_fired": False, "dilution_cycle_count": 0, "alert_type": alert_type})
                elif is_currently_triggered and current_state["is_triggered"]:
                    duration = ts - current_state["alert_start_time"]
                    if duration >= persistence_delta and not current_state["has_fired"]:
                        self._handle_persistent_alert(ts, sensor_id, sensor_row, trigger_reasons, data)
                        current_state["has_fired"] = True
                elif not is_currently_triggered and current_state["is_triggered"]:
                    current_state.update({"is_triggered": False, "alert_start_time": None, "has_fired": False})
        logger.info("Simulation finished.")
        return self.log_records