# Import Libraries
from datetime import timedelta
from src.data_ingestion import fetch_psi_data
import logging
import polars as pl

# Initialization
logger = logging.getLogger(__name__)

class IAQLogicEngine:
    def __init__(self, config: dict):
        """
        Initializes the logic engine, loads, and validates the configuration.

        Args:
            config (dict): The loaded configuration from config.yaml.
        """
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
        """
        Performs a strict check on the loaded configuration to ensure all required
        keys and sections are present. Fails fast on startup if the config is invalid.
        """
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
            "co2_ppm_above_outdoor", "tvoc_ug_m3", "pm2_5_ug_m3",
            "pm10_ug_m3", "hcho_ug_m3", "rh_percent_max"
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
        """
        A helper method to log an event to the console and simultaneously store it 
        as a structured record for the final CSV report.
        """
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

    def _check_bms_filter_alarms(self, ts: timedelta, ahu_data_for_ts: pl.DataFrame) -> bool:
        """
        Checks for active BMS filter alarms using filter status flags from the AHU data.
        This corresponds to the top-right logic block in the flowchart.

        Args:
            ts (timedelta): The current timestamp of the simulation.
            ahu_data_for_ts (pl.DataFrame): The AHU data filtered for the current timestamp.

        Returns:
            bool: True if an alarm was found and handled, False otherwise.
        """
        if not self.config.get("parameters", {}).get("enable_bms_filter_check", False):
            return False
        if ahu_data_for_ts.is_empty():
            return False
        ahu_row = ahu_data_for_ts.to_dicts()[0]
        primary_filter_status = ahu_row.get("sne22_1_ddc_19_1_ahu_19_1_pri_filt_sts")
        secondary_filter_status = ahu_row.get("sne22_1_ddc_19_1_ahu_19_1_sec_fil_sts")
        if primary_filter_status == 1 or secondary_filter_status == 1:
            details = (
                f"AHU filter clog detected (Primary Status: {primary_filter_status}, "
                f"Secondary Status: {secondary_filter_status}). FM team to inspect."
            )
            self._log_action(ts, sensor_id="SYSTEM_BMS", event="BMS Filter Alarm", details=details)
            return True
        return False

    def _check_iaq_triggers(self, sensor_data: dict) -> list[str]:
        """
        Checks a single sensor's data against all IAQ triggering thresholds from the config.

        Args:
            sensor_data (dict): A dictionary of a single sensor's readings for one timestamp.

        Returns:
            list[str]: A list of reasons for the trigger (e.g., ["co2", "tvoc"]).
        """
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
        """
        Checks if a sensor's pollutant levels have returned to the normal range 
        after a dilution cycle (Branch A).

        Returns:
            bool: True if all pollutant levels are below normalization thresholds.
        """
        norm_thresholds = self.thresholds["normalization"]
        return (
            sensor_data.get("co2", self.sensor_default) < self.outdoor_co2 + norm_thresholds["co2_ppm_above_outdoor"] and
            sensor_data.get("tvoc", self.sensor_default) < norm_thresholds["tvoc_ug_m3"] and
            sensor_data.get("pm2_5", self.sensor_default) < norm_thresholds["pm2_5_ug_m3"] and
            sensor_data.get("pm10", self.sensor_default) < norm_thresholds["pm10_ug_m3"] and
            sensor_data.get("hcho", self.sensor_default) < norm_thresholds["hcho_ug_m3"]
        )

    def _check_for_comfort_normalization(self, sensor_data: dict) -> bool:
        """
        Checks if a sensor's temperature has returned to the normal comfort band 
        (between temp_c_min and temp_c_max).

        Returns:
            bool: True if the temperature is within the normal range.
        """
        temp = sensor_data.get("temperature")
        trigger_thresholds = self.thresholds["triggering"]
        norm_min = trigger_thresholds["temp_c_min"]
        norm_max = trigger_thresholds["temp_c_max"]
        return temp is not None and norm_min <= temp <= norm_max

    def _check_for_dehumid_normalization(self, sensor_data: dict) -> bool:
        """
        Checks if a sensor's humidity and temperature have returned to normal 
        after a dehumidification cycle (Branch D).

        Returns:
            bool: True if both RH and temperature are within normal ranges.
        """
        rh_norm_threshold = self.thresholds["normalization"]["rh_percent_max"]
        rh_normalized = sensor_data.get("humidity", self.sensor_default) < rh_norm_threshold
        return self._check_for_comfort_normalization(sensor_data) and rh_normalized

    def _execute_branch_a(self, ts: timedelta, sensor_id: str, all_data: dict, reasons: list[str]):
        """
        Executes the 'Dilution Mode' logic for pollutant-based alerts (Branch A),
        which involves controlling VAV and PAD/FAD systems.
        """
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

    def _execute_branch_b(self, ts: timedelta, sensor_id: str, all_data: dict, reasons: list[str]):
        """
        Executes the 'Cooling Mode' logic for hot and dry comfort alerts (Branch B).
        """
        current_state = self.sensor_states[sensor_id]
        max_cycles = self.thresholds["triggering"]["max_dilution_cycles"]
        if current_state["dilution_cycle_count"] >= max_cycles:
            self._log_action(ts, sensor_id, "Cooling Failed", f"Max cycles ({max_cycles}) reached", reasons)
            return
        current_state["dilution_cycle_count"] += 1
        cycle = current_state["dilution_cycle_count"]
        vav_id = self.sensor_to_vav_map.get(sensor_id)
        if not vav_id:
            self._log_action(ts, sensor_id, "Branch B Skipped", "No VAV mapping found", reasons, cycle)
            return
        self._log_action(ts, sensor_id, "Cooling Cycle Started", f"Cycle #{cycle} for VAV '{vav_id}'", reasons, cycle)
        vav_data = all_data["vav"].filter((pl.col("datetime") == ts) & (pl.col("vav_id") == vav_id))
        if vav_data.is_empty():
            self._log_action(ts, sensor_id, "Branch B Halted", f"VAV mapping exists for '{vav_id}', but no data found at this timestamp", reasons, cycle)
            return
        vav_max_setpoint = vav_data.select("cmaxflo").item()
        vav_current_setpoint = vav_data.select("supflosp").item()
        if vav_current_setpoint < vav_max_setpoint:
            increase_pct = self.actions_config["branch_b"]["vav_flow_increase_pct"]
            self._log_action(ts, sensor_id, "VAV Action (Cooling)", f"VAV '{vav_id}' not at max. Increasing flow setpoint by {increase_pct}%", reasons, cycle)
        else:
            increase_pct = self.actions_config["branch_b"]["chw_valve_increase_pct"]
            self._log_action(ts, sensor_id, "CHW Valve Action (Cooling)", f"VAV at max. Increasing Chilled Water Valve position by {increase_pct}%", reasons, cycle)

    def _execute_branch_c(self, ts: timedelta, sensor_id: str, all_data: dict, reasons: list[str]):
        """
        Executes the 'Warming Mode' logic for cold and dry comfort alerts (Branch C).
        """
        current_state = self.sensor_states[sensor_id]
        max_cycles = self.thresholds["triggering"]["max_dilution_cycles"]
        if current_state["dilution_cycle_count"] >= max_cycles:
            self._log_action(ts, sensor_id, "Warming Failed", f"Max cycles ({max_cycles}) reached", reasons)
            return
        current_state["dilution_cycle_count"] += 1
        cycle = current_state["dilution_cycle_count"]
        vav_id = self.sensor_to_vav_map.get(sensor_id)
        if not vav_id:
            self._log_action(ts, sensor_id, "Branch C Skipped", "No VAV mapping found", reasons, cycle)
            return
        self._log_action(ts, sensor_id, "Warming Cycle Started", f"Cycle #{cycle} for VAV '{vav_id}'", reasons, cycle)
        vav_data = all_data["vav"].filter((pl.col("datetime") == ts) & (pl.col("vav_id") == vav_id))
        if vav_data.is_empty():
            self._log_action(ts, sensor_id, "Branch C Halted", f"VAV mapping exists for '{vav_id}', but no data found at this timestamp", reasons, cycle)
            return
        vav_min_setpoint = vav_data.select("ocmnc_sp").item()
        vav_current_setpoint = vav_data.select("supflosp").item()
        if vav_current_setpoint > vav_min_setpoint:
            decrease_pct = self.actions_config["branch_c"]["vav_flow_decrease_pct"]
            self._log_action(ts, sensor_id, "VAV Action (Warming)", f"VAV '{vav_id}' not at min. Decreasing flow setpoint by {decrease_pct}%", reasons, cycle)
        else:
            decrease_pct = self.actions_config["branch_c"]["chw_valve_decrease_pct"]
            self._log_action(ts, sensor_id, "CHW Valve Action (Warming)", f"VAV at min. Decreasing Chilled Water Valve position by {decrease_pct}%", reasons, cycle)

    def _execute_branch_d(self, ts: timedelta, sensor_id: str, all_data: dict, reasons: list[str]):
        """
        Executes the 'Dehumidification Mode' logic for high humidity comfort alerts (Branch D).
        """
        current_state = self.sensor_states[sensor_id]
        max_cycles = self.thresholds["triggering"]["max_dilution_cycles"]
        if current_state["dilution_cycle_count"] >= max_cycles:
            self._log_action(ts, sensor_id, "Dehumidification Failed", f"Max cycles ({max_cycles}) reached", reasons)
            return
        current_state["dilution_cycle_count"] += 1
        cycle = current_state["dilution_cycle_count"]
        self._log_action(ts, sensor_id, "Dehumidification Cycle Started", f"Cycle #{cycle}", reasons, cycle)
        increase_pct = self.actions_config["branch_d"]["chw_valve_increase_pct"]
        self._log_action(ts, sensor_id, "CHW Valve Action (Dehumidifying)", f"Increasing Chilled Water Valve position by {increase_pct}%", reasons, cycle)

    def _handle_persistent_alert(self, ts: timedelta, sensor_id: str, sensor_data: dict, reasons: list[str], all_data: dict):
        """
        The main router function. It takes a persistent alert and decides which
        logic branch (A, B, C, or D) to execute based on the trigger reasons.
        """
        pollutant_triggers = {"co2", "tvoc", "pm2_5", "pm10", "hcho"}
        is_pollutant_alert = any(reason in pollutant_triggers for reason in reasons)
        if is_pollutant_alert:
            self.sensor_states[sensor_id]["alert_type"] = "pollutant"
            self._log_action(ts, sensor_id, "Branch Routing", "Pollutant alert. Routing to Branch A.", reasons)
            self._execute_branch_a(ts, sensor_id, all_data, reasons)
        else:
            trigger_thresholds = self.thresholds["triggering"]
            rh_max = trigger_thresholds["rh_percent_max"]
            temp_max = trigger_thresholds["temp_c_max"]
            temp_min = trigger_thresholds["temp_c_min"]
            rh = sensor_data.get("humidity", self.sensor_default)
            temp = sensor_data.get("temperature")
            if rh < rh_max and temp > temp_max:
                self.sensor_states[sensor_id]["alert_type"] = "comfort_hot"
                self._log_action(ts, sensor_id, "Branch Routing", "Comfort alert (Too Hot). Routing to Branch B.", reasons)
                self._execute_branch_b(ts, sensor_id, all_data, reasons)
            elif rh < rh_max and temp < temp_min:
                self.sensor_states[sensor_id]["alert_type"] = "comfort_cold"
                self._log_action(ts, sensor_id, "Branch Routing", "Comfort alert (Too Cold). Routing to Branch C.", reasons)
                self._execute_branch_c(ts, sensor_id, all_data, reasons)
            elif rh >= rh_max:
                self.sensor_states[sensor_id]["alert_type"] = "comfort_humid"
                self._log_action(ts, sensor_id, "Branch Routing", "Comfort alert (Too Humid). Routing to Branch D.", reasons)
                self._execute_branch_d(ts, sensor_id, all_data, reasons)
            else:
                self._log_action(ts, sensor_id, "Conflict Alert", "Ambiguous comfort triggers. Sending alert to FM team", reasons)

    def run_simulation(self, data: dict[str, pl.DataFrame]) -> list[dict]:
        """
        The main entry point for the simulation. It iterates through every timestamp 
        in the dataset and applies the full flowchart logic, including PSI checks,
        BMS filter checks, and sensor-level alert handling.

        Returns:
            list[dict]: A list of all actions and events that occurred during the simulation.
        """
        iaq_df = data["iaq"]
        ahu_df = data["ahu"]
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
            ahu_data_for_ts = ahu_df.filter(pl.col("datetime") == ts)
            if self._check_bms_filter_alarms(ts, ahu_data_for_ts):
                continue
            readings_for_ts = iaq_df.filter(pl.col("datetime") == ts)
            for sensor_row in readings_for_ts.to_dicts():
                sensor_id = sensor_row["sensor_id"]
                if sensor_id not in self.sensor_states:
                    self.sensor_states[sensor_id] = {"is_triggered": False, "alert_start_time": None, "has_fired": False, "dilution_cycle_count": 0, "alert_type": None}
                current_state = self.sensor_states[sensor_id]
                if current_state["is_triggered"]:
                    normalized = False
                    alert_type = current_state["alert_type"]
                    if alert_type == "pollutant":
                        if self._check_for_normalization(sensor_row):
                            self._log_action(ts, sensor_id, "Normalization", "Dilution Successful! Pollutant levels normalized.")
                            normalized = True
                    elif alert_type in ["comfort_hot", "comfort_cold"]:
                         if self._check_for_comfort_normalization(sensor_row):
                            self._log_action(ts, sensor_id, "Normalization", "Comfort Restored! Temperature is normal.")
                            normalized = True
                    elif alert_type == "comfort_humid":
                         if self._check_for_dehumid_normalization(sensor_row):
                            self._log_action(ts, sensor_id, "Normalization", "Dehumidification Successful! RH and Temp are normal.")
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