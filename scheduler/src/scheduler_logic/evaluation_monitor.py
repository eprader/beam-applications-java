import metrics.metrics_collector
import scheduler_logic.scheduler_logic
import database.database_access
import threading
import time
import logging
import utils.Utils
from datetime import datetime


class EvaluationMonitor:
    def __init__(
        self,
        running_framework: utils.Utils.Framework,
        evaluation_event: threading.Event,
        application: str,
        dataset: str,
        threshold_dict_sf: dict,
        threshold_dict_sl: dict,
        periodic_checking_min=1,
        timeout_duration_min=10,
        sleep_interval_seconds=30,
    ) -> None:
        self.interval_seconds = periodic_checking_min * 60
        self.application = application
        self.dataset = dataset
        self.running_framework = running_framework
        self.evaluation_event = evaluation_event
        self.timeout_duration_sec = timeout_duration_min * 60
        self.sleep_interval = sleep_interval_seconds
        self.timeout_counter = 0
        self.periodic_counter = 0
        self.threshold_dict_sf = threshold_dict_sf
        self.threshold_dict_sl = threshold_dict_sl

    def start_monitoring(self):
        periodic_checks = self.interval_seconds / self.sleep_interval
        while True:
            self.monitor_iteration(periodic_checks)
            time.sleep(self.sleep_interval)

    def monitor_iteration(self, periodic_checks):
        timeout_counter = self.timeout_counter
        periodic_counter = self.periodic_counter
        metrics = self.collect_metrics()
        database.database_access.insert_scheduler_metrics(
            datetime.now(), metrics[0], self.running_framework.name
        )
        if self.check_for_safety_net(metrics[1]) and timeout_counter == 0:
            if self.evaluate_and_act():
                timeout_counter = self.timeout_duration_sec / self.sleep_interval
        elif periodic_counter >= periodic_checks and timeout_counter == 0:
            logging.info("Periodical check-up")
            if self.evaluate_and_act():
                timeout_counter = self.timeout_duration_sec / self.sleep_interval
                periodic_counter = 0
            else:
                periodic_counter = -1

        if timeout_counter != 0:
            timeout_counter -= 1
        else:
            periodic_counter += 1

        self.timeout_counter = timeout_counter
        self.periodic_counter = periodic_counter

    def check_for_safety_net(self, metrics_dic: dict):
        try:
            if self.running_framework is utils.Utils.Framework.SF:
                for metric, value in metrics_dic.items():
                    if metric == "idleTime":
                        if value > self.threshold_dict_sf[metric]:
                            return True
                    if metric == "busyTime":
                        if value < self.threshold_dict_sf[metric]:
                            return True
            if self.running_framework is utils.Utils.Framework.SL:
                for metric, value in metrics_dic.items():
                    if value > self.threshold_dict_sl[metric]:
                        return True
        except Exception as e:
            logging.error(f"Error when accessing safety net: {e}")
            return False
        return False

    def collect_metrics(self):
        try:
            if self.running_framework is utils.Utils.Framework.SF:
                return (
                    metrics.metrics_collector.get_objectives_for_sf(self.application),
                    metrics.metrics_collector.get_critical_metrics_for_sf(
                        self.application
                    ),
                )
            elif self.running_framework is utils.Utils.Framework.SL:
                return (
                    metrics.metrics_collector.get_objectives_for_sl(self.application),
                    metrics.metrics_collector.get_critical_metrics_for_sl(),
                )
            raise Exception("No valid Framework is given")
        except Exception as e:
            logging.error(f"Failed to collect metrics: {e}")
            return None, None

    def evaluate_and_act(self):
        decision = scheduler_logic.scheduler_logic.run_evaluation(
            self.running_framework
        )
        if decision != self.running_framework:
            self.handle_switch(decision)
            return True
        else:
            # database.database_access.store_decision_in_db(datetime.now(),self.running_framework)
            return False

    def handle_switch(self, decision: utils.Utils.Framework):
        self.evaluation_event.set()
        database.database_access.store_decision_in_db(datetime.now(), decision)
        while self.evaluation_event.is_set():
            time.sleep(30)
            logging.info("Waiting for event to unset")
        self.running_framework = decision
