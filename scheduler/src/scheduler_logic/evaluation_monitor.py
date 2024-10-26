from metrics.metrics_collector import MetricsCollector
from scheduler_logic.scheduler_logic import run_evaluation
from database.database_access import store_decision_in_db
from main import Framework
from threading import Event
import time
import logging


class EvaluationMonitor:
    def __init__(
        self,
        running_framework:Framework,
        evaluation_event:Event,
        periodic_checking_min=5,
        timeout_duration_min=10,
        sleep_interval_seconds=30,
    ) -> None:
        self.interval_seconds = periodic_checking_min * 60
        self.metric_collector = MetricsCollector()
        self.running_framework = running_framework
        self.evaluation_event = evaluation_event
        self.timeout_duration_sec = timeout_duration_min * 60
        self.sleep_interval = sleep_interval_seconds

    def start_monitoring(self):
        periodic_checks = self.interval_seconds / self.sleep_interval
        timeout_counter = 0
        periodic_counter = 1
        while True:
            metrics = self.collect_metrics()
            # FIXME
            # save metrics to db
            if timeout_counter != 0:
                timeout_counter -= 1

            if self.check_for_safety_net():
                if self.evaluate_and_act():
                    timeout_counter = self.timeout_duration_sec / self.sleep_interval
            elif timeout_counter != 0 and periodic_counter == periodic_checks:
                if self.evaluate_and_act():
                    timeout_counter = self.timeout_duration_sec / self.sleep_interval
                periodic_counter = 0

            periodic_counter += 1
            time.sleep(self.sleep_interval)

    # FIXME
    def check_for_safety_net(self, critical_metrics:dict):
        pass
        # Return True if there is a safety cause

    def collect_metrics(self):
        try:
            if self.running_framework is Framework.SF:
                return (
                    self.metric_collector.get_objectives_for_sf(),
                    self.metric_collector.get_critical_metrics_for_sf(),
                )
            elif self.running_framework is Framework.SL:
                return (
                    self.metric_collector.get_objectives_for_sl(),
                    self.metric_collector.get_critical_metrics_for_sl(),
                )
            raise Exception("No valid Framework is given")
        except Exception as e:
            logging.error(f"Failed to collect metrics: {e}")
            return None, None

    def evaluate_and_act(self):
        decision = run_evaluation()
        if decision != self.running_framework:
            self.handle_switch(decision)
            return True
        else:
            store_decision_in_db(self.running_framework)
            return False

    # FIXME
    def handle_switch(self, decision:Framework):
        self.evaluation_event.set()
        store_decision_in_db(decision)
        while self.evaluation_event.is_set():
            time.sleep(30)
        self.running_framework = decision
