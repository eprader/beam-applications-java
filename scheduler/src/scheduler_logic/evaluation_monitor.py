from metrics.metrics_collector import MetricsCollector
from scheduler_logic.scheduler_logic import run_evaluation
import time


class EvaluationMonitor:
    def __init__(self, running_framework, evaluation_event,periodic_checking_min=5, timeout_duration_min=10) -> None:
        self.interval_seconds = periodic_checking_min * 60
        self.metric_collector = MetricsCollector()
        self.running_framework= running_framework
        self.evaluation_event = evaluation_event
        self.timeout_duration_sec = timeout_duration_min*60 
    
        

    def run_main(self):
        periodic_checks = self.interval_seconds / 30
        timeout_counter = self.timeout_duration_sec /30
        periodic_counter = 1
        while True:
            if self.running_framework =="SF":
                metrics = self.metric_collector.get_objectives_for_sf()
                critical_metrics = self.metric_collector.get_critical_metrics_for_sf()
            else:
                metrics = self.metric_collector.get_objectives_for_sl()
                critical_metrics = self.metric_collector.get_critical_metrics_for_sl()
            # FIXME
            # save metrics to db

            if self.check_for_safety_net():
                decision =run_evaluation()
                if decision !=self.running_framework:
                    self.handle_switch(decision)
                else:
                    self.store_decision_in_db()                
            elif periodic_counter == periodic_checks:
                decision =run_evaluation()
                periodic_counter = 0
                if decision !=self.running_framework:
                    self.handle_switch(decision)
                else:
                    self.store_decision_in_db()
            periodic_counter += 1
            time.sleep(30)

    # FIXME
    def check_for_safety_net(self, critical_metrics):
        pass
        # Return True if there is a safety cause

    #FIXME
    def store_decision_in_db(self, decision):
        pass

    #FIXME
    def handle_switch(self, decision):
        self.evaluation_event.set()
        self.store_decision_in_db(decision)
        while True:
            if not self.evaluation_event.is_set():
                break
            else:
                time.sleep(30)


