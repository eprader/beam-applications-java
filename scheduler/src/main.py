import logging
import os
import signal
import sys
from framework_scheduling.framework_scheduler import FrameworkScheduler
from enum import Enum
import threading

class Framework(Enum):
    SF = ("SF", [])
    SL = ("SL", [])

    def __init__(self, name, critical_metrics):
        self.name = name
        self.critical_metrics = critical_metrics

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


def handle_sigterm(signum, frame):
    logging.info("Received SIGTERM signal. Shutting down scheduler gracefully")
    cleanup(framework_scheduler)
    exit(0)


def cleanup(framework_scheduler: FrameworkScheduler):
    framework_scheduler.cleanup()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)
    is_serverful_framework_used = True
    evaluation_event = threading.Event()
    framework_scheduler = FrameworkScheduler(is_serverful_framework_used)
    try:
        application = os.getenv("APPLICATION")
        mongodb_address = os.getenv("MONGODB")
        dataset = os.getenv("DATASET")
        if not (dataset == "FIT" or dataset == "SYS" or dataset == "TAXI"):
            raise Exception("Unsupported dataset argument")
        path_manifest_flink_session_cluster = (
            "/app/flink-session-cluster-deployment.yaml"
        )
        manifest_docs_flink_session_cluster = FrameworkScheduler.read_manifest(
            path_manifest_flink_session_cluster
        )
        # framework_scheduler.main_run(manifest_docs_flink_session_cluster, application, dataset, mongodb_address)
        framework_scheduler.debug_run(
            manifest_docs_flink_session_cluster, application, dataset, mongodb_address
        )
        scheduler_thread = threading.Thread(target=framework_scheduler.debug_run, name="FrameworkSchedulerThread")
        monitor_thread = threading.Thread(target=metrics_monitor.start, name="MetricsMonitorThread")

        scheduler_thread.start()
        monitor_thread.start()

        scheduler_thread.join()
        monitor_thread.join()
    except KeyboardInterrupt:
        logging.info("Shutting down")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

    finally:
        cleanup(framework_scheduler)
