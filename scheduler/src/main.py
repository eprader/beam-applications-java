import logging
import traceback
import os
import signal
import sys
from framework_scheduling.framework_scheduler import FrameworkScheduler
import scheduler_logic.evaluation_monitor
import threading
import scheduler_logic.scheduler_logic
import utils.Utils
import database.database_access

logging.basicConfig(
    stream=sys.stdout,
    level=logging.WARN,
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
    try:
        # Get settings from the manifest
        application = os.getenv("APPLICATION")
        mongodb_address = os.getenv("MONGODB")
        dataset = os.getenv("DATASET")
        if not (dataset == "FIT" or dataset == "SYS" or dataset == "TAXI"):
            raise Exception("Unsupported dataset argument")
        path_manifest_flink_session_cluster = (
            "/app/config_frameworks/flink-session-cluster-deployment.yaml"
        )
        manifest_docs_flink_session_cluster = utils.Utils.read_manifest(
            path_manifest_flink_session_cluster
        )
        # User settings
        start_framework = utils.Utils.Framework.SF
        window_size_dtw = 10
        threshold_dict_sf = {"idleTime": 850, "busyTime": 100}
        threshold_dict_sl = {"busyTime": 800, "backPressuredTime": 800}

        framework_running_event = threading.Event()
        evaluation_event = threading.Event()
        evaluation_monitor = scheduler_logic.evaluation_monitor.EvaluationMonitor(
            start_framework,
            evaluation_event,
            framework_running_event,
            application,
            dataset,
            threshold_dict_sf,
            threshold_dict_sl,
            window_size_dtw=window_size_dtw,
        )
        framework_scheduler = FrameworkScheduler(
            start_framework, evaluation_event, framework_running_event
        )
        scheduler_thread = threading.Thread(
            target=framework_scheduler.main_run,
            args=(
                manifest_docs_flink_session_cluster,
                application,
                dataset,
                mongodb_address,
            ),
            name="FrameworkSchedulerThread",
        )
        monitor_thread = threading.Thread(
            target=evaluation_monitor.start_monitoring, name="MetricsMonitorThread"
        )

        database.database_access.init_database(True)

        scheduler_thread.start()
        monitor_thread.start()

        scheduler_thread.join()
        logging.error("Scheduler thread has joined")
        monitor_thread.join()
        logging.error("Monitor thread has joined")

    except KeyboardInterrupt:
        logging.warning("Shutting down")
    except Exception as e:
        logging.error(f"An error occurred in the main class: {e}")
        logging.error("Traceback details:\n%s", traceback.format_exc())

    finally:
        cleanup(framework_scheduler)
