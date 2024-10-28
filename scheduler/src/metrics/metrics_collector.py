import logging
import requests
import sys
import statistics


def read_metric_from_prometheus(metric_name):
    prometheus_url = "http://prometheus-operated.default.svc.cluster.local:9090"
    try:
        response = requests.get(
            f"{prometheus_url}/api/v1/query",
            params={"query": metric_name},
        )
        data = response.json()
        values = [float(result["value"][1]) for result in data["data"]["result"]]
        return values
    except Exception as e:
        logging.error(f"Error, when reading from prometheus: {e}")
        return list()

def get_critical_metrics_for_sf():
    critical_sf_task_operators = [
        "flink_taskmanager_job_task_idleTimeMsPerSecond",
        "flink_taskmanager_job_task_busyTimeMsPerSecond",
    ]
    mean_metrics = {}

    for metric in critical_sf_task_operators:
        values = read_metric_from_prometheus(metric)

        if values:
            mean_value = statistics.mean(values)
            mean_metrics[metric] = mean_value
            logging.info(f"Mean value for {metric}: {mean_value}")
        else:
            mean_metrics[metric] = None
            logging.warning(f"No values returned for {metric}")

    return mean_metrics


def get_critical_metrics_for_sl():
    critical_sl_task_operators = [
        "flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
        "flink_taskmanager_job_task_busyTimeMsPerSecond",
    ]
    mean_metrics = {}

    for metric in critical_sl_task_operators:
        values = read_metric_from_prometheus(metric)

        if values:
            mean_value = statistics.mean(values)
            mean_metrics[metric] = mean_value
            logging.info(f"Mean value for {metric}: {mean_value}")
        else:
            mean_metrics[metric] = None
            logging.warning(f"No values returned for {metric}")

    return mean_metrics


def get_objectives_for_sf():
    objectives_sf = [
        "flink_taskmanager_job_task_numRecordsOutPerSecond",
        "flink_taskmanager_job_task_operator_at_ac_uibk_dps_streamprocessingapplications_beam_Sink_custom_latency",
        "flink_taskmanager_Status_JVM_CPU_Load",
    ]
    mean_objectives = {}

    for metric in objectives_sf:
        values = read_metric_from_prometheus(metric)

        if values:
            mean_value = statistics.mean(values)
            mean_objectives[metric] = mean_value
            logging.info(f"Mean value for {metric}: {mean_value}")
        else:
            mean_objectives[metric] = None
            logging.warning(f"No values returned for {metric}")

    return mean_objectives


def get_objectives_for_sl(application):
    objectives_sl = list()
    if application == "PRED":
        objectives_sl.append(
            "flink_taskmanager_job_task_operator_functions_pred_mqttPublish_outLocalRate"
        )
    else:
        objectives_sl.append(
            "flink_taskmanager_job_task_operator_functions_pred_mqttPublishTrain_outLocalRate"
        )
    objectives_sl.append("flink_taskmanager_Status_JVM_CPU_Load")
    mean_objectives = {}

    for metric in objectives_sl:
        values = read_metric_from_prometheus(metric)

        if values:
            mean_value = statistics.mean(values)
            mean_objectives[metric] = mean_value
            logging.info(f"Mean value for {metric}: {mean_value}")
        else:
            mean_objectives[metric] = None
            logging.warning(f"No values returned for {metric}")

    return mean_objectives
