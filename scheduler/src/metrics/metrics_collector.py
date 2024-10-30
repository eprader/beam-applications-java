import logging
import requests


def read_metric_from_prometheus(metric_name):
    prometheus_url = "http://prometheus-operated.default.svc.cluster.local:9090"
    try:
        response = requests.get(
            f"{prometheus_url}/api/v1/query",
            params={"query": metric_name},
        )
        data = response.json()
        return data
    except Exception as e:
        logging.error(f"Error, when reading from prometheus: {e}")
        return list()


# Return as dict {"metric": value}
def get_critical_metrics_for_sf():
    critical_sf_task_operators = [
        "flink_taskmanager_job_task_idleTimeMsPerSecond",
        "flink_taskmanager_job_task_busyTimeMsPerSecond",
    ]
    mean_metrics = {}

    for metric in critical_sf_task_operators:
        values = read_metric_from_prometheus(metric)
        filtered_list = filter_critical_values_sf(
            values.get("data", {}).get("result", [])
        )
        mean = calculate_mean_of_dicts(filtered_list)
        if mean != None:
            mean_metrics[metric] = mean
            logging.info(f"Mean value for {metric}: {mean}")
        else:
            mean_metrics[metric] = None
            logging.warning(f"None returned for {metric}")

    return mean_metrics


# FIXME: Filter only the relevant operators
def filter_critical_values_sf(response):
    filtered_metrics = [
        result for result in response if result["metric"].get("task_name") != "feedback"
    ]
    extracted_data = [
        {"task_name": result["metric"]["task_name"], "value": result["value"][1]}
        for result in filtered_metrics
    ]

    return extracted_data


# Return as dict {"metric": value}
def get_critical_metrics_for_sl():
    critical_sl_task_operators = [
        "flink_taskmanager_job_task_backPressuredTimeMsPerSecond",
        "flink_taskmanager_job_task_busyTimeMsPerSecond",
    ]
    mean_metrics = {}

    for metric in critical_sl_task_operators:
        values = read_metric_from_prometheus(metric)
        filtered_list = filter_critical_values_sl(
            values.get("data", {}).get("result", [])
        )
        mean = calculate_mean_of_dicts(filtered_list)
        if mean != None:
            mean_metrics[metric] = mean
            logging.info(f"Mean value for {metric}: {mean}")
        else:
            mean_metrics[metric] = None
            logging.warning(f"None returned for {metric}")

    return mean_metrics


def filter_critical_values_sl(response):
    filtered_metrics = [
        result for result in response if result["metric"].get("task_name") != "feedback"
    ]
    extracted_data = [
        {"task_name": result["metric"]["task_name"], "value": result["value"][1]}
        for result in filtered_metrics
    ]

    return extracted_data


def calculate_mean_of_dicts(filtered_response):
    values = [
        float(metric["value"])
        for metric in filtered_response
        if metric["value"].replace(".", "", 1).isdigit()
    ]
    if values:
        mean_value = sum(values) / len(values)
        return mean_value
    else:
        return None


# Return as dict {"metric": value}
def get_objectives_for_sf():
    objectives_sf = [
        "flink_taskmanager_job_task_numRecordsOutPerSecond",
        "flink_taskmanager_job_task_operator_at_ac_uibk_dps_streamprocessingapplications_beam_Sink_custom_latency",
        "flink_taskmanager_Status_JVM_CPU_Load",
    ]
    objectives = {}

    for metric in objectives_sf:
        values = read_metric_from_prometheus(metric)
        if metric != "flink_taskmanager_job_task_numRecordsOutPerSecond":
            numeric_value = filter_objectives_sl(values)
        else:
            numeric_value = filter_num_records_out_sf(values)
        if numeric_value != None:
            objectives[metric] = numeric_value
            logging.info(f"Value for {metric}: {numeric_value}")
        else:
            objectives[metric] = None
            logging.warning(f"No values returned for {metric}")

    return objectives


def filter_num_records_out_sf(values):
    pass


# Return as dict {"metric": value}
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
    objectives = {}

    for metric in objectives_sl:
        values = read_metric_from_prometheus(metric)
        numeric_value = filter_objectives_sl(values)

        if numeric_value != None:
            objectives[metric] = numeric_value
            logging.info(f"Value for {metric}: {numeric_value}")
        else:
            objectives[metric] = None
            logging.warning(f"No values returned for {metric}")

    return objectives


def filter_objectives_sl(response):
    try:
        results = response.get("data", {}).get("result", [])
        if results:
            value = results[0].get("value", [None, None])[1]
            return float(value)
        else:
            return None
    except (IndexError, ValueError, TypeError) as e:
        logging.error(f"Error extracting metric value: {e}")
        return None
