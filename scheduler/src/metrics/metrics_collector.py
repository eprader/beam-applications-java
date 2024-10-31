import logging
import requests
import json
import utils.Utils
from kafka import KafkaConsumer


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


def read_metric_from_prometheus_single_metric(metric_name):
    prometheus_url = "http://prometheus-operated.default.svc.cluster.local:9090"
    try:
        response = requests.get(
            f"{prometheus_url}/api/v1/query",
            params={"query": metric_name},
        )
        data = response.json()
        value = data["data"]["result"][0]["value"][1]
        return int(value)
    except Exception as e:
        logging.error(f"Error, when reading from prometheus: {e}")
        return None


# Return as {"idleTime":, "busyTime":}
def get_critical_metrics_for_sf(application: str):
    critical_sf_task_operators = [
        "flink_taskmanager_job_task_idleTimeMsPerSecond",
        "flink_taskmanager_job_task_busyTimeMsPerSecond",
    ]
    mean_metrics = {}

    for metric in critical_sf_task_operators:
        values = read_metric_from_prometheus(metric)
        filtered_list = filter_critical_values_sf(
            values.get("data", {}).get("result", []), application
        )
        mean = calculate_mean_of_dicts(filtered_list)
        if mean != None:
            if "idleTime" in metric:
                mean_metrics["idleTime"] = mean
            elif "busyTime" in metric:
                mean_metrics["busyTime"] = mean
            logging.info(f"Mean value for {metric}: {mean}")
        else:
            mean_metrics[metric] = None
            logging.warning(f"None returned for {metric}")

    return mean_metrics


def filter_critical_values_sf(response, application: str):
    filtered_metrics = [
        result
        for result in response
        if check_operator_name_sf(result["metric"].get("task_name"), application)
    ]
    extracted_data = [
        {"task_name": result["metric"]["task_name"], "value": result["value"][1]}
        for result in filtered_metrics
    ]

    return extracted_data


def check_operator_name_sf(name, application: str):
    if application == "PRED":
        operator_list = [
            "AverageBeam",
            "DecisionTreeBeam2",
            "ErrorEstimateBeam1",
            "ErrorEstimateBeam2",
            "LinearRegressionBeam1",
            "ParsePredictBeam",
            "Sink",
            "SourceBeam",
        ]
    elif application == "TRAIN":
        operator_list = [
            "AnnotateBeam",
            "BlobWriteBeam",
            "DecisionTreeBeam",
            "LinearRegressionBeam",
            "Sink",
            "TableReadBeam",
            "TimerSourceBeam",
        ]
    for operator in operator_list:
        if operator in name:
            return True
    return False


# Return as {"backPressuredTime":, "busyTime":}
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
            if "backPressureTime" in metric:
                mean_metrics["backPressureTime"] = mean
            elif "busyTime" in metric:
                mean_metrics["busyTime"] = mean
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


# Return as {"latency":500, "cpu_load":0.2, "throughput":500}
def get_objectives_for_sf(application):
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
            numeric_value = filter_num_records_out_sf(values, application)
        if numeric_value != None:
            if "numRecordsOut" in metric:
                objectives["throughput"] = numeric_value
            elif "CPU_Load" in metric:
                objectives["cpu_load"] = numeric_value
            elif "custom_latency" in metric:
                objectives["latency"] = numeric_value
            logging.info(f"Value for {metric}: {numeric_value}")
        else:
            objectives[metric] = None
            logging.warning(f"No values returned for {metric}")

    return objectives


def filter_num_records_out_sf(response, application: str):
    filtered_metrics = [
        result
        for result in response
        if "flink_taskmanager_job_task_numRecordsOutPerSecond_WriteStringSink_Write_SenML_strings_to_Kafka_KafkaIO_Write_Kafka_ProducerRecord_Map_ParMultiDo_Anonymous"
        in result["metric"].get("task_name")
    ]
    extracted_data = [
        {"task_name": result["metric"]["task_name"], "value": result["value"][1]}
        for result in filtered_metrics
    ]

    return extracted_data


# Return as {"latency":500, "cpu_load":0.2, "throughput":500}
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
    objectives_sl.append("latency")
    objectives = {}

    for metric in objectives_sl:
        if metric == "latency":
            objectives[metric] = float(get_latest_latency_value_sl())
            continue

        values = read_metric_from_prometheus(metric)
        numeric_value = filter_objectives_sl(values)

        if numeric_value != None:
            if "CPU_Load" in metric:
                objectives["CPU_Load"] = numeric_value
            elif "outLocalRate" in metric:
                objectives["throughput"] = numeric_value
            logging.info(f"Value for {metric}: {numeric_value}")
        else:
            objectives[metric] = None
            logging.warning(f"No values returned for {metric}")

    return objectives


def get_latest_latency_value_sl():
    try:
        kafka_consumer = KafkaConsumer(
            "pred-publish",
            bootstrap_servers=["kafka-cluster-kafka-bootstrap.default.svc:9092"],
        )
        message = next(kafka_consumer.poll(timeout_ms=1000).values())
        if message:
            message_value = json.loads(message[0].value.decode("utf-8"))
            logging.info(f"Latest message from 'pred-publish': {message_value}")
            return message_value
        else:
            logging.warning("No new messages in 'pred-publish' topic.")
            return None
    except StopIteration:
        logging.warning("No new messages in 'pred-publish' topic.")
        return None
    except Exception as e:
        logging.error(f"Error fetching message from Kafka: {e}")
        return None


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

def get_numRecordsOut(framework: utils.Utils.Framework, application: str):
    if framework == utils.Utils.Framework.SL:
        if application == "TRAIN":
            metric_name = "flink_taskmanager_job_task_operator_functions_pred_mqttPublishTrain_outEgress"
        elif application == "PRED":
            metric_name = "flink_taskmanager_job_task_operator_functions_pred_mqttPublish_outEgress"
        return read_metric_from_prometheus_single_metric(metric_name)
    elif framework == utils.Utils.Framework.SF:
        metric_name = "flink_taskmanager_job_task_numRecordsIn"
        response = read_metric_from_prometheus(metric_name)
        filtered_metrics = [
            result for result in response if "Sink" in result["metric"].get("task_name")
        ]
    extracted_data = [
        {"task_name": result["metric"]["task_name"], "value": result["value"][1]}
        for result in filtered_metrics
    ]
    return extracted_data[0]
