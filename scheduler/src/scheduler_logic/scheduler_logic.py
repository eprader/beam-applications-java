import numpy as np
import utils.Utils
import logging
import timeseriesPredictor.LoadPredictor
import database.database_access
import datetime
import scheduler_logic.similarity_calculation
from collections import defaultdict


def calculate_utility_value(metrics_dict: dict, weights: dict):
    w1, w2, w3 = weights["throughput"], weights["latency"], weights["cpu_load"]
    throughput, latency, cpu = (
        metrics_dict["throughput"],
        metrics_dict["latency"],
        metrics_dict["cpu_load"],
    )
    utility = w1 * throughput - w2 * latency - w3 * cpu
    return utility


def normalize_dataset(data):
    normalized_data = np.zeros_like(data)
    for i in range(data.shape[1]):
        min_val = np.min(data[:, i])
        max_val = np.max(data[:, i])
        normalized_data[:, i] = (data[:, i] - min_val) / (max_val - min_val)
    return normalized_data


def normalize_weights(weights_list):
    total_sum = sum(weights_list)
    normalized_weights = [w / total_sum for w in weights_list]
    return normalized_weights


def normalize_latency(latency):
    latency_min, latency_max = 10, 600000
    return (latency - latency_min) / (latency_max - latency_min)


def normalize_throughput(throughput):
    throughput_min, throughput_max = 0, 1000
    return (throughput - throughput_min) / (throughput_max - throughput_min)


def check_historic_data_validity(historic_data, window_size: int):
    if len(historic_data) == 0:
        return False
    if len(historic_data) < window_size:
        return False
    return True


def run_evaluation(current_framework: utils.Utils.Framework, window_size: int):
    """
    Return either SL or SF
    Add latency penalty to not running framework
    """
    test_instance = timeseriesPredictor.LoadPredictor.LoadPredictor()
    history = database.database_access.retrieve_input_rates_current_data()
    if check_historic_data_validity(history):
        logging.warning("input data for ARIMA too small")
        return current_framework
    
    values_list = [entry["input_rate_records_per_second"] for entry in history]
    test_instance.make_model_arima(values_list)
    predictions = test_instance.make_predictions_arima(window_size)

    historic_data_sf = database.database_access.retrieve_historic_data("SF")
    historic_data_sl = database.database_access.retrieve_historic_data("SL")

    if not (
        check_historic_data_validity(historic_data_sf, window_size)
        and check_historic_data_validity(historic_data_sl, window_size)
    ):
        logging.warning(
            "No decision could be made, because historic data has not enough entries"
        )
        return current_framework

    best_window_sf, best_distance_sf = (
        scheduler_logic.similarity_calculation.find_most_similar_window(
            predictions, historic_data_sf
        )
    )
    best_window_sl, best_distance_sl = (
        scheduler_logic.similarity_calculation.find_most_similar_window(
            predictions, historic_data_sl
        )
    )
    if len(predictions) != window_size or (
        len(predictions) != len(best_window_sf)
        and len(best_window_sf) != len(best_window_sl)
    ):
        raise Exception("List have not the same length")

    normalized_metrics_sf = [normalize_metrics(element) for element in best_window_sf]
    normalized_metrics_sl = [normalize_metrics(element) for element in best_window_sl]

    if current_framework == utils.Utils.Framework.SL:
        if "latency" in normalized_metrics_sf[-1]:
            normalized_metrics_sf[-1]["latency"] += normalize_latency(10000)
    else:
        if "latency" in normalized_metrics_sl[-1]:
            normalized_metrics_sl[-1]["latency"] += normalize_latency(10000)

    normalized_mean_metrics_sf = calculate_normalized_mean(normalized_metrics_sf)
    normalized_mean_metrics_sl = calculate_normalized_mean(normalized_metrics_sl)

    weights = calculate_weights_with_entropy(
        normalized_mean_metrics_sf, normalized_mean_metrics_sl
    )

    u_score_list_sf = [
        calculate_utility_value(entry, weights) for entry in normalized_metrics_sf
    ]
    u_score_list_sl = [
        calculate_utility_value(entry, weights) for entry in normalized_metrics_sl
    ]

    u_score_mean_sf = sum(u_score_list_sf) / len(u_score_list_sf)
    u_score_mean_sl = sum(u_score_list_sl) / len(u_score_list_sl)
    decision = make_decision_based_on_score(
        u_score_mean_sf, u_score_mean_sl, current_framework
    )
    decision_dict = dict()
    decision_dict["used_framework"] = decision
    decision_dict["u_sf"] = u_score_mean_sf
    decision_dict["u_sl"] = u_score_mean_sl
    database.database_access.store_decision_in_db(
        datetime.datetime.now(), decision_dict
    )

    return decision


def calculate_normalized_mean(metrics_list):
    metrics_sum = defaultdict(float)
    metrics_count = defaultdict(int)
    for entry in metrics_list:
        for metric, value in entry.items():
            metrics_sum[metric] += value
            metrics_count[metric] += 1
    return {
        metric: metrics_sum[metric] / metrics_count[metric] for metric in metrics_sum
    }


def make_decision_based_on_score(
    u_mean_sf: float,
    u_mean_sl: float,
    current_framework: utils.Utils.Framework,
    threshold=None,
):
    if u_mean_sf > u_mean_sl:
        decision = utils.Utils.Framework.SF

    elif u_mean_sf < u_mean_sl:
        decision = utils.Utils.Framework.SL

    else:
        decision = current_framework
    return decision


def normalize_metrics(window_element: dict):
    metrics_normalized = dict()

    metrics_normalized["throughput"] = normalize_throughput(
        window_element["throughput"]
    )
    metrics_normalized["latency"] = normalize_latency(window_element["latency"])
    metrics_normalized["cpu_load"] = window_element["cpu_load"]
    return metrics_normalized


def compute_entropy(metrics_sf, metrics_sl, metric_name, k=3, n=2):
    sf_value = metrics_sf[metric_name]
    sl_value = metrics_sl[metric_name]

    if sf_value <= 0 or sl_value <= 0:
        raise ValueError(
            f"Metric values for {metric_name} must be positive and non-zero."
        )

    entropy_sum = sf_value * np.log10(sf_value) + sl_value * np.log10(sl_value)
    return (-1.0 / np.log10(k)) * entropy_sum


def compute_degree_of_divergence(entropy):
    return 1 - entropy


def calculate_weights_with_entropy(metrics_sf: dict, metrics_sl: dict):
    divergence_sum = 0
    divergence_dict = dict()
    relevant_metrics = ["latency", "cpu_load", "throughput"]
    for metric in relevant_metrics:
        entropy = compute_entropy(metrics_sf, metrics_sl, metric)
        divergence = compute_degree_of_divergence(entropy)
        divergence_dict[metric] = divergence
        divergence_sum += divergence

    weights_dict = {
        metric: divergence / divergence_sum
        for metric, divergence in divergence_dict.items()
    }
    return weights_dict


def main(user_weights):
    metrics_sf = [600, 130, 0.60]
    metrics_sl = [500, 80, 0.75]

    metrics_normalized_sf = list()
    metrics_normalized_sl = list()

    metrics_normalized_sf.append(normalize_throughput(metrics_sf[0]))
    metrics_normalized_sf.append(normalize_latency(metrics_sf[1]))
    metrics_normalized_sf.append(metrics_sf[2])

    metrics_normalized_sl.append(normalize_throughput(metrics_sl[0]))
    metrics_normalized_sl.append(normalize_latency(metrics_sl[1]))
    metrics_normalized_sl.append(metrics_sl[2])

    normalized_weights = normalize_weights(user_weights)
    u_sf = calculate_utility_value(
        metrics_normalized_sf[0],
        metrics_normalized_sf[1],
        metrics_normalized_sf[2],
        normalized_weights,
    )
    print("SF: ", u_sf)
    u_sl = calculate_utility_value(
        metrics_normalized_sl[0],
        metrics_normalized_sl[1],
        metrics_normalized_sl[2],
        normalized_weights,
    )
    print("SL: ", u_sl)
    print("Decision ", "SF" if (u_sf > u_sl) else "SL")


if __name__ == "__main__":
    weights = [0.1, 0.7, 0.1]
    main(weights)
