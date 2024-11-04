import numpy as np
import utils.Utils
import timeseriesPredictor.LoadPredictor
import database.database_access
import datetime
import database.database_access


def calculate_utility_value(throughput, latency, cpu, weights):
    w1, w2, w3 = weights
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
    latency_min, latency_max = 100, 1000
    return (latency - latency_min) / (latency_max - latency_min)


def normalize_throughput(throughput):
    throughput_min, throughput_max = 100, 1000
    return (throughput - throughput_min) / (throughput_max - throughput_min)


# FIXME
#Implement this properly
def run_evaluation(current_framework: utils.Utils.Framework, window_size:int):
    """
    Return either SL or SF
    Add latency penalty to not running framework, only one value
    """
    predictor = timeseriesPredictor.LoadPredictor.LoadPredictor()
    history = database.database_access.retrieve_input_rates_current_data()
    predictor.make_model_arima(history)
    
    database.database_access.store_decision_in_db(datetime.now(), decision)
    decision = current_framework
    return decision


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
    for metric in metrics_sf.keys():
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
