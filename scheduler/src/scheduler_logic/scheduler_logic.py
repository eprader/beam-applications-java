import numpy as np


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
    normalized_weights = [w / total_sum for w in weights]
    return normalized_weights


def normalize_latency(latency):
    latency_min, latency_max = 100, 1000
    return (latency - latency_min) / (latency_max - latency_min)


def normalize_throughput(throughput):
    throughput_min, throughput_max = 100, 1000
    return (throughput - throughput_min) / (throughput_max - throughput_min)


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
