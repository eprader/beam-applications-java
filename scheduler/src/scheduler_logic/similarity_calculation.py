from fastdtw import fastdtw
from scipy.spatial.distance import euclidean
import numpy as np

#DEBUG
def get_full_historic_data_debug(cursor, table_name, framework):
    list_sl = [
        {
            "date": "2024-11-11",
            "load": 500,
            "cpu": 0.1,
            "throughput": 400,
            "latency": 6000,
            "framework": "SL",
        },
        {
            "date": "2024-11-12",
            "load": 520,
            "cpu": 0.12,
            "throughput": 420,
            "latency": 5900,
            "framework": "SL",
        },
        {
            "date": "2024-11-13",
            "load": 480,
            "cpu": 0.08,
            "throughput": 380,
            "latency": 6100,
            "framework": "SL",
        },
        {
            "date": "2024-11-14",
            "load": 510,
            "cpu": 0.11,
            "throughput": 405,
            "latency": 6050,
            "framework": "SL",
        },
        {
            "date": "2024-11-15",
            "load": 495,
            "cpu": 0.09,
            "throughput": 395,
            "latency": 6150,
            "framework": "SL",
        },
        {
            "date": "2024-11-16",
            "load": 500,
            "cpu": 0.13,
            "throughput": 440,
            "latency": 5850,
            "framework": "SL",
        },
        {
            "date": "2024-11-17",
            "load": 500,
            "cpu": 0.07,
            "throughput": 370,
            "latency": 6200,
            "framework": "SL",
        },
        {
            "date": "2024-11-18",
            "load": 500,
            "cpu": 0.1,
            "throughput": 415,
            "latency": 5950,
            "framework": "SL",
        },
        {
            "date": "2024-11-19",
            "load": 500,
            "cpu": 0.09,
            "throughput": 400,
            "latency": 6000,
            "framework": "SL",
        },
        {
            "date": "2024-11-20",
            "load": 500,
            "cpu": 0.12,
            "throughput": 430,
            "latency": 5900,
            "framework": "SL",
        },
    ]
    list_sf = list_sf = [
        {
            "date": "2024-11-11",
            "load": 490,
            "cpu": 0.11,
            "throughput": 410,
            "latency": 6100,
            "framework": "SF",
        },
        {
            "date": "2024-11-12",
            "load": 515,
            "cpu": 0.13,
            "throughput": 425,
            "latency": 6000,
            "framework": "SF",
        },
        {
            "date": "2024-11-13",
            "load": 470,
            "cpu": 0.1,
            "throughput": 390,
            "latency": 6200,
            "framework": "SF",
        },
        {
            "date": "2024-11-14",
            "load": 500,
            "cpu": 0.12,
            "throughput": 415,
            "latency": 6050,
            "framework": "SF",
        },
        {
            "date": "2024-11-15",
            "load": 485,
            "cpu": 0.09,
            "throughput": 405,
            "latency": 6150,
            "framework": "SF",
        },
        {
            "date": "2024-11-16",
            "load": 520,
            "cpu": 0.14,
            "throughput": 435,
            "latency": 5950,
            "framework": "SF",
        },
        {
            "date": "2024-11-17",
            "load": 465,
            "cpu": 0.08,
            "throughput": 380,
            "latency": 6250,
            "framework": "SF",
        },
        {
            "date": "2024-11-18",
            "load": 510,
            "cpu": 0.11,
            "throughput": 420,
            "latency": 6000,
            "framework": "SF",
        },
        {
            "date": "2024-11-19",
            "load": 480,
            "cpu": 0.1,
            "throughput": 400,
            "latency": 6050,
            "framework": "SF",
        },
        {
            "date": "2024-11-20",
            "load": 525,
            "cpu": 0.13,
            "throughput": 430,
            "latency": 5950,
            "framework": "SF",
        },
    ]
    if framework == "SL":
        return list_sl
    else:
        return list_sf


def calculate_dtw(current_data, historic_data_window):
    distance, _ = fastdtw(current_data, historic_data_window, dist=2)
    return distance


def find_most_similar_window(current_rate, historic_data):
    min_distance = float("inf")
    best_window = []
    window_size = len(current_rate)
    for i in range(len(historic_data) - window_size + 1):
        window = historic_data[i : i + window_size]
        history_rate = [entry["input_rate_records_per_second"] for entry in window]
        distance = calculate_dtw(current_rate, history_rate)

        if distance < min_distance:
            min_distance = distance
            best_window = window

    return best_window, min_distance


def main():
    current_rate =  [500, 500, 500, 500, 500]
    list_sf = get_full_historic_data_debug("", "", "SF")
    list_sl = get_full_historic_data_debug("", "", "SL")
    best_window_sf, best_distance_sf = find_most_similar_window(current_rate, list_sf)
    best_window_sl, best_distance_sl = find_most_similar_window(current_rate, list_sl)

    print(best_window_sf, best_distance_sf)
    print(best_window_sl, best_distance_sl)

if __name__ =="__main__":
    main()
