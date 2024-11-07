from fastdtw import fastdtw


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
