import pytest
from scheduler_logic.scheduler_logic import (
    calculate_utility_value,
    normalize_weights,
    normalize_throughput,
    normalize_latency,
    normalize_dataset,
    compute_entropy,
    compute_degree_of_divergence,
    calculate_weights_with_entropy,
    run_evaluation,
)
import numpy as np
from unittest.mock import patch
import datetime
import utils.Utils


def test_calculate_utility_value():
    throughput, latency, cpu = 0.8, 0.5, 0.3
    metrics_dict = {"throughput": throughput, "latency": latency, "cpu_load": cpu}
    weights = {"throughput": 0.3, "latency": 0.5, "cpu_load": 0.2}
    utility = calculate_utility_value(metrics_dict, weights)
    expected_utility = 0.3 * throughput - 0.5 * latency - 0.2 * cpu
    assert utility == pytest.approx(expected_utility), "Utility calculation failed"


def test_normalize_weights():
    weights = [1, 2, 3]
    normalized = normalize_weights(weights)
    assert sum(normalized) == pytest.approx(
        1.0
    ), "Weights should sum to 1 after normalization"
    assert all(
        0 <= w <= 1 for w in normalized
    ), "Normalized weights should be between 0 and 1"


def test_normalize_throughput():
    throughput = 550
    normalized = normalize_throughput(throughput)
    expected = (throughput - 0) / (1000 - 0)
    assert normalized == pytest.approx(expected), "Throughput normalization failed"


def test_normalize_latency():
    latency = 150
    normalized = normalize_latency(latency)
    expected = (latency - 10) / (600000 - 10)
    assert normalized == pytest.approx(expected), "Latency normalization failed"


def test_normalize_dataset():
    data = np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    normalized_data = normalize_dataset(data)
    assert np.all(
        (normalized_data >= 0) & (normalized_data <= 1)
    ), "Data values should be between 0 and 1"
    assert np.allclose(
        np.min(normalized_data, axis=0), 0
    ), "Minimum value of each column should be 0"
    assert np.allclose(
        np.max(normalized_data, axis=0), 1
    ), "Maximum value of each column should be 1"


historic_sl_data = [
    {
        "id": 1,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 30, 0),
        "latency": 85.4,
        "cpu_load": 0.25,
        "throughput": 320.0,
        "input_rate_records_per_second": 180.5,
        "framework": "SL",
    },
    {
        "id": 2,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 31, 0),
        "latency": 90.1,
        "cpu_load": 0.27,
        "throughput": 325.0,
        "input_rate_records_per_second": 185.5,
        "framework": "SL",
    },
    {
        "id": 3,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 32, 0),
        "latency": 87.6,
        "cpu_load": 0.26,
        "throughput": 322.0,
        "input_rate_records_per_second": 182.0,
        "framework": "SL",
    },
    {
        "id": 4,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 33, 0),
        "latency": 88.2,
        "cpu_load": 0.28,
        "throughput": 330.0,
        "input_rate_records_per_second": 187.0,
        "framework": "SL",
    },
    {
        "id": 5,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 34, 0),
        "latency": 86.5,
        "cpu_load": 0.24,
        "throughput": 318.0,
        "input_rate_records_per_second": 179.0,
        "framework": "SL",
    },
    {
        "id": 6,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 35, 0),
        "latency": 89.7,
        "cpu_load": 0.29,
        "throughput": 332.0,
        "input_rate_records_per_second": 188.5,
        "framework": "SL",
    },
    {
        "id": 7,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 36, 0),
        "latency": 84.9,
        "cpu_load": 0.23,
        "throughput": 315.0,
        "input_rate_records_per_second": 177.5,
        "framework": "SL",
    },
    {
        "id": 8,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 37, 0),
        "latency": 91.0,
        "cpu_load": 0.30,
        "throughput": 335.0,
        "input_rate_records_per_second": 190.0,
        "framework": "SL",
    },
    {
        "id": 9,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 38, 0),
        "latency": 85.8,
        "cpu_load": 0.26,
        "throughput": 323.0,
        "input_rate_records_per_second": 183.0,
        "framework": "SL",
    },
    {
        "id": 10,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 39, 0),
        "latency": 87.2,
        "cpu_load": 0.25,
        "throughput": 319.0,
        "input_rate_records_per_second": 181.0,
        "framework": "SL",
    },
]

historic_sf_data = [
    {
        "id": 11,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 40, 0),
        "latency": 78.4,
        "cpu_load": 0.22,
        "throughput": 310.0,
        "input_rate_records_per_second": 170.5,
        "framework": "SF",
    },
    {
        "id": 12,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 41, 0),
        "latency": 79.1,
        "cpu_load": 0.23,
        "throughput": 315.0,
        "input_rate_records_per_second": 172.5,
        "framework": "SF",
    },
    {
        "id": 13,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 42, 0),
        "latency": 80.0,
        "cpu_load": 0.24,
        "throughput": 320.0,
        "input_rate_records_per_second": 175.0,
        "framework": "SF",
    },
    {
        "id": 14,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 43, 0),
        "latency": 77.5,
        "cpu_load": 0.21,
        "throughput": 305.0,
        "input_rate_records_per_second": 168.0,
        "framework": "SF",
    },
    {
        "id": 15,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 44, 0),
        "latency": 82.0,
        "cpu_load": 0.25,
        "throughput": 325.0,
        "input_rate_records_per_second": 178.0,
        "framework": "SF",
    },
    {
        "id": 16,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 45, 0),
        "latency": 76.8,
        "cpu_load": 0.20,
        "throughput": 300.0,
        "input_rate_records_per_second": 165.0,
        "framework": "SF",
    },
    {
        "id": 17,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 46, 0),
        "latency": 83.1,
        "cpu_load": 0.26,
        "throughput": 330.0,
        "input_rate_records_per_second": 180.0,
        "framework": "SF",
    },
    {
        "id": 18,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 47, 0),
        "latency": 79.9,
        "cpu_load": 0.22,
        "throughput": 315.0,
        "input_rate_records_per_second": 173.0,
        "framework": "SF",
    },
    {
        "id": 19,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 48, 0),
        "latency": 81.4,
        "cpu_load": 0.24,
        "throughput": 318.0,
        "input_rate_records_per_second": 174.0,
        "framework": "SF",
    },
    {
        "id": 20,
        "timestamp": datetime.datetime(2024, 11, 4, 10, 49, 0),
        "latency": 80.7,
        "cpu_load": 0.23,
        "throughput": 317.0,
        "input_rate_records_per_second": 173.5,
        "framework": "SF",
    },
]

input_rate_return_data = [
    {"input_rate_records_per_second": 505.0},
    {"input_rate_records_per_second": 495.0},
    {"input_rate_records_per_second": 500.0},
    {"input_rate_records_per_second": 510.0},
    {"input_rate_records_per_second": 495.0},
    {"input_rate_records_per_second": 505.0},
    {"input_rate_records_per_second": 498.0},
    {"input_rate_records_per_second": 502.0},
    {"input_rate_records_per_second": 497.0},
    {"input_rate_records_per_second": 503.0},
    {"input_rate_records_per_second": 499.0},
    {"input_rate_records_per_second": 501.0},
]


@patch("database.database_access.retrieve_input_rates_current_data", autospec=True)
@patch("database.database_access.store_decision_in_db", autospec=True)
@patch("database.database_access.retrieve_historic_data", autospec=True)
def test_run_evaluation_starting_with_sf(
    mock_retrieve_historic, mock_store_decision, mock_retrieve_input_rates
):
    mock_retrieve_input_rates.return_value = input_rate_return_data

    def side_effect_function(framework):
        if framework == "SL":
            return historic_sl_data
        elif framework == "SF":
            return historic_sf_data

    mock_retrieve_historic.side_effect = side_effect_function

    current_framework = "SF"
    window_size = 5
    result = run_evaluation(current_framework, window_size)
    assert result in [utils.Utils.Framework.SF, utils.Utils.Framework.SL]
    mock_retrieve_input_rates.assert_called_once()
    mock_store_decision.assert_called_once()

    store_decision_args = mock_store_decision.call_args
    print("store_decision arguments:", store_decision_args)


@patch("database.database_access.retrieve_input_rates_current_data", autospec=True)
@patch("database.database_access.store_decision_in_db", autospec=True)
@patch("database.database_access.retrieve_historic_data", autospec=True)
def test_run_evaluation_starting_with_sl(
    mock_retrieve_historic, mock_store_decision, mock_retrieve_input_rates
):
    mock_retrieve_input_rates.return_value = input_rate_return_data

    def side_effect_function(framework):
        if framework == "SL":
            return historic_sl_data
        elif framework == "SF":
            return historic_sf_data

    mock_retrieve_historic.side_effect = side_effect_function

    current_framework = "SL"
    window_size = 5
    result = run_evaluation(current_framework, window_size)
    assert result in [utils.Utils.Framework.SF, utils.Utils.Framework.SL]
    mock_retrieve_input_rates.assert_called_once()
    mock_store_decision.assert_called_once()

    store_decision_args = mock_store_decision.call_args
    print("store_decision arguments:", store_decision_args)


def test_compute_entropy():
    metrics_sf = {"latency": 100, "cpu_load": 0.5, "throughput": 800}
    metrics_sl = {"latency": 120, "cpu_load": 0.6, "throughput": 750}

    entropy = compute_entropy(metrics_sf, metrics_sl, "latency")

    expected_entropy = (-1.0 / np.log10(3)) * (
        metrics_sf["latency"] * np.log10(metrics_sf["latency"])
        + metrics_sl["latency"] * np.log10(metrics_sl["latency"])
    )
    assert entropy == pytest.approx(
        expected_entropy
    ), "Entropy calculation failed for latency metric"


def test_compute_degree_of_divergence():
    entropy = 0.8
    divergence = compute_degree_of_divergence(entropy)
    expected_divergence = 1 - entropy
    assert divergence == pytest.approx(
        expected_divergence
    ), "Degree of divergence calculation failed"


@patch("scheduler_logic.scheduler_logic.compute_entropy")
@patch("scheduler_logic.scheduler_logic.compute_degree_of_divergence")
def test_calculate_weights_with_entropy(mock_compute_divergence, mock_compute_entropy):
    metrics_sf = {"latency": 100, "cpu_load": 0.5, "throughput": 800}
    metrics_sl = {"latency": 120, "cpu_load": 0.6, "throughput": 750}

    mock_compute_entropy.side_effect = [0.2, 0.3, 0.4]
    mock_compute_divergence.side_effect = [0.8, 0.7, 0.6]

    weights = calculate_weights_with_entropy(metrics_sf, metrics_sl)

    total_divergence = sum([0.8, 0.7, 0.6])
    expected_weights = {
        "latency": 0.8 / total_divergence,
        "cpu_load": 0.7 / total_divergence,
        "throughput": 0.6 / total_divergence,
    }
    assert weights == pytest.approx(
        expected_weights
    ), "Weight calculation with entropy failed"

    assert mock_compute_entropy.call_count == 3
    assert mock_compute_divergence.call_count == 3
