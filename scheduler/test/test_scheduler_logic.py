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
)
import numpy as np
from unittest.mock import patch


def test_calculate_utility_value():
    throughput, latency, cpu = 0.8, 0.5, 0.3
    weights = [0.3, 0.5, 0.2]
    utility = calculate_utility_value(throughput, latency, cpu, weights)
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
    expected = (throughput - 100) / (1000 - 100)
    assert normalized == pytest.approx(expected), "Throughput normalization failed"


def test_normalize_latency():
    latency = 150
    normalized = normalize_latency(latency)
    expected = (latency - 100) / (1000 - 100)
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


@pytest.mark.skip(reason="Test needs to be implemented once the function is ready")
def test_run_evaluation():
    pass


def test_compute_entropy():
    metrics_sf = {"latency": 100, "cpu": 0.5, "throughput": 800}
    metrics_sl = {"latency": 120, "cpu": 0.6, "throughput": 750}

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
    metrics_sf = {"latency": 100, "cpu": 0.5, "throughput": 800}
    metrics_sl = {"latency": 120, "cpu": 0.6, "throughput": 750}

    mock_compute_entropy.side_effect = [0.2, 0.3, 0.4]
    mock_compute_divergence.side_effect = [0.8, 0.7, 0.6]

    weights = calculate_weights_with_entropy(metrics_sf, metrics_sl)

    total_divergence = sum([0.8, 0.7, 0.6])
    expected_weights = {
        "latency": 0.8 / total_divergence,
        "cpu": 0.7 / total_divergence,
        "throughput": 0.6 / total_divergence,
    }
    assert weights == pytest.approx(
        expected_weights
    ), "Weight calculation with entropy failed"

    assert mock_compute_entropy.call_count == 3
    assert mock_compute_divergence.call_count == 3
