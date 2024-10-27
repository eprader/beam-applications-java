import pytest
from scheduler_logic.scheduler_logic import calculate_utility_value, normalize_weights, normalize_throughput, normalize_latency, normalize_dataset
import numpy as np

def test_calculate_utility_value():
    throughput, latency, cpu = 0.8, 0.5, 0.3
    weights = [0.3, 0.5, 0.2]
    utility = calculate_utility_value(throughput, latency, cpu, weights)
    expected_utility = 0.3 * throughput - 0.5 * latency - 0.2 * cpu
    assert utility == pytest.approx(expected_utility), "Utility calculation failed"

def test_normalize_weights():
    weights = [1, 2, 3]
    normalized = normalize_weights(weights)
    assert sum(normalized) == pytest.approx(1.0), "Weights should sum to 1 after normalization"
    assert all(0 <= w <= 1 for w in normalized), "Normalized weights should be between 0 and 1"

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
    data = np.array([
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9]
    ])
    normalized_data = normalize_dataset(data)
    assert np.all((normalized_data >= 0) & (normalized_data <= 1)), "Data values should be between 0 and 1"
    assert np.allclose(np.min(normalized_data, axis=0), 0), "Minimum value of each column should be 0"
    assert np.allclose(np.max(normalized_data, axis=0), 1), "Maximum value of each column should be 1"

@pytest.mark.skip(reason="Test needs to be implemented once the function is ready")
def test_run_evaluation():
    pass