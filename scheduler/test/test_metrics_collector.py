import pytest
from unittest.mock import patch, MagicMock
import json
import logging
from metrics.metrics_collector import get_latest_latency_value_sl



@pytest.fixture
def kafka_consumer_mock():
    with patch("metrics.metrics_collector.KafkaConsumer") as MockConsumer:
        mock_consumer_instance = MockConsumer.return_value
        mock_message = MagicMock()
        mock_message.value = b"1000, 500"
        mock_consumer_instance.poll = MagicMock(
            return_value={"partition": [mock_message]}
        )
        yield MockConsumer


def test_get_latest_latency_value_sl(kafka_consumer_mock):
    result = get_latest_latency_value_sl()
    assert result == float("500"), "Failed to retrieve correct latency data"


    kafka_consumer_mock.return_value.poll.return_value = {}
    result = get_latest_latency_value_sl()
    assert result is None, "Expected None when no messages are present"


def test_get_latest_latency_value_sl_error(kafka_consumer_mock):
    kafka_consumer_mock.return_value.poll.side_effect = Exception("Kafka error")
    with patch.object(logging, 'error') as mock_logging_error:
        result = get_latest_latency_value_sl()
        assert result is None, "Expected None on Kafka consumer error"
