import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from database.database_access import (
    create_database,
    store_scheduler_metrics,
    retrieve_historic_data,
    db_name,
)

sample_timestamp = datetime.now()
sample_objectives = {"latency": 100.0, "cpu_load": 0.5, "throughput": 200.0}
sample_input_rate = {"input_rate_records_per_second": 150.0}
sample_framework = "SL"


@pytest.fixture
def mock_db_connection():
    with patch("database.database_access.mysql.connector.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        yield mock_conn


def test_create_database(mock_db_connection):
    mock_cursor = mock_db_connection.cursor.return_value
    create_database(mock_cursor, db_name)
    mock_cursor.execute.assert_called_with(f"CREATE DATABASE IF NOT EXISTS {db_name}")


def test_store_scheduler_metrics(mock_db_connection):
    mock_cursor = mock_db_connection.cursor.return_value
    store_scheduler_metrics(
        sample_timestamp, sample_objectives, sample_input_rate, sample_framework
    )

    expected_data = (
        sample_timestamp,
        sample_objectives["latency"],
        sample_objectives["cpu_load"],
        sample_objectives["throughput"],
        sample_input_rate["input_rate_records_per_second"],
        sample_framework,
    )
    insert_query = """
        INSERT INTO scheduler_metrics (timestamp, latency, cpu_load, throughput, input_rate_records_per_second, framework)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    mock_db_connection.commit.assert_called_once()


def test_retrieve_historic_data(mock_db_connection):
    mock_cursor = mock_db_connection.cursor.return_value
    sample_data = [
        {"id": 1, "timestamp": sample_timestamp, "latency": 100.0, "framework": "SL"}
    ]
    mock_cursor.fetchall.return_value = sample_data

    result = retrieve_historic_data("SL")
    assert result == sample_data

    select_query = f"SELECT * FROM historic_metrics WHERE framework = %s"
    mock_cursor.execute.assert_called_with(select_query, ("SL",))
