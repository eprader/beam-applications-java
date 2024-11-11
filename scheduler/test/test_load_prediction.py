import pytest
import datetime
import pandas as pd
from timeseriesPredictor.LoadPredictor import LoadPredictor


list_sl = [
    {
        "date": "2024-11-11",
        "input_rate_records_per_second": 500,
        "cpu": 0.1,
        "throughput": 400,
        "latency": 6000,
        "framework": "SL",
    },
    {
        "date": "2024-11-12",
        "input_rate_records_per_second": 520,
        "cpu": 0.12,
        "throughput": 420,
        "latency": 5900,
        "framework": "SL",
    },
    {
        "date": "2024-11-13",
        "input_rate_records_per_second": 480,
        "cpu": 0.08,
        "throughput": 380,
        "latency": 6100,
        "framework": "SL",
    },
    {
        "date": "2024-11-14",
        "input_rate_records_per_second": 510,
        "cpu": 0.11,
        "throughput": 405,
        "latency": 6050,
        "framework": "SL",
    },
    {
        "date": "2024-11-15",
        "input_rate_records_per_second": 495,
        "cpu": 0.09,
        "throughput": 395,
        "latency": 6150,
        "framework": "SL",
    },
    {
        "date": "2024-11-16",
        "input_rate_records_per_second": 500,
        "cpu": 0.13,
        "throughput": 440,
        "latency": 5850,
        "framework": "SL",
    },
    {
        "date": "2024-11-17",
        "input_rate_records_per_second": 500,
        "cpu": 0.07,
        "throughput": 370,
        "latency": 6200,
        "framework": "SL",
    },
    {
        "date": "2024-11-18",
        "input_rate_records_per_second": 500,
        "cpu": 0.1,
        "throughput": 415,
        "latency": 5950,
        "framework": "SL",
    },
    {
        "date": "2024-11-19",
        "input_rate_records_per_second": 500,
        "cpu": 0.09,
        "throughput": 400,
        "latency": 6000,
        "framework": "SL",
    },
    {
        "date": "2024-11-20",
        "input_rate_records_per_second": 500,
        "cpu": 0.12,
        "throughput": 430,
        "latency": 5900,
        "framework": "SL",
    },
]
list_sf = list_sf = [
    {
        "date": "2024-11-11",
        "input_rate_records_per_second": 490,
        "cpu": 0.11,
        "throughput": 410,
        "latency": 6100,
        "framework": "SF",
    },
    {
        "date": "2024-11-12",
        "input_rate_records_per_second": 515,
        "cpu": 0.13,
        "throughput": 425,
        "latency": 6000,
        "framework": "SF",
    },
    {
        "date": "2024-11-13",
        "input_rate_records_per_second": 470,
        "cpu": 0.1,
        "throughput": 390,
        "latency": 6200,
        "framework": "SF",
    },
    {
        "date": "2024-11-14",
        "input_rate_records_per_second": 500,
        "cpu": 0.12,
        "throughput": 415,
        "latency": 6050,
        "framework": "SF",
    },
    {
        "date": "2024-11-15",
        "input_rate_records_per_second": 485,
        "cpu": 0.09,
        "throughput": 405,
        "latency": 6150,
        "framework": "SF",
    },
    {
        "date": "2024-11-16",
        "input_rate_records_per_second": 520,
        "cpu": 0.14,
        "throughput": 435,
        "latency": 5950,
        "framework": "SF",
    },
    {
        "date": "2024-11-17",
        "input_rate_records_per_second": 465,
        "cpu": 0.08,
        "throughput": 380,
        "latency": 6250,
        "framework": "SF",
    },
    {
        "date": "2024-11-18",
        "input_rate_records_per_second": 510,
        "cpu": 0.11,
        "throughput": 420,
        "latency": 6000,
        "framework": "SF",
    },
    {
        "date": "2024-11-19",
        "input_rate_records_per_second": 480,
        "cpu": 0.1,
        "throughput": 400,
        "latency": 6050,
        "framework": "SF",
    },
    {
        "date": "2024-11-20",
        "input_rate_records_per_second": 525,
        "cpu": 0.13,
        "throughput": 430,
        "latency": 5950,
        "framework": "SF",
    },
]


@pytest.fixture
def sample_data():
    date_range = pd.date_range(start="2023-01-01", periods=100, freq="30s")
    data = pd.Series(range(100), index=date_range)
    return data


@pytest.fixture
def predictor():
    return LoadPredictor()


@pytest.mark.skip(reason="SARIMAX method have been removed")
def test_load_predictor_model_creation_SARIMAX(predictor, sample_data):
    model = predictor.make_model_sarimax(sample_data)
    assert model is not None, "SARIMAX model creation failed."


@pytest.mark.skip(reason="SARIMAX method have been removed")
def test_load_predictor_predictions_SARIMAX(predictor, sample_data):
    model = predictor.make_model_sarimax(sample_data)
    predictions = predictor.make_predictions_sarimax(model, forecast_periods=5)
    assert len(predictions) == 5, "Prediction generation failed; expected 5 values."


def test_load_predictor_model_creation_ARIMA(predictor, sample_data):
    model = predictor.make_model_arima(sample_data)
    assert model is not None, "SARIMAX model creation failed."


def test_load_predictor_predictions_ARIMA(predictor, sample_data):
    model = predictor.make_model_arima(sample_data)
    predictions = predictor.make_predictions_arima(5)
    assert len(predictions) == 5, "Prediction generation failed; expected 5 values."


def test_load_predictor_model_creation_auto_arima(predictor, sample_data):
    assert predictor.make_model_auto_arima(
        sample_data
    ), "auto_arima model creation failed."
    assert (
        predictor.is_model_set
    ), "Model was not set properly after auto_arima creation."


def test_load_predictor_predictions_auto_arima(predictor, sample_data):
    predictor.make_model_auto_arima(sample_data)
    predictions = predictor.make_predictions_auto_arima(5)
    assert len(predictions) == 5, "Prediction generation failed; expected 5 values."


def test_load_predictor_update_auto_arima(predictor, sample_data):
    predictor.make_model_auto_arima(sample_data)
    assert predictor.update_auto_arima(
        [505, 495, 500]
    ), "Updating auto_arima model failed."


def test_last_update_timestamp_setter_getter(predictor):
    timestamp = datetime.datetime(2024, 11, 20)
    predictor.last_update_timestamp = timestamp
    assert (
        predictor.last_update_timestamp == timestamp
    ), "Getter/Setter for last_update_timestamp failed."
    with pytest.raises(
        ValueError, match="last_update_timestamp must be a datetime object or None"
    ):
        predictor.last_update_timestamp = "InvalidType"
