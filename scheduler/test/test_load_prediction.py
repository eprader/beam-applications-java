import pytest
import pandas as pd
from timeseriesPredictor.LoadPredictor import LoadPredictor


@pytest.fixture
def sample_data():
    date_range = pd.date_range(start="2023-01-01", periods=100, freq="30s")
    data = pd.Series(range(100), index=date_range)
    return data

@pytest.mark.skip(reason="SARIMAX method have been removed")
def test_load_predictor_model_creation_SARIMAX(sample_data):
    predictor = LoadPredictor()
    model = predictor.make_model_sarimax(sample_data)
    assert model is not None, "SARIMAX model creation failed."


@pytest.mark.skip(reason="SARIMAX method have been removed")
def test_load_predictor_predictions_SARIMAX(sample_data):
    predictor = LoadPredictor()
    model = predictor.make_model_sarimax(sample_data)
    predictions = predictor.make_predictions_sarimax(model, forecast_periods=5)
    assert len(predictions) == 5, "Prediction generation failed; expected 5 values."


def test_load_predictor_model_creation_ARIMA(sample_data):
    predictor = LoadPredictor()
    model = predictor.make_model_arima(sample_data)
    assert model is not None, "SARIMAX model creation failed."


def test_load_predictor_predictions_ARIMA(sample_data):
    predictor = LoadPredictor()
    model = predictor.make_model_arima(sample_data)
    predictions = predictor.make_predictions_arima(5)
    assert len(predictions) == 5, "Prediction generation failed; expected 5 values."
