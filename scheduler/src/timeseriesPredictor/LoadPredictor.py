from statsmodels.tsa.statespace.sarimax import SARIMAX, SARIMAXResults
from statsmodels.tsa.arima.model import ARIMA
import pickle
import database.database_access
import logging
import pandas as pd
import matplotlib.pyplot as plt


class LoadPredictor:
    def __init__(self):
        self.model = None

    def make_model_sarimax(self, grouped_df):
        grouped_df = grouped_df.asfreq("30s")
        order = (4, 1, 0)
        seasonal_order = (1, 1, 0, 24)
        model = SARIMAX(
            endog=grouped_df, order=order, seasonal_order=seasonal_order, freq="30s"
        )
        self.model = model.fit()
        return model

    def make_model_arima(self, history):
        model = ARIMA(history, order=(5, 1, 0))
        self.model = model.fit()
        return model

    def make_predictions_arima(self, model, forecast_periods=5):
        output = model.forecast(forecast_periods)
        return output

    def make_predictions_sarimax(self, model, forecast_periods=5):
        # return model.predict(forecast_periods,return_conf_int=False)
        start = len(model.data.endog)
        end = start + forecast_periods - 1
        return model.predict(start=start, end=end)

    def save_model_to_database(self):
        binary_model = pickle.dumps(self.model)
        database.database_access.store_model_in_database("loadpredictor", binary_model)

    def load_model_from_database(self, model_name="loadpredictor"):
        try:
            binary_model = database.database_access.load_model_from_database(model_name)
            self.model = pickle.loads(binary_model)
            if not isinstance(self.model, SARIMAXResults):
                raise ValueError("Loaded object is not a valid SARIMAX model.")
            logging.info(f"Model '{model_name}' loaded successfully.")
        except Exception as e:
            logging.error(f"Error loading model '{model_name}': {e}")
            self.model = None
