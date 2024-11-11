from statsmodels.tsa.arima.model import ARIMA
import logging
import pmdarima
from datetime import datetime


class LoadPredictor:
    def __init__(self):
        self.model = None
        self.last_update_timestamp = None

    def make_model_arima(self, history):
        try:
            model = ARIMA(history, order=(5, 1, 0))
            self.model = model.fit()
            return True
        except Exception as e:
            logging.error("Error when creating ARIMA model", e)
            return False

    def make_predictions_arima(self, periods):
        try:
            if self.model == None:
                raise Exception("Model is None")
            output = self.model.forecast(steps=periods)
            return output
        except Exception as e:
            logging.error("Error when predicting" + str(e))
            return []

    def make_model_auto_arima(self, history):
        try:
            self.model = pmdarima.auto_arima(
                history,
                start_p=1,
                start_q=0,
                d=0,
                max_p=5,
                max_q=5,
                suppress_warnings=True,
                stepwise=True,
                error_action="ignore",
            )
            return True
        except Exception as e:
            logging.error("Error when creating auto_ARIMA model", e)
            return False

    def make_predictions_auto_arima(self, periods):
        try:
            if self.model == None:
                raise Exception("Model is None")
            output = self.model.predict(n_periods=periods)
            return output
        except Exception as e:
            logging.error("Error when predicting auto_arima" + str(e))
            return []

    def update_auto_arima(self, data):
        try:
            self.model.update(data)
            return True
        except Exception as e:
            logging.error("Error when updating auto_ARIMA model", e)
            return False

    @property
    def is_model_set(self):
        return self.model != None

    @property
    def last_update_timestamp(self):
        return self._last_update_timestamp

    @last_update_timestamp.setter
    def last_update_timestamp(self, value):
        if value is None or isinstance(value, datetime):
            self._last_update_timestamp = value
        else:
            logging.error(
                "Invalid type for last_update_timestamp. Must be datetime or None."
            )
            raise ValueError("last_update_timestamp must be a datetime object or None")


def main():
    history = [
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
    values_list = [entry["input_rate_records_per_second"] for entry in history]
    test_instance = LoadPredictor()
    test_instance.make_model_auto_arima(values_list)
    print("Test")
    print(test_instance.make_predictions_auto_arima(5))


if __name__ == "__main__":
    main()
