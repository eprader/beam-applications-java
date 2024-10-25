from statsmodels.tsa.statespace.sarimax import SARIMAX


class LoadPredictor:
    def __init__(self):
        pass

    def make_model_sarimax(self, grouped_df):
        grouped_df = grouped_df.asfreq("30s")
        order = (4, 1, 0)
        seasonal_order = (1, 1, 0, 24)
        model = SARIMAX(
            endog=grouped_df, order=order, seasonal_order=seasonal_order, freq="30s"
        )
        model = model.fit()
        return model

    def make_predictions_sarimax(self, model, forecast_periods=5):
        # return model.predict(forecast_periods,return_conf_int=False)
        start = len(model.data.endog)
        end = start + forecast_periods - 1
        return model.predict(start=start, end=end)

    def update_model(value):
        raise NotImplementedError
