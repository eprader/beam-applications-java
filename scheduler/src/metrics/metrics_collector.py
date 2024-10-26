import logging
import requests
import sys

def read_current_metric_from_prometheus(metric_name: str):
    prometheus_url = "http://prometheus-operated.default.svc.cluster.local:9090"
    try:
        response = requests.get(
            f"{prometheus_url}/api/v1/query",
            params={"query": metric_name},
        )
        data = response.json()
        value = data["data"]["result"][0]["value"][1]
        return int(value)
    except Exception as e:
        logging.error(f"Error, when reading from prometheus: {e}")
        return None

# FIXME
def get_critical_metrics_for_sf():
    pass

# FIXME
def get_critical_metrics_for_sl():
    pass

# FIXME
def get_objectives_for_sf():
    pass

# FIXME
def get_objectives_for_sl():
    pass