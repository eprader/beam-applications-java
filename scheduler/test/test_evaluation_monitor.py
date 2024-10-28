import pytest
from unittest.mock import patch, MagicMock, call
import threading
import utils.Utils
from scheduler_logic.evaluation_monitor import EvaluationMonitor  # Adjust the import based on your file structure


@pytest.fixture
def evaluation_monitor():
    event = threading.Event()
    return EvaluationMonitor(
        running_framework=utils.Utils.Framework.SF,
        evaluation_event=event,
        periodic_checking_min=1,
        timeout_duration_min=10,
        sleep_interval_seconds=30,
    )


@patch("scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf", return_value="sf_objectives")
@patch("scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf", return_value="sf_critical_metrics")
def test_collect_metrics_sf(mock_get_objectives, mock_get_critical, evaluation_monitor):
    objectives, critical_metrics = evaluation_monitor.collect_metrics()
    assert objectives == "sf_objectives"
    assert critical_metrics == "sf_critical_metrics"
    mock_get_objectives.assert_called_once()
    mock_get_critical.assert_called_once()


@patch("scheduler_logic.evaluation_monitor.scheduler_logic.scheduler_logic.run_evaluation", return_value=utils.Utils.Framework.SL)
@patch("scheduler_logic.evaluation_monitor.database.database_access.store_decision_in_db")
def test_evaluate_and_act_switch(mock_store_decision, mock_run_evaluation, evaluation_monitor):
    with patch.object(evaluation_monitor.evaluation_event, "is_set", return_value=False):
        assert evaluation_monitor.evaluate_and_act() is True  # Expected to switch and return True
        mock_store_decision.assert_called_once_with(utils.Utils.Framework.SL)
        assert evaluation_monitor.running_framework == utils.Utils.Framework.SL


@patch("scheduler_logic.evaluation_monitor.scheduler_logic.scheduler_logic.run_evaluation", return_value=utils.Utils.Framework.SF)
@patch("scheduler_logic.evaluation_monitor.database.database_access.store_decision_in_db")
def test_evaluate_and_act_no_switch(mock_store_decision, mock_run_evaluation, evaluation_monitor):
    with patch.object(evaluation_monitor.evaluation_event, "is_set", return_value=False):
        assert evaluation_monitor.evaluate_and_act() is False
        mock_store_decision.assert_called_once_with(utils.Utils.Framework.SF)


@patch("scheduler_logic.evaluation_monitor.time.sleep", return_value=None)
@patch("scheduler_logic.evaluation_monitor.database.database_access.store_decision_in_db")
def test_handle_switch(mock_store_decision, mock_sleep, evaluation_monitor):
    with patch.object(evaluation_monitor.evaluation_event, "is_set", return_value=False):
        evaluation_monitor.handle_switch(utils.Utils.Framework.SL)
        assert mock_store_decision.call_count == 1
        assert evaluation_monitor.running_framework == utils.Utils.Framework.SL


#FIXME: Test counting interval logic