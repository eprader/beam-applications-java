import pytest
from unittest.mock import patch, MagicMock, call
import threading
import utils.Utils
from scheduler_logic.evaluation_monitor import (
    EvaluationMonitor,
)  # Adjust the import based on your file structure


@pytest.fixture
def evaluation_monitor():
    event = threading.Event()
    return EvaluationMonitor(
        running_framework=utils.Utils.Framework.SF,
        evaluation_event=event,
        periodic_checking_min=1,
        timeout_duration_min=5,
        sleep_interval_seconds=30,
    )


@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf",
    return_value="sf_objectives",
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value="sf_critical_metrics",
)
def test_collect_metrics_sf(mock_get_objectives, mock_get_critical, evaluation_monitor):
    objectives, critical_metrics = evaluation_monitor.collect_metrics()
    assert objectives == "sf_objectives"
    assert critical_metrics == "sf_critical_metrics"
    mock_get_objectives.assert_called_once()
    mock_get_critical.assert_called_once()


@patch(
    "scheduler_logic.evaluation_monitor.scheduler_logic.scheduler_logic.run_evaluation",
    return_value=utils.Utils.Framework.SL,
)
@patch(
    "scheduler_logic.evaluation_monitor.database.database_access.store_decision_in_db"
)
def test_evaluate_and_act_switch(
    mock_store_decision, mock_run_evaluation, evaluation_monitor
):
    with patch.object(
        evaluation_monitor.evaluation_event, "is_set", return_value=False
    ):
        assert (
            evaluation_monitor.evaluate_and_act() is True
        )
        mock_store_decision.assert_called_once_with(utils.Utils.Framework.SL)
        assert evaluation_monitor.running_framework == utils.Utils.Framework.SL


@patch(
    "scheduler_logic.evaluation_monitor.scheduler_logic.scheduler_logic.run_evaluation",
    return_value=utils.Utils.Framework.SF,
)
@patch(
    "scheduler_logic.evaluation_monitor.database.database_access.store_decision_in_db"
)
def test_evaluate_and_act_no_switch(
    mock_store_decision, mock_run_evaluation, evaluation_monitor
):
    with patch.object(
        evaluation_monitor.evaluation_event, "is_set", return_value=False
    ):
        assert evaluation_monitor.evaluate_and_act() is False
        mock_store_decision.assert_called_once_with(utils.Utils.Framework.SF)


@patch("scheduler_logic.evaluation_monitor.time.sleep", return_value=None)
@patch(
    "scheduler_logic.evaluation_monitor.database.database_access.store_decision_in_db"
)
def test_handle_switch(mock_store_decision, mock_sleep, evaluation_monitor):
    with patch.object(
        evaluation_monitor.evaluation_event, "is_set", return_value=False
    ):
        evaluation_monitor.handle_switch(utils.Utils.Framework.SL)
        assert mock_store_decision.call_count == 1
        assert evaluation_monitor.running_framework == utils.Utils.Framework.SL


# FIXME: Test counting interval logic


@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf",
    return_value="mock_objectives",
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value="mock_critical_metrics",
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
    return_value=False,
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    return_value=False,
)
def test_monitor_iteration_no_safety_net_no_act(
    mock_evaluate_and_act,
    mock_check_for_safety_net,
    mock_get_critical_metrics,
    mock_get_objectives,
    evaluation_monitor,
):
    evaluation_monitor.monitor_iteration(periodic_checks=2)

    assert evaluation_monitor.timeout_counter == 0
    assert evaluation_monitor.periodic_counter == 1
    mock_get_objectives.assert_called_once()
    mock_get_critical_metrics.assert_called_once()
    mock_check_for_safety_net.assert_called_once()
    mock_evaluate_and_act.assert_not_called()


@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf",
    return_value="mock_objectives",
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value="mock_critical_metrics",
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
    return_value=False,
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    return_value=False,
)
def test_monitor_iteration_periodic_check_without_safety_net(
    mock_evaluate_and_act,
    mock_check_for_safety_net,
    mock_get_critical_metrics,
    mock_get_objectives,
    evaluation_monitor,
):
    periodic_check = (
        evaluation_monitor.interval_seconds / evaluation_monitor.sleep_interval
    )
    for i in range(3):
        evaluation_monitor.monitor_iteration(periodic_checks=periodic_check)
    
    assert evaluation_monitor.timeout_counter == 0
    assert evaluation_monitor.periodic_counter == 0
    assert mock_get_objectives.call_count == 3
    assert mock_get_critical_metrics.call_count == 3
    assert mock_check_for_safety_net.call_count == 3
    mock_evaluate_and_act.assert_called_once()



@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf",
    return_value="mock_objectives",
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value="mock_critical_metrics",
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
   side_effect=[True, False, False]
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    return_value=False,
)
def test_monitor_iteration_periodic_check_with_safety_net(
    mock_evaluate_and_act,
    mock_check_for_safety_net,
    mock_get_critical_metrics,
    mock_get_objectives,
    evaluation_monitor,
):
    periodic_check = (
        evaluation_monitor.interval_seconds / evaluation_monitor.sleep_interval
    )
    for i in range(3):
        evaluation_monitor.monitor_iteration(periodic_checks=periodic_check)
    
    assert evaluation_monitor.timeout_counter == 0
    assert evaluation_monitor.periodic_counter == 0
    assert mock_get_objectives.call_count == 3
    assert mock_get_critical_metrics.call_count == 3
    assert mock_check_for_safety_net.call_count == 3
    assert mock_evaluate_and_act.call_count == 2



@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf",
    return_value="mock_objectives",
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value="mock_critical_metrics",
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
   side_effect=[True, False, True, False]
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    return_value=False,
)
def test_monitor_iteration_periodic_check_with_safety_net_two_times(
    mock_evaluate_and_act,
    mock_check_for_safety_net,
    mock_get_critical_metrics,
    mock_get_objectives,
    evaluation_monitor,
):
    periodic_check = (
        evaluation_monitor.interval_seconds / evaluation_monitor.sleep_interval
    )
    for i in range(4):
        evaluation_monitor.monitor_iteration(periodic_checks=periodic_check)
    
    assert evaluation_monitor.timeout_counter == 0
    assert evaluation_monitor.periodic_counter == 0
    assert mock_get_objectives.call_count == 4
    assert mock_get_critical_metrics.call_count == 4
    assert mock_check_for_safety_net.call_count == 4
    assert mock_evaluate_and_act.call_count == 3


@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf",
    return_value="mock_objectives",
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value="mock_critical_metrics",
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
   side_effect=[True] +[False]*12
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    side_effect=[True]+[False]*12
)
def test_monitor_iteration_periodic_check_timeout_from_safety_net(
    mock_evaluate_and_act,
    mock_check_for_safety_net,
    mock_get_critical_metrics,
    mock_get_objectives,
    evaluation_monitor,
):
    periodic_check = (
        evaluation_monitor.interval_seconds / evaluation_monitor.sleep_interval
    )
    for i in range(13):
        evaluation_monitor.monitor_iteration(periodic_checks=periodic_check)
    
    assert evaluation_monitor.timeout_counter == 0
    assert evaluation_monitor.periodic_counter == 0
    assert mock_get_objectives.call_count == 13
    assert mock_get_critical_metrics.call_count == 13
    assert mock_check_for_safety_net.call_count == 13
    assert mock_evaluate_and_act.call_count == 2


@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf",
    return_value="mock_objectives",
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value="mock_critical_metrics",
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
   side_effect=[False]*13
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    side_effect=[True, False, False]+[False]*10
)
def test_monitor_iteration_periodic_check_timeout_from_periodic(
    mock_evaluate_and_act,
    mock_check_for_safety_net,
    mock_get_critical_metrics,
    mock_get_objectives,
    evaluation_monitor,
):
    periodic_check = (
        evaluation_monitor.interval_seconds / evaluation_monitor.sleep_interval
    )
    for i in range(13):
        evaluation_monitor.monitor_iteration(periodic_checks=periodic_check)
        print(evaluation_monitor.periodic_counter)
    
    assert evaluation_monitor.timeout_counter == 0
    assert evaluation_monitor.periodic_counter == 1
    assert mock_get_objectives.call_count == 13
    assert mock_get_critical_metrics.call_count == 13
    assert mock_check_for_safety_net.call_count == 13
    assert mock_evaluate_and_act.call_count == 1

@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf",
    return_value="mock_objectives",
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value="mock_critical_metrics",
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
   side_effect=[False]*15
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    side_effect=[True, False, False]+[False]*12
)
def test_monitor_iteration_periodic_check_timeout_from_periodic_second_periodic_check(
    mock_evaluate_and_act,
    mock_check_for_safety_net,
    mock_get_critical_metrics,
    mock_get_objectives,
    evaluation_monitor,
):
    periodic_check = (
        evaluation_monitor.interval_seconds / evaluation_monitor.sleep_interval
    )
    for i in range(15):
        evaluation_monitor.monitor_iteration(periodic_checks=periodic_check)
        print(evaluation_monitor.periodic_counter)
    
    assert evaluation_monitor.timeout_counter == 0
    assert evaluation_monitor.periodic_counter == 0
    assert mock_get_objectives.call_count == 15
    assert mock_get_critical_metrics.call_count == 15
    assert mock_check_for_safety_net.call_count == 15
    assert mock_evaluate_and_act.call_count == 2






    

