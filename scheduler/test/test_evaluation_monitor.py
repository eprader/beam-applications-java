import pytest
from unittest.mock import patch, MagicMock, call
import threading
from datetime import datetime
import utils.Utils
from scheduler_logic.evaluation_monitor import (
    EvaluationMonitor,
)


@pytest.fixture
def evaluation_monitor():
    event = threading.Event()
    framework_running = threading.Event()
    return EvaluationMonitor(
        running_framework=utils.Utils.Framework.SF,
        evaluation_event=event,
        framework_running_event=framework_running,
        application="TRAIN",
        dataset="FIT",
        threshold_dict_sf = {"idleTime": 990, "busyTime": 10},
        threshold_dict_sl = {"busyTime": 800, "backPressuredTime": 800},
        periodic_checking_min=1,
        timeout_duration_min=5,
        sleep_interval_seconds=30,
    )


@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf",
    return_value="sf_objectives",autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value="sf_critical_metrics",autospec=True
)
def test_collect_metrics_sf(mock_get_objectives, mock_get_critical, evaluation_monitor):
    objectives, critical_metrics = evaluation_monitor.collect_metrics()
    assert objectives == "sf_objectives"
    assert critical_metrics == "sf_critical_metrics"
    mock_get_objectives.assert_called_once()
    mock_get_critical.assert_called_once()


@patch(
    "scheduler_logic.evaluation_monitor.scheduler_logic.scheduler_logic.run_evaluation",autospec=True,
    return_value=utils.Utils.Framework.SL,
)
@patch(
    "scheduler_logic.evaluation_monitor.database.database_access.store_decision_in_db",autospec=True
)
def test_evaluate_and_act_switch(
    mock_store_decision, mock_run_evaluation, evaluation_monitor
):
    with patch.object(
        evaluation_monitor.evaluation_event, "is_set", return_value=False
    ):
        assert evaluation_monitor.evaluate_and_act() is True
        mock_store_decision.assert_called_once_with(utils.Utils.Framework.SL)
        assert evaluation_monitor.running_framework == utils.Utils.Framework.SL


@patch(
    "scheduler_logic.evaluation_monitor.scheduler_logic.scheduler_logic.run_evaluation",autospec=True,
    return_value=utils.Utils.Framework.SF,
)
@patch(
    "scheduler_logic.evaluation_monitor.database.database_access.store_decision_in_db",autospec=True
)
def test_evaluate_and_act_no_switch(
    mock_store_decision, mock_run_evaluation, evaluation_monitor
):
    with patch.object(
        evaluation_monitor.evaluation_event, "is_set", return_value=False
    ):
        assert evaluation_monitor.evaluate_and_act() is False
        mock_store_decision.assert_called_once_with(utils.Utils.Framework.SF)


@patch("scheduler_logic.evaluation_monitor.time.sleep", return_value=None,autospec=True)
@patch(
    "scheduler_logic.evaluation_monitor.database.database_access.store_decision_in_db",autospec=True
)
def test_handle_switch(mock_store_decision, mock_sleep, evaluation_monitor):
    with patch.object(
        evaluation_monitor.evaluation_event, "is_set", return_value=False
    ):
        evaluation_monitor.handle_switch(utils.Utils.Framework.SL)
        assert mock_store_decision.call_count == 1
        assert evaluation_monitor.running_framework == utils.Utils.Framework.SL


@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf",
    return_value={"latency":500, "cpu_load":0.2, "throughput":500},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value={"idleTime":500, "busyTime":400},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
    return_value=False,autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    return_value=False,autospec=True
)
@patch("mysql.connector.connect")
def test_monitor_iteration_no_safety_net_no_act(
    mock_connect,
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
    return_value={"latency":500, "cpu_load":0.2, "throughput":500},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value={"idleTime":500, "busyTime":400},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
    return_value=False,autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    return_value=False,autospec=True
)
@patch("mysql.connector.connect")
def test_monitor_iteration_periodic_check_without_safety_net(
    mock_connect,
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
    return_value={"latency":500, "cpu_load":0.2, "throughput":500},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value={"idleTime":500, "busyTime":400},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
    side_effect=[True, False, False],autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    return_value=False,autospec=True
)
@patch("mysql.connector.connect")
def test_monitor_iteration_periodic_check_with_safety_net(
    mock_connect,
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
    return_value={"latency":500, "cpu_load":0.2, "throughput":500},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value={"idleTime":500, "busyTime":400},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
    side_effect=[True, False, True, False],autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    return_value=False,autospec=True
)
@patch("mysql.connector.connect")
def test_monitor_iteration_periodic_check_with_safety_net_two_times(
    mock_connect,
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
    return_value={"latency":500, "cpu_load":0.2, "throughput":500},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value={"idleTime":500, "busyTime":400},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
    side_effect=[True] + [False] * 12,autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    side_effect=[True] + [False] * 12,autospec=True
)
@patch("mysql.connector.connect")
def test_monitor_iteration_periodic_check_timeout_from_safety_net(
    mock_connect,
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
    return_value={"latency":500, "cpu_load":0.2, "throughput":500},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value={"idleTime":500, "busyTime":400},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
    side_effect=[False] * 13,autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    side_effect=[True, False, False] + [False] * 10,autospec=True
)
@patch("mysql.connector.connect")
def test_monitor_iteration_periodic_check_timeout_from_periodic(
    mock_connect,
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
    assert evaluation_monitor.periodic_counter == 1
    assert mock_get_objectives.call_count == 13
    assert mock_get_critical_metrics.call_count == 13
    assert mock_check_for_safety_net.call_count == 13
    assert mock_evaluate_and_act.call_count == 1


@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_objectives_for_sf",
    return_value={"latency":500, "cpu_load":0.2, "throughput":500},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value={"idleTime":500, "busyTime":400},autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.check_for_safety_net",
    side_effect=[False] * 15,autospec=True
)
@patch(
    "scheduler_logic.evaluation_monitor.EvaluationMonitor.evaluate_and_act",
    side_effect=[True, False, False] + [False] * 12,autospec=True
)
@patch("mysql.connector.connect")
def test_monitor_iteration_periodic_check_timeout_from_periodic_second_periodic_check(
    mock_connect,
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

    assert evaluation_monitor.timeout_counter == 0
    assert evaluation_monitor.periodic_counter == 0
    assert mock_get_objectives.call_count == 15
    assert mock_get_critical_metrics.call_count == 15
    assert mock_check_for_safety_net.call_count == 15
    assert mock_evaluate_and_act.call_count == 2

@patch(
    "metrics.metrics_collector.get_objectives_for_sl",
    return_value={"sl_objective": 400},autospec=True
)
@patch(
    "metrics.metrics_collector.get_critical_metrics_for_sl",
    return_value={"sl_metric": 300},autospec=True
)
@patch(
    "metrics.metrics_collector.get_objectives_for_sf",
    return_value={"sf_objective": 200},autospec=True
)
@patch(
    "metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value={"sf_metric": 100},autospec=True
)
def test_collect_metrics_sf(
    mock_crit_for_sf,
    mock_obj_for_sf,
    mock_crit_for_sl,
    mock_obj_for_sl,
    evaluation_monitor,
):
    objectives, critical_metrics = evaluation_monitor.collect_metrics()
    assert objectives == {"sf_objective": 200}
    assert critical_metrics == {"sf_metric": 100}

    mock_crit_for_sf.assert_called_once()
    mock_obj_for_sf.assert_called_once()

    mock_obj_for_sl.assert_not_called()
    mock_crit_for_sl.assert_not_called()


@patch(
    "metrics.metrics_collector.get_objectives_for_sl",
    return_value={"sl_objective": 400},autospec=True
)
@patch(
    "metrics.metrics_collector.get_critical_metrics_for_sl",
    return_value={"sl_metric": 300},autospec=True
)
@patch(
    "metrics.metrics_collector.get_objectives_for_sf",
    return_value={"sf_objective": 200},autospec=True
)
@patch(
    "metrics.metrics_collector.get_critical_metrics_for_sf",
    return_value={"sf_metric": 100},autospec=True
)
def test_collect_metrics_sl(
    mock_crit_for_sf,
    mock_obj_for_sf,
    mock_crit_for_sl,
    mock_obj_for_sl,
    evaluation_monitor,
):
    evaluation_monitor.running_framework = utils.Utils.Framework.SL
    objectives, critical_metrics = evaluation_monitor.collect_metrics()
    assert objectives == {"sl_objective": 400}
    assert critical_metrics == {"sl_metric": 300}

    mock_crit_for_sf.assert_not_called()
    mock_obj_for_sf.assert_not_called()

    mock_obj_for_sl.assert_called_once()
    mock_crit_for_sl.assert_called_once()


@patch("scheduler_logic.scheduler_logic.run_evaluation",autospec=True)
@patch("database.database_access.store_decision_in_db",autospec=True)
@patch("time.sleep", return_value=None)
def test_evaluate_and_act_switch(
    mock_time, mock_store_decision, mock_run_evaluation, evaluation_monitor
):
    mock_run_evaluation.return_value = utils.Utils.Framework.SL

    with patch.object(
        evaluation_monitor.evaluation_event, "is_set", side_effect=[True, False]
    ):
        result = evaluation_monitor.evaluate_and_act()
        assert result is True
        mock_store_decision.assert_called_once()
        assert evaluation_monitor.running_framework == utils.Utils.Framework.SL


@patch("scheduler_logic.scheduler_logic.run_evaluation",autospec=True)
@patch("database.database_access.store_decision_in_db",autospec=True)
@patch("time.sleep", return_value=None)
def test_evaluate_and_act_no_switch(
    mock_time, mock_store_decision, mock_run_evaluation, evaluation_monitor
):
    mock_run_evaluation.return_value = utils.Utils.Framework.SF

    with patch.object(
        evaluation_monitor.evaluation_event, "is_set", side_effect=[True, False]
    ):
        result = evaluation_monitor.evaluate_and_act()
        assert result is False
        assert evaluation_monitor.running_framework == utils.Utils.Framework.SF

@patch("scheduler_logic.evaluation_monitor.logging")
def test_check_for_safety_net_sf_idle_exceeds_threshold(mock_logging, evaluation_monitor):
    evaluation_monitor.running_framework = utils.Utils.Framework.SF
    metrics_dic = {"idleTime": 995, "busyTime": 250}
    assert evaluation_monitor.check_for_safety_net(metrics_dic) is True

@patch("scheduler_logic.evaluation_monitor.logging")
def test_check_for_safety_net_sf_busy_below_threshold(mock_logging, evaluation_monitor):
    evaluation_monitor.running_framework = utils.Utils.Framework.SF
    metrics_dic = {"idleTime": 400, "busyTime": 7}
    assert evaluation_monitor.check_for_safety_net(metrics_dic) is True

@patch("scheduler_logic.evaluation_monitor.logging")
def test_check_for_safety_net_sf_no_threshold_violation(mock_logging, evaluation_monitor):
    evaluation_monitor.running_framework = utils.Utils.Framework.SF
    metrics_dic = {"idleTime": 400, "busyTime": 250}
    assert evaluation_monitor.check_for_safety_net(metrics_dic) is False

@patch("scheduler_logic.evaluation_monitor.logging")
def test_check_for_safety_net_sl_exceeds_threshold(mock_logging, evaluation_monitor):
    evaluation_monitor.running_framework = utils.Utils.Framework.SL
    metrics_dic = {"busyTime": 350, "backPressuredTime": 1000} 
    assert evaluation_monitor.check_for_safety_net(metrics_dic) is True

@patch("scheduler_logic.evaluation_monitor.logging")
def test_check_for_safety_net_sl_no_threshold_violation(mock_logging, evaluation_monitor):
    evaluation_monitor.running_framework = utils.Utils.Framework.SL
    metrics_dic = {"busyTime": 250, "backPressuredTime": 20}
    assert evaluation_monitor.check_for_safety_net(metrics_dic) is False

@patch("scheduler_logic.evaluation_monitor.logging")
def test_check_for_safety_net_exception_handling(mock_logging, evaluation_monitor):
    evaluation_monitor.running_framework = utils.Utils.Framework.SF
    metrics_dic = {"someMetric": 1000}
    assert evaluation_monitor.check_for_safety_net(metrics_dic) is False

