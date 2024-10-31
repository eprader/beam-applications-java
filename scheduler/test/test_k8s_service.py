import pytest
from unittest.mock import patch, MagicMock, mock_open
import framework_scheduling.kubernetes_service as k8s_service
import utils.Utils


def test_delete_all_jobs_from_serverful_framework():
    with patch(
        "framework_scheduling.kubernetes_service.get_jobid_of_running_job",
        autospec=True,
        return_value=["job1", "job2"],
    ), patch(
        "framework_scheduling.kubernetes_service.stop_flink_job", autospec=True
    ) as mock_stop:

        k8s_service.delete_all_jobs_from_serverful_framework()
        mock_stop.assert_any_call("flink-session-cluster-rest", "job1")
        mock_stop.assert_any_call("flink-session-cluster-rest", "job2")


def test_is_flink_deployment_ready():
    mock_k8s_api = MagicMock()
    mock_k8s_api.get_namespaced_custom_object.return_value = {
        "status": {"jobManagerDeploymentStatus": "READY"}
    }
    assert k8s_service.is_flink_deployment_ready(mock_k8s_api, "flink-app")


def test_wait_for_flink_deployment():
    with patch("time.sleep", return_value=None), patch(
        "framework_scheduling.kubernetes_service.is_flink_deployment_ready",
        return_value=True,
        autospec=True,
    ):

        mock_k8s_api = MagicMock()
        assert k8s_service.wait_for_flink_deployment(mock_k8s_api, "flink-app")


def test_start_flink_deployment():
    mock_k8s_api = MagicMock()
    with patch(
        "framework_scheduling.kubernetes_service.client.CustomObjectsApi",
        return_value=mock_k8s_api,
        autospec=True,
    ), patch(
        "framework_scheduling.kubernetes_service.wait_for_flink_deployment",
        return_value=True,
        autospec=True,
    ), patch(
        "framework_scheduling.kubernetes_service.config.load_incluster_config"
    ):

        path_manifest = [
            {"kind": "FlinkDeployment", "metadata": {"name": "test-deployment"}}
        ]
        k8s_service.start_flink_deployment(path_manifest)


"""def test_submit_flink_job():
    job_jar_path = "/path/to/nonexistent.jar"
    job_manager_host = "flink-session-cluster-rest"
    database_url = "mock_db_url"
    experiment_run_id = "FIT-120"
    
    # Mock the open function to simulate opening a file
    with patch("builtins.open", mock_open(read_data=b"mock jar file data")), \
         patch("requests.post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200, json=lambda: {"filename": "jar/test.jar"})
        
        try:
            k8s_service.submit_flink_job(job_jar_path, job_manager_host, database_url, experiment_run_id)
        except Exception as e:
            pytest.fail(f"submit_flink_job raised an exception: {e}")

        mock_post.assert_any_call(
            f"http://{job_manager_host}:8081/jars/upload",
            files={"jarfile": mock_open(read_data=b"mock jar file data").return_value}
        )

        mock_post.assert_any_call(
            f"http://{job_manager_host}:8081/jars/test.jar/run",
            json={
                "programArgs": f"--databaseUrl={database_url} --experiRunId={experiment_run_id} --streaming --operatorChaining=false"
            }
        )"""


def test_get_jobid_of_running_job():
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"jobs": [{"id": "job1", "status": "RUNNING"}]}
    with patch("requests.get", return_value=mock_response):
        running_jobs = k8s_service.get_jobid_of_running_job("localhost")
        assert running_jobs == ["job1"]


def test_stop_flink_job():
    mock_response = MagicMock(status_code=200)
    with patch("requests.get", return_value=mock_response):
        try:
            k8s_service.stop_flink_job("localhost", "job-id")
        except Exception as e:
            pytest.fail(f"stop_flink_job raised an exception: {e}")


def test_start_deployment_and_service():
    with patch("framework_scheduling.kubernetes_service.client.CoreV1Api"), patch(
        "framework_scheduling.kubernetes_service.client.AppsV1Api"
    ), patch(
        "framework_scheduling.kubernetes_service.wait_for_deployment_and_service",
        autospec=True,
        return_value=True,
    ), patch(
        "framework_scheduling.kubernetes_service.wait_for_deployment",
        autospec=True,
        return_value=True,
    ), patch(
        "framework_scheduling.kubernetes_service.config.load_incluster_config"
    ):

        path_manifest = [
            {"kind": "Deployment", "metadata": {"name": "test-deployment"}},
            {"kind": "Service", "metadata": {"name": "test-service"}},
        ]

        try:
            k8s_service.start_deployment_and_service(path_manifest)
        except Exception as e:
            pytest.fail(f"start_deployment_and_service raised an exception: {e}")


def test_delete_minio():
    with patch(
        "framework_scheduling.kubernetes_service.terminate_deployment_and_service",
        autospec=True,
    ), patch(
        "framework_scheduling.kubernetes_service.utils.Utils.read_manifest",
        autospec=True,
        return_value=[],
    ):

        try:
            k8s_service.delete_minio()
        except Exception as e:
            pytest.fail(f"delete_minio raised an exception: {e}")


@patch("metrics.metrics_collector.get_numRecordsOut", return_value=10)
@patch("framework_scheduling.kubernetes_service.create_serverful_framework")
@patch("framework_scheduling.kubernetes_service.create_serverless_framework")
@patch("framework_scheduling.kubernetes_service.terminate_serverless_framework")
@patch("framework_scheduling.kubernetes_service.terminate_serverful_framework")
@patch("time.sleep", return_value=None)  # to avoid actual waiting
def test_make_change_serverless_to_serverful(
    mock_sleep,
    mock_terminate_serverful,
    mock_terminate_serverless,
    mock_create_serverless,
    mock_create_serverful,
    mock_get_numRecordsOut,
):
    framework_used = utils.Utils.Framework.SL
    number_messages_sent = 5
    application = "test_app"
    manifest_docs = "test_manifest"
    mongodb_address = "test_mongo"
    dataset = "test_dataset"

    # Execute the function with SL framework
    k8s_service.make_change(
        framework_used,
        number_messages_sent,
        application,
        manifest_docs,
        mongodb_address,
        dataset,
    )

    # Assert that termination of serverless and creation of serverful framework are called
    mock_terminate_serverless.assert_called_once()
    mock_create_serverful.assert_called_once_with(
        dataset, manifest_docs, mongodb_address, application
    )
    mock_terminate_serverful.assert_not_called()
    mock_create_serverless.assert_not_called()


@patch("metrics.metrics_collector.get_numRecordsOut", return_value=10)
@patch("framework_scheduling.kubernetes_service.create_serverful_framework")
@patch("framework_scheduling.kubernetes_service.create_serverless_framework")
@patch("framework_scheduling.kubernetes_service.terminate_serverless_framework")
@patch("framework_scheduling.kubernetes_service.terminate_serverful_framework")
@patch("time.sleep", return_value=None)  # to avoid actual waiting
def test_make_change_serverful_to_serverless(
    mock_sleep,
    mock_terminate_serverful,
    mock_terminate_serverless,
    mock_create_serverless,
    mock_create_serverful,
    mock_get_numRecordsOut,
):
    framework_used = utils.Utils.Framework.SF
    number_messages_sent = 5
    application = "test_app"
    manifest_docs = "test_manifest"
    mongodb_address = "test_mongo"
    dataset = "test_dataset"

    # Execute the function with SL framework
    k8s_service.make_change(
        framework_used,
        number_messages_sent,
        application,
        manifest_docs,
        mongodb_address,
        dataset,
    )

    # Assert that termination of serverless and creation of serverful framework are called
    mock_terminate_serverful.assert_called_once_with(manifest_docs)
    mock_create_serverless.assert_called_once_with(
        mongodb_address, dataset, application
    )
    mock_terminate_serverless.assert_not_called()
    mock_create_serverful.assert_not_called()
