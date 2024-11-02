from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
import logging
import time
import requests
import utils.Utils
import metrics.metrics_collector


def delete_all_jobs_from_serverful_framework():
    list_of_running_jobs = get_jobid_of_running_job("flink-session-cluster-rest")
    for job in list_of_running_jobs:
        stop_flink_job("flink-session-cluster-rest", job)
        logging.info(f"Deleted job {job}")


def is_flink_deployment_ready(
    k8s_custom_objects_api, flink_deployment_name, namespace="default"
):
    flink_deployment = k8s_custom_objects_api.get_namespaced_custom_object(
        group="flink.apache.org",
        version="v1beta1",
        namespace=namespace,
        plural="flinkdeployments",
        name=flink_deployment_name,
    )

    job_manager_status = flink_deployment.get("status", {}).get(
        "jobManagerDeploymentStatus"
    )
    if job_manager_status == "READY":
        return True

    return False


def wait_for_flink_deployment(
    k8s_apps_v1,
    deployment_name,
    namespace="default",
    timeout=300,
    interval=5,
):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_flink_deployment_ready(k8s_apps_v1, deployment_name, namespace):
            logging.info(f"Deployment '{deployment_name}' is ready.")
            return True
        time.sleep(interval)
    logging.error(f"Timeout reached. Deployment '{deployment_name} not ready.")
    return False


def start_flink_deployment(path_manifest):
    logging.info("Starting flink-session-cluster")
    config.load_incluster_config()
    k8s_custom_objects_api = client.CustomObjectsApi()
    deployment_name = None
    start_time = time.time()
    for doc in path_manifest:
        kind = doc.get("kind")
        metadata = doc.get("metadata", {})
        name = metadata.get("name")
        if kind == "FlinkDeployment":
            deployment_name = name
            resp = k8s_custom_objects_api.create_namespaced_custom_object(
                group="flink.apache.org",
                version="v1beta1",
                plural="flinkdeployments",
                body=doc,
                namespace="default",
            )
            logging.info(
                f"FlinkDeployment '{deployment_name}' created. Status='{resp['metadata']['name']}'"
            )
    if deployment_name:
        if wait_for_flink_deployment(k8s_custom_objects_api, deployment_name):
            end_time = time.time()
            duration = end_time - start_time
            logging.info(
                f"Time taken to create flink-session-cluster: {duration:.2f} seconds"
            )

        else:
            logging.error("Deployment did not become ready in time.")
    else:
        logging.error("Deployment name not found in manifest.")


def terminate_flink_deployment(manifest_docs):
    config.load_incluster_config()
    k8s_custom_objects_api = client.CustomObjectsApi()

    for doc in manifest_docs:
        kind = doc.get("kind")
        metadata = doc.get("metadata", {})
        name = metadata.get("name")
        if kind == "FlinkDeployment":
            resp = k8s_custom_objects_api.delete_namespaced_custom_object(
                group="flink.apache.org",
                version="v1beta1",
                namespace="default",
                plural="flinkdeployments",
                name=name,
                body=client.V1DeleteOptions(),
            )
            logging.info(f"FlinkDeployment '{name}' deleted: {resp}")


def submit_flink_job(job_jar_path, job_manager_host, database_url, experiment_run_id):
    url = f"http://{job_manager_host}:8081/jars/upload"

    with open(job_jar_path, "rb") as jar_file:
        response = requests.post(url, files={"jarfile": jar_file})

    if response.status_code != 200:
        raise Exception(f"Failed to upload JAR file: {response.text}")

    jar_id = response.json()["filename"].split("/")[-1]
    logging.info("Jar: id" + str(jar_id))
    submit_url = f"http://{job_manager_host}:8081/jars/{jar_id}/run"
    job_params = {
        "programArgs": f"--databaseUrl={database_url} --experiRunId={experiment_run_id} --streaming --operatorChaining=false"
    }

    response = requests.post(submit_url, json=job_params)
    logging.info(str(response.content))
    # Otherwise there is an error about the detached mode
    """
    if response.status_code != 200:
        logging.error("Failed jar-submission: "+str(response.status_code))
        raise Exception(f"Failed to submit job: {response.text}")

    """
    logging.info(f"Job submitted successfully: {response.json()}")


def get_jobid_of_running_job(job_manager_host):
    submit_url = f"http://{job_manager_host}:8081/jobs"
    response = requests.get(submit_url)
    logging.info(str(response.content))
    if response.status_code == 200:
        jobs_data = response.json()
        running_jobs = []
        for job in jobs_data["jobs"]:
            if job["status"] == "RUNNING":
                running_jobs.append(job["id"])
        return running_jobs
    else:
        logging.error(f"Failed to fetch jobs. Status code: {response.status_code}")
        return []


def stop_flink_job(job_manager_host, job_id):
    url = f"http://{job_manager_host}:8081/jobs/{job_id}/yarn-cancel"

    try:
        response = requests.get(url)
        if response.status_code == 200 or response.status_code == 202:
            logging.info("Job stopped successfully.")
        else:
            logging.error(
                f"Failed to stop the job. Status code: {response.status_code}"
            )
            logging.error(f"Response: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"An error occurred: {e}")


def start_deployment_and_service(path_manifest, is_statefun_starter=False):
    logging.info("Starting deployment and service")
    config.load_incluster_config()
    k8s_core_v1 = client.CoreV1Api()
    k8s_apps_v1 = client.AppsV1Api()
    deployment_name = None
    service_name = None
    start_time = time.time()
    for doc in path_manifest:
        kind = doc.get("kind")
        metadata = doc.get("metadata", {})
        name = metadata.get("name")
        if kind == "Deployment":
            deployment_name = name
            resp = k8s_apps_v1.create_namespaced_deployment(
                body=doc, namespace="statefun"
            )
            logging.info(
                f"Deployment '{deployment_name}' created. Status='{resp.metadata.name}'"
            )
        elif kind == "Service":
            service_name = name
            resp = k8s_core_v1.create_namespaced_service(
                body=doc, namespace="statefun", pretty="true"
            )
            logging.info(
                f"Service '{service_name}' created. Status='{resp.metadata.name}'"
            )
        elif kind == "ConfigMap":
            config_map_name = name
            resp = k8s_core_v1.create_namespaced_config_map(
                body=doc, namespace="statefun"
            )
            logging.info(
                f"ConfigMap '{config_map_name}' created. Status='{resp.metadata.name}'"
            )

    if deployment_name and service_name and not is_statefun_starter:
        if wait_for_deployment_and_service(
            k8s_apps_v1, k8s_core_v1, deployment_name, service_name
        ):
            end_time = time.time()
            duration = end_time - start_time
            logging.info(f"Time taken to create deployment: {duration:.2f} seconds")
    elif deployment_name and is_statefun_starter:
        if wait_for_deployment(k8s_apps_v1, deployment_name):
            end_time = time.time()
            duration = end_time - start_time
            logging.info(f"Time taken to create deployment: {duration:.2f} seconds")
        else:
            logging.error("Deployment or Service did not become ready in time.")
    else:
        logging.error("Deployment or Service name not found in manifest.")


def wait_for_deployment_and_service(
    k8s_apps_v1,
    k8s_core_v1,
    deployment_name,
    service_name,
    namespace="statefun",
    timeout=300,
    interval=5,
):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_deployment_ready(
            k8s_apps_v1, deployment_name, namespace
        ) and is_service_ready(k8s_core_v1, service_name, namespace):
            logging.info(
                f"Deployment '{deployment_name}' is ready and Service '{service_name}' is ready."
            )
            return True
        time.sleep(interval)
    logging.error(
        f"Timeout reached. Deployment '{deployment_name}' or Service '{service_name}' not ready."
    )
    return False


def wait_for_deployment(
    k8s_apps_v1,
    deployment_name,
    namespace="statefun",
    timeout=300,
    interval=5,
):
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_deployment_ready(k8s_apps_v1, deployment_name, namespace):
            logging.info(f"Deployment '{deployment_name}' is ready.")
            return True
        time.sleep(interval)
    logging.error(f"Timeout reached. Deployment '{deployment_name} not ready.")
    return False


def is_deployment_ready(k8s_apps_v1, deployment_name, namespace="statefun"):
    deployment = k8s_apps_v1.read_namespaced_deployment(
        name=deployment_name, namespace=namespace
    )
    return deployment.status.ready_replicas == deployment.spec.replicas


def is_service_ready(k8s_core_v1, service_name, namespace="statefun"):
    endpoints = k8s_core_v1.read_namespaced_endpoints(
        name=service_name, namespace=namespace
    )
    return len(endpoints.subsets) > 0


def create_minio():
    manifest = utils.Utils.read_manifest("/app/minio.yaml")
    start_deployment_and_service(manifest)


def create_statefun_environment():
    # Note this is a ConfigMap
    manifest_module = utils.Utils.read_manifest("/app/00-module.yaml")
    manifest_runtime = utils.Utils.read_manifest("/app/01-statefun-runtime.yaml")
    start_deployment_and_service(manifest_module)
    start_deployment_and_service(manifest_runtime)


def create_statefun_starter(mongodb, dataset, application):
    manifest = utils.Utils.read_manifest_statefun_starter(
        "/app/statefunStarter-manifest.yaml", mongodb, dataset, application, False
    )
    start_deployment_and_service(manifest, True)


def delete_minio():
    manifest = utils.Utils.read_manifest("/app/minio.yaml")
    terminate_deployment_and_service(manifest)


def delete_statefun_environment():
    manifest_module = utils.Utils.read_manifest("/app/00-module.yaml")
    manifest_runtime = utils.Utils.read_manifest("/app/01-statefun-runtime.yaml")
    terminate_deployment_and_service(manifest_module)
    terminate_deployment_and_service(manifest_runtime)


def delete_statefun_starter():
    manifest = utils.Utils.read_manifest("/app/statefunStarter-manifest.yaml")
    terminate_deployment_and_service(manifest)


def terminate_serverless_framework():
    delete_statefun_starter()
    delete_statefun_environment()
    delete_minio()


def create_serverless_framework(mongodb, dataset, application):
    create_minio()
    create_statefun_environment()
    create_statefun_starter(mongodb, dataset, application)


def create_serverful_framework(dataset, path_manifest, mongodb_address, application):
    start_flink_deployment(path_manifest)
    basic_path = "/app/config_frameworks/"
    if application == "PRED":
        basic_path = basic_path + "pred/"
    elif application == "TRAIN":
        basic_path = basic_path + "train/"
    submit_flink_job(
        basic_path + "FlinkJob.jar",
        "flink-session-cluster-rest",
        mongodb_address,
        dataset + "-120",
    )


def terminate_serverful_framework(manifest_docs):
    delete_all_jobs_from_serverful_framework()
    terminate_flink_deployment(manifest_docs)


def terminate_deployment_and_service(manifest_docs):
    config.load_incluster_config()
    k8s_core_v1 = client.CoreV1Api()
    k8s_apps_v1 = client.AppsV1Api()
    for doc in manifest_docs:
        kind = doc.get("kind")
        metadata = doc.get("metadata", {})
        name = metadata.get("name")
        if kind == "Deployment":
            resp = k8s_apps_v1.delete_namespaced_deployment(
                name=name, namespace="statefun"
            )
            logging.info(f"Deployment '{name}' deleted")
        elif kind == "Service":
            resp = k8s_core_v1.delete_namespaced_service(
                name=name, namespace="statefun"
            )
            logging.info(f"Service '{name}' deleted")
        elif kind == "ConfigMap":
            resp = k8s_core_v1.delete_namespaced_config_map(
                name=name, namespace="statefun"
            )
            logging.info(f"ConfigMap '{name}' deleted.")


def make_change(
    framework_used: utils.Utils.Framework,
    number_messages_sent: int,
    application: str,
    manifest_docs,
    mongodb_address,
    dataset,
):
    while True:
        if 2 * number_messages_sent >= metrics.metrics_collector.get_numRecordsOut(
            framework_used, application
        ):
            break
        time.sleep(20)

    if framework_used == utils.Utils.Framework.SL:
        terminate_serverless_framework()
        create_serverful_framework(dataset, manifest_docs, mongodb_address, application)

    if framework_used == utils.Utils.Framework.SF:
        terminate_serverful_framework(manifest_docs)
        create_serverless_framework(mongodb_address, dataset, application)
