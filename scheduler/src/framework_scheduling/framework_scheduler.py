from kafka import KafkaConsumer, KafkaProducer
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
import time
import yaml
import os
import requests
import logging
import sys
import struct
import signal
from kubernetes.stream import portforward


consumer = KafkaConsumer(
    "scheduler-input",
    bootstrap_servers=["kafka-cluster-kafka-bootstrap.default.svc:9092"],
)
producer = KafkaProducer(
    bootstrap_servers=["kafka-cluster-kafka-bootstrap.default.svc:9092"],
    key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
    value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
)
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
forwarder = None
is_serverful_framework_used = True


def handle_sigterm(signum, frame):
    logging.info("Received SIGTERM signal. Shutting down scheduler gracefully...")
    cleanup()
    exit(0)


def cleanup():
    global forwarder
    try:
        consumer.close()
        producer.close()
        global is_serverful_framework_used
        if is_serverful_framework_used:
            path_manifest_flink_session_cluster = (
                "/app/flink-session-cluster-deployment.yaml"
            )
            manifest_docs_flink_session_cluster = read_manifest(
                path_manifest_flink_session_cluster
            )
            terminate_serverful_framework(manifest_docs_flink_session_cluster)
        else:
            terminate_serverless_framework()
    except Exception as e:
        logging.error(f"Cleanup error: {e}")


def port_forward_service(service_name, namespace, local_port, service_port):
    try:
        config.load_incluster_config()
        v1 = client.CoreV1Api()

        service = v1.read_namespaced_service(name=service_name, namespace=namespace)

        selector = service.spec.selector
        if not selector:
            logging.error(f"Service {service_name} has no selector defined")
            return None

        pods = v1.list_namespaced_pod(
            namespace,
            label_selector=",".join([f"{k}={v}" for k, v in selector.items()]),
        )

        if not pods.items:
            logging.error(f"No pods found for service {service_name}")
            return None

        pod_name = pods.items[0].metadata.name
        logging.info(f"Forwarding from service '{service_name}' using pod '{pod_name}'")

        pf = portforward(
            v1.connect_get_namespaced_pod_portforward,
            pod_name,
            namespace,
            ports=str(local_port),
        )

        pf.local_port = local_port
        pf.remote_port = service_port
        logging.info(f"Port-forwarding started: {pf.local_port} -> {pf.remote_port}")

        return pf
    except Exception as e:
        logging.error(f"Port-forwarding error: {e}")
        return None


def stop_port_forward(pf):
    if pf:
        pf.close()
        logging.info("Port-forwarding stopped")


def read_metric_from_prometheus(metric_name):
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
        return 0


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


def read_manifest(path_manifest):
    with open(path_manifest, "r") as f:
        manifest = list(yaml.safe_load_all(f))
    return manifest


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
    manifest = read_manifest("/app/minio.yaml")
    start_deployment_and_service(manifest)


def create_statefun_environment():
    # Note this is a ConfigMap
    manifest_module = read_manifest("/app/00-module.yaml")
    manifest_runtime = read_manifest("/app/01-statefun-runtime.yaml")
    start_deployment_and_service(manifest_module)
    start_deployment_and_service(manifest_runtime)


def create_statefun_starter(mongodb, dataset, application):
    # FIXME for remote running
    manifest = read_manifest_statefun_starter(
        "/app/statefunStarter-manifest.yaml", mongodb, dataset, application, True
    )
    start_deployment_and_service(manifest, True)


def delete_minio():
    manifest = read_manifest("/app/minio.yaml")
    terminate_deployment_and_service(manifest)


def delete_statefun_environment():
    manifest_module = read_manifest("/app/00-module.yaml")
    manifest_runtime = read_manifest("/app/01-statefun-runtime.yaml")
    terminate_deployment_and_service(manifest_module)
    terminate_deployment_and_service(manifest_runtime)


def delete_statefun_starter():
    manifest = read_manifest("/app/statefunStarter-manifest.yaml")
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
    submit_flink_job(
        "/app/FlinkJob.jar",
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


def read_manifest_statefun_starter(
    path_manifest, mongodb, dataset, application, run_locally=False
):
    with open(path_manifest, "r") as f:
        manifest = list(yaml.safe_load_all(f))
    for item in manifest:
        if item["kind"] == "Deployment":
            containers = item["spec"]["template"]["spec"]["containers"]
            for container in containers:
                if run_locally:
                    container["imagePullPolicy"] = "IfNotPresent"
                else:
                    container["imagePullPolicy"] = "Always"
                env_vars = container.get("env", [])
                for env in env_vars:
                    if env["name"] == "MONGODB":
                        env["value"] = mongodb
                    elif env["name"] == "DATASET":
                        env["value"] = dataset
                    elif env["name"] == "APPLICATION":
                        env["value"] = application
    return manifest


def delete_all_jobs_from_serverful_framework():
    list_of_running_jobs = get_jobid_of_running_job("flink-session-cluster-rest")
    for job in list_of_running_jobs:
        stop_flink_job("flink-session-cluster-rest", job)
        logging.info(f"Deleted job {job}")


def main(manifest_docs, application, dataset, mongodb):
    is_deployed = False
    number_sent_messages_serverful = 0
    number_sent_messages_serverless = 0
    global is_serverful_framework_used
    serverful_topic = "senml-cleaned"
    if application == "TRAIN":
        serverful_topic = "train-source"

    while True:
        message = consumer.poll(timeout_ms=5000)
        if message:
            for tp, messages in message.items():
                for msg in messages:
                    if not is_deployed:
                        if is_serverful_framework_used:
                            create_serverful_framework(
                                dataset, manifest_docs, mongodb, application
                            )
                            is_deployed = True
                        else:
                            create_serverless_framework(mongodb, dataset, application)
                            is_deployed = True

                    if is_serverful_framework_used:
                        producer.send(
                            serverful_topic,
                            key=struct.pack(">Q", int(time.time() * 1000)),
                            value=msg.decode().encode("utf-8"),
                        )
                        number_sent_messages_serverful = (
                            number_sent_messages_serverful + 1
                        )
                        logging.info(
                            f"Number of sent messages serverful: {number_sent_messages_serverful}"
                        )
                    else:
                        producer.send(
                            "statefun-starter-input",
                            key=str(int(time.time() * 1000)).encode("utf-8"),
                            value=msg.decode(),
                        )
                        number_sent_messages_serverless = (
                            number_sent_messages_serverless + 1
                        )
                        logging.info(
                            f"Number of sent messages serverless: {number_sent_messages_serverless}"
                        )


def debug_main(manifest_docs, application, dataset, mongodb):
    is_deployed = False
    number_sent_messages = 0

    global is_serverful_framework_used
    is_serverful_framework_used = False

    serverful_topic = "senml-cleaned"
    if application == "TRAIN":
        serverful_topic = "train-source"

    while True:
        if is_serverful_framework_used:
            if not is_deployed:
                create_serverful_framework(dataset, manifest_docs, mongodb, application)
                is_deployed = True

            test_string = '[{"u":"string","n":"source","vs":"ci4lrerertvs6496"},{"v":"64.57491754110376","u":"lon","n":"longitude"},{"v":"171.83173176418288","u":"lat","n":"latitude"},{"v":"67.7","u":"far","n":"temperature"},{"v":"76.6","u":"per","n":"humidity"},{"v":"1351","u":"per","n":"light"},{"v":"929.74","u":"per","n":"dust"},{"v":"26","u":"per","n":"airquality_raw"}]'
            producer.send(
                serverful_topic,
                key=struct.pack(">Q", int(time.time() * 1000)),
                value=test_string.encode("utf-8"),
            )
            logging.info("send message for serverful")
        else:
            if not is_deployed:
                create_serverless_framework(mongodb, dataset, application)
                is_deployed = True

            test_string = '[{"u":"string","n":"source","vs":"ci4lrerertvs6496"},{"v":"64.57491754110376","u":"lon","n":"longitude"},{"v":"171.83173176418288","u":"lat","n":"latitude"},{"v":"67.7","u":"far","n":"temperature"},{"v":"76.6","u":"per","n":"humidity"},{"v":"1351","u":"per","n":"light"},{"v":"929.74","u":"per","n":"dust"},{"v":"26","u":"per","n":"airquality_raw"}]'
            producer.send(
                "statefun-starter-input",
                key=str(int(time.time() * 1000)).encode("utf-8"),
                value=test_string,
            )
            logging.info("send message for serverless")
        time.sleep(60)


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, handle_sigterm)
    try:
        application = os.getenv("APPLICATION")
        mongodb_address = os.getenv("MONGODB")
        dataset = os.getenv("DATASET")
        if not (dataset == "FIT" or dataset == "SYS" or dataset == "TAXI"):
            raise Exception("Unsupported dataset argument")
        path_manifest_flink_session_cluster = (
            "/app/flink-session-cluster-deployment.yaml"
        )
        manifest_docs_flink_session_cluster = read_manifest(
            path_manifest_flink_session_cluster
        )
        # main(manifest_docs, application)
        debug_main(
            manifest_docs_flink_session_cluster, application, dataset, mongodb_address
        )
    except KeyboardInterrupt:
        logging.info("Shutting down")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

    finally:
        cleanup()
