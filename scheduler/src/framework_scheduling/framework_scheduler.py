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
from kubernetes_service import KubernetesService


class FrameworkScheduler:
    def __init__(self, is_serverful_framework_used):
        self.is_serverful_framework_used = is_serverful_framework_used
        self.kubernetes_service = KubernetesService()
        consumer = KafkaConsumer(
            "scheduler-input",
            bootstrap_servers=["kafka-cluster-kafka-bootstrap.default.svc:9092"],
        )
        producer = KafkaProducer(
            bootstrap_servers=["kafka-cluster-kafka-bootstrap.default.svc:9092"],
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        )

    @staticmethod
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

    @staticmethod
    def read_manifest(path_manifest):
        with open(path_manifest, "r") as f:
            manifest = list(yaml.safe_load_all(f))
        return manifest

    def cleanup(self):
        try:
            self.consumer.close()
            self.producer.close()
            self.is_serverful_framework_used
            if is_serverful_framework_used:
                path_manifest_flink_session_cluster = (
                    "/app/flink-session-cluster-deployment.yaml"
                )
                manifest_docs_flink_session_cluster = FrameworkScheduler.read_manifest(
                    path_manifest_flink_session_cluster
                )
                self.kubernetes_service.terminate_serverful_framework(
                    manifest_docs_flink_session_cluster
                )
            else:
                self.kubernetes_service.terminate_serverless_framework()
        except Exception as e:
            logging.error(f"Cleanup error: {e}")

    def main_run(self, manifest_docs, application, dataset, mongodb):
        is_deployed = False
        number_sent_messages_serverful = 0
        number_sent_messages_serverless = 0
        serverful_topic = "senml-cleaned"
        if application == "TRAIN":
            serverful_topic = "train-source"

        if not is_deployed:
            if is_serverful_framework_used:
                self.kubernetes_service.create_serverful_framework(
                    dataset, manifest_docs, mongodb, application
                )
                is_deployed = True
            else:
                self.kubernetes_service.create_serverless_framework(
                    mongodb, dataset, application
                )
                is_deployed = True

        while True:
            message = self.consumer.poll(timeout_ms=5000)
            if message:
                for tp, messages in message.items():
                    for msg in messages:
                        if is_serverful_framework_used:
                            self.producer.send(
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
                            self.producer.send(
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

    def debug_run(self, manifest_docs, application, dataset, mongodb):
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
                    self.kubernetes_service.create_serverful_framework(
                        dataset, manifest_docs, mongodb, application
                    )
                    is_deployed = True

                test_string = '[{"u":"string","n":"source","vs":"ci4lrerertvs6496"},{"v":"64.57491754110376","u":"lon","n":"longitude"},{"v":"171.83173176418288","u":"lat","n":"latitude"},{"v":"67.7","u":"far","n":"temperature"},{"v":"76.6","u":"per","n":"humidity"},{"v":"1351","u":"per","n":"light"},{"v":"929.74","u":"per","n":"dust"},{"v":"26","u":"per","n":"airquality_raw"}]'
                self.producer.send(
                    serverful_topic,
                    key=struct.pack(">Q", int(time.time() * 1000)),
                    value=test_string.encode("utf-8"),
                )
                logging.info("send message for serverful")
            else:
                if not is_deployed:
                    self.kubernetes_service.create_serverless_framework(
                        mongodb, dataset, application
                    )
                    is_deployed = True

                test_string = '[{"u":"string","n":"source","vs":"ci4lrerertvs6496"},{"v":"64.57491754110376","u":"lon","n":"longitude"},{"v":"171.83173176418288","u":"lat","n":"latitude"},{"v":"67.7","u":"far","n":"temperature"},{"v":"76.6","u":"per","n":"humidity"},{"v":"1351","u":"per","n":"light"},{"v":"929.74","u":"per","n":"dust"},{"v":"26","u":"per","n":"airquality_raw"}]'
                self.producer.send(
                    "statefun-starter-input",
                    key=str(int(time.time() * 1000)).encode("utf-8"),
                    value=test_string,
                )
                logging.info("send message for serverless")
            time.sleep(60)


if __name__ == "__main__":
    pass
