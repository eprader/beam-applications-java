from kafka import KafkaConsumer, KafkaProducer
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
import time
import logging
import struct
from kubernetes.stream import portforward
from framework_scheduling.kubernetes_service import KubernetesService
from threading import Event
import utils.Utils


class FrameworkScheduler:
    def __init__(self, framework: utils.Utils.Framework, evaluation_event:Event):
        self.framework_used = framework
        self.evaluation_event = evaluation_event
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

    def cleanup(self):
        try:
            self.consumer.close()
            self.producer.close()
            if self.framework_used ==  utils.Utils.Framework.SF:
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
            if self.framework_used ==  utils.Utils.Framework.SF:
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

            if self.evaluation_event.is_set():
                self.kubernetes_service.make_change(self.framework_used)

            message = self.consumer.poll(timeout_ms=5000)
            if message:
                for tp, messages in message.items():
                    for msg in messages:
                        if self.framework_used ==  utils.Utils.Framework.SF:
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
        serverful_topic = "senml-cleaned"
        if application == "TRAIN":
            serverful_topic = "train-source"

        while True:
            if self.framework_used == utils.Utils.Framework.SF:
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
