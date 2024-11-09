from kafka import KafkaConsumer, KafkaProducer
from kubernetes import client, config
from kubernetes.client.exceptions import ApiException
import time
import logging
import struct
from kubernetes.stream import portforward
import framework_scheduling.kubernetes_service
from threading import Event
import utils.Utils


class FrameworkScheduler:
    def __init__(
        self,
        framework: utils.Utils.Framework,
        evaluation_event: Event,
        framework_running_event: Event,
    ):
        self.framework_used = framework
        self.evaluation_event = evaluation_event
        self.framework_running_event = framework_running_event
        self.consumer = KafkaConsumer(
            "scheduler-input",
            bootstrap_servers=["kafka-cluster-kafka-bootstrap.default.svc:9092"],
            group_id="scheduler-framework-scheduler",
        )
        self.producer = KafkaProducer(
            bootstrap_servers=["kafka-cluster-kafka-bootstrap.default.svc:9092"],
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        )
        self.framework_is_running = False

    def cleanup(self):
        try:
            self.consumer.close()
            self.producer.close()
            if self.framework_is_running:
                if self.framework_used == utils.Utils.Framework.SF:
                    path_manifest_flink_session_cluster = (
                        "/app/config_frameworks/flink-session-cluster-deployment.yaml"
                    )
                    manifest_docs_flink_session_cluster = utils.Utils.read_manifest(
                        path_manifest_flink_session_cluster
                    )
                    framework_scheduling.kubernetes_service.terminate_serverful_framework(
                        manifest_docs_flink_session_cluster
                    )
                else:
                    framework_scheduling.kubernetes_service.terminate_serverless_framework()
        except Exception as e:
            logging.error(f"Cleanup error: {e}")
            raise e

    def main_run(self, manifest_docs, application, dataset, mongodb):
        setup_successful = self.main_loop_setup(manifest_docs, application, dataset, mongodb)
        if not setup_successful:
            logging.error("Exiting main_run")
            return
        self.framework_is_running = True
        self.framework_running_event.set()
        if application == "TRAIN":
            serverful_topic = "train-source"
        elif application == "PRED":
            serverful_topic = "senml-cleaned"
        number_messages_sent = 0
        while True:
            number_messages_sent = self.main_loop_logic(
                serverful_topic,
                number_messages_sent,
            )
            if self.evaluation_event.is_set():
                self.framework_is_running = False
                framework_scheduling.kubernetes_service.make_change(
                    self.framework_used,
                    number_messages_sent,
                    application,
                    manifest_docs,
                    mongodb,
                    dataset,
                )
                self.framework_used = utils.Utils.get_opposite_framework(
                    self.framework_used
                )
                number_messages_sent = 0
                self.framework_is_running = True
                self.evaluation_event.clear()

    def main_loop_setup(self, manifest_docs, application, dataset, mongodb):
        try:
            if self.framework_used == utils.Utils.Framework.SF:
                framework_scheduling.kubernetes_service.create_serverful_framework(
                    dataset, manifest_docs, mongodb, application
                )
            else:
                framework_scheduling.kubernetes_service.create_serverless_framework(
                    mongodb, dataset, application
                )
            return True
        except Exception as e:
            logging.error("Error, when setting up main loop", e)
            return False

    # FIXME: Check if from the KafkaProducer a byte or a string arrives as messages
    # this is important for the value in the producer
    def main_loop_logic(self, serverful_topic, number_messages_sent):
        message = self.consumer.poll(timeout_ms=5000)
        if message:
            for tp, messages in message.items():
                for msg in messages:
                    if self.framework_used == utils.Utils.Framework.SF:
                        self.producer.send(
                            serverful_topic,
                            key=struct.pack(">Q", int(time.time() * 1000)),
                            value=msg.value.decode().encode("utf-8"),
                        )
                        number_messages_sent = number_messages_sent + 1
                        logging.info(
                            f"Number of sent messages serverful: {number_messages_sent}"
                        )
                    else:
                        self.producer.send(
                            "statefun-starter-input",
                            key=str(int(time.time() * 1000)).encode("utf-8"),
                            value=msg.value.decode(),
                        )
                        number_messages_sent = number_messages_sent + 1

                        logging.info(
                            f"Number of sent messages serverless: {number_messages_sent}"
                        )
        return number_messages_sent
