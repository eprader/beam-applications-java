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
    def __init__(self, framework: utils.Utils.Framework, evaluation_event: Event):
        self.framework_used = framework
        self.evaluation_event = evaluation_event
        self.consumer = KafkaConsumer(
            "scheduler-input",
            bootstrap_servers=["kafka-cluster-kafka-bootstrap.default.svc:9092"],
        )
        self.producer = KafkaProducer(
            bootstrap_servers=["kafka-cluster-kafka-bootstrap.default.svc:9092"],
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            value_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else v,
        )

    def cleanup(self):
        try:
            self.consumer.close()
            self.producer.close()
            if self.framework_used == utils.Utils.Framework.SF:
                path_manifest_flink_session_cluster = (
                    "/app/flink-session-cluster-deployment.yaml"
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
        self.main_loop_setup(manifest_docs, application, dataset, mongodb)
        if application == "TRAIN":
            serverful_topic = "train-source"
        number_sent_messages_serverful = 0
        number_sent_messages_serverless = 0
        while True:
            self.main_loop_logic(
                serverful_topic,
                number_sent_messages_serverful,
                number_sent_messages_serverless,
            )
            if self.evaluation_event.is_set():
                framework_scheduling.kubernetes_service.make_change(self.framework_used)

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
            return e

    #FIXME: Check if from the KafkaProducer a byte or a string arrives as messages
    #this is important for the value in the producer
    def main_loop_logic(
        self,
        serverful_topic,
        number_sent_messages_serverful,
        number_sent_messages_serverless,
    ):
        serverful_topic = "senml-cleaned"
        message = self.consumer.poll(timeout_ms=5000)
        if message:
            for tp, messages in message.items():
                for msg in messages:
                    if self.framework_used == utils.Utils.Framework.SF:
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
                    framework_scheduling.kubernetes_service.create_serverful_framework(
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
                    framework_scheduling.kubernetes_service.create_serverless_framework(
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
