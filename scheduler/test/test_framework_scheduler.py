import pytest
from unittest.mock import patch, MagicMock
from framework_scheduling.framework_scheduler import FrameworkScheduler
from threading import Event
import utils.Utils
import logging
import struct
import time


@pytest.fixture
def mock_framework_scheduler():
    with patch(
        "framework_scheduling.framework_scheduler.KafkaConsumer"
    ) as MockConsumer, patch(
        "framework_scheduling.framework_scheduler.KafkaProducer"
    ) as MockProducer, patch(
        "framework_scheduling.kubernetes_service.terminate_serverful_framework",
        new=MagicMock(),
    ) as mock_terminate_serverful, patch(
        "framework_scheduling.kubernetes_service.terminate_serverless_framework",
        new=MagicMock(),
    ) as mock_terminate_serverless, patch(
        "framework_scheduling.kubernetes_service.create_serverful_framework",
        new=MagicMock(),
    ) as mock_create_serverful, patch(
        "framework_scheduling.kubernetes_service.create_serverless_framework",
        new=MagicMock(),
    ) as mock_create_serverless, patch(
        "utils.Utils.read_manifest", new=MagicMock()
    ) as mock_read_manifest, patch(
        "framework_scheduling.kubernetes_service.make_change", new=MagicMock()
    ) as mock_make_change:

        mock_consumer_instance = MockConsumer.return_value
        mock_consumer_instance.poll = MagicMock(
            return_value={"partition": [b"test message"]}
        )

        mock_producer_instance = MockProducer.return_value
        mock_producer_instance.send = MagicMock()

        framework = utils.Utils.Framework.SF
        evaluation_event = Event()
        scheduler = FrameworkScheduler(framework, evaluation_event)
        scheduler.consumer = mock_consumer_instance
        scheduler.producer = mock_producer_instance

        yield scheduler, MockConsumer, MockProducer, mock_create_serverful, mock_create_serverless, mock_make_change, mock_terminate_serverful, mock_terminate_serverless, mock_read_manifest


def test_framework_scheduler_init(mock_framework_scheduler):
    (
        scheduler,
        MockConsumer,
        MockProducer,
        mock_create_serverful,
        mock_create_serverless,
        mock_make_change,
        mock_terminate_serverful,
        mock_terminate_serverless,
        mock_read_manifest,
    ) = mock_framework_scheduler
    assert scheduler.framework_used == utils.Utils.Framework.SF
    assert isinstance(scheduler.evaluation_event, Event)


def test_framework_scheduler_cleanup_SF(mock_framework_scheduler):
    (
        scheduler,
        MockConsumer,
        MockProducer,
        mock_create_serverful,
        mock_create_serverless,
        mock_make_change,
        mock_terminate_serverful,
        mock_terminate_serverless,
        mock_read_manifest,
    ) = mock_framework_scheduler
    scheduler.framework_used = utils.Utils.Framework.SF
    scheduler.cleanup()
    scheduler.consumer.close.assert_called_once()
    scheduler.producer.close.assert_called_once()
    mock_read_manifest.assert_called_once()
    mock_terminate_serverful.assert_called_once()
    mock_terminate_serverless.assert_not_called()


def test_framework_scheduler_cleanup_SL(mock_framework_scheduler):
    (
        scheduler,
        MockConsumer,
        MockProducer,
        mock_create_serverful,
        mock_create_serverless,
        mock_make_change,
        mock_terminate_serverful,
        mock_terminate_serverless,
        mock_read_manifest,
    ) = mock_framework_scheduler
    scheduler.framework_used = utils.Utils.Framework.SL
    scheduler.cleanup()
    scheduler.consumer.close.assert_called_once()
    scheduler.producer.close.assert_called_once()
    mock_read_manifest.assert_not_called()
    mock_terminate_serverful.assert_not_called()
    mock_terminate_serverless.assert_called_once()


def test_main_loop_setup_serverless_success(mock_framework_scheduler):
    (
        scheduler,
        MockConsumer,
        MockProducer,
        mock_create_serverful,
        mock_create_serverless,
        mock_make_change,
        mock_terminate_serverful,
        mock_terminate_serverless,
        mock_read_manifest,
    ) = mock_framework_scheduler
    scheduler.framework_used = utils.Utils.Framework.SL
    manifest_docs = "mock_manifest"
    application = "TRAIN"
    dataset = "mock_dataset"
    mongodb = "mock_mongodb"
    result = scheduler.main_loop_setup(manifest_docs, application, dataset, mongodb)
    mock_create_serverless.assert_called_once_with(mongodb, dataset, application)
    mock_create_serverful.assert_not_called()
    assert (
        result is True
    ), "Expected main_loop_setup to return True on successful serverless setup"


def test_main_loop_setup_serverful_success(mock_framework_scheduler):
    (
        scheduler,
        MockConsumer,
        MockProducer,
        mock_create_serverful,
        mock_create_serverless,
        mock_make_change,
        mock_terminate_serverful,
        mock_terminate_serverless,
        mock_read_manifest,
    ) = mock_framework_scheduler
    scheduler.framework_used = utils.Utils.Framework.SF
    manifest_docs = "mock_manifest"
    application = "TRAIN"
    dataset = "mock_dataset"
    mongodb = "mock_mongodb"
    result = scheduler.main_loop_setup(manifest_docs, application, dataset, mongodb)
    mock_create_serverful.assert_called_once_with(
        dataset, manifest_docs, mongodb, application
    )
    mock_create_serverless.assert_not_called()
    assert (
        result is True
    ), "Expected main_loop_setup to return True on successful serverless setup"


def test_main_loop_logic_serverful(mock_framework_scheduler):
    (
        scheduler,
        MockConsumer,
        MockProducer,
        mock_create_serverful,
        mock_create_serverless,
        mock_make_change,
        mock_terminate_serverful,
        mock_terminate_serverless,
        mock_read_manifest,
    ) = mock_framework_scheduler
    serverful_topic = "senml-cleaned"
    number_sent_messages_serverful = 0
    number_sent_messages_serverless = 0
    scheduler.main_loop_logic(
        serverful_topic, number_sent_messages_serverful, number_sent_messages_serverless
    )
    scheduler.producer.send.assert_called_once()

def test_main_loop_logic_serverless(mock_framework_scheduler):
    (
        scheduler,
        MockConsumer,
        MockProducer,
        mock_create_serverful,
        mock_create_serverless,
        mock_make_change,
        mock_terminate_serverful,
        mock_terminate_serverless,
        mock_read_manifest,
    ) = mock_framework_scheduler
    serverful_topic = "senml-cleaned"
    number_sent_messages_serverful = 0
    number_sent_messages_serverless = 0
    scheduler.framework_used = utils.Utils.Framework.SL
    scheduler.main_loop_logic(
        serverful_topic, number_sent_messages_serverful, number_sent_messages_serverless
    )
    scheduler.producer.send.assert_called_once()
