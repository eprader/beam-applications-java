import pytest
import yaml
import os
from utils.Utils import (
    Framework,
    get_opposite_framework,
    read_manifest_statefun_starter,
    read_manifest
)

class TestSchedulerLogic:

    def test_get_opposite_framework(self):
        assert get_opposite_framework(Framework.SF) == Framework.SL, "Expected SL for SF input"
        assert get_opposite_framework(Framework.SL) == Framework.SF, "Expected SF for SL input"

    def test_read_manifest_statefun_starter_local(self, tmp_path):
        manifest_path = os.path.abspath(
            os.path.join(os.getcwd(), "../config_frameworks/statefunStarter-manifest.yaml")
)
        
        mongodb = "test_db"
        dataset = "test_dataset"
        application = "test_application"
        result = read_manifest_statefun_starter(
            manifest_path, mongodb, dataset, application, run_locally=False
        )
        assert result[0]["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"] == "Always"
        env_vars = result[0]["spec"]["template"]["spec"]["containers"][0]["env"]
        env_dict = {env["name"]: env["value"] for env in env_vars}
        assert env_dict["MONGODB"] == mongodb
        assert env_dict["DATASET"] == dataset
        assert env_dict["APPLICATION"] == application

    def test_read_manifest(self, tmp_path):
        manifest_path = os.path.abspath(
            os.path.join(os.getcwd(), "../config_frameworks/minio.yaml"))
        result = read_manifest(manifest_path)

        assert result[0]["kind"] == "Deployment"
        assert result[0]["metadata"]["name"] == "minio"
        assert result[1]["kind"] == "Service"
        assert result[1]["metadata"]["name"] == "minio"