import yaml
from enum import Enum

class Framework(Enum):
    SF = 1
    SL = 2


def get_opposite_framework(current_framework:Framework):
    if current_framework == Framework.SF:
        return Framework.SL
    return Framework.SF

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

def read_manifest(path_manifest):
    with open(path_manifest, "r") as f:
        manifest = list(yaml.safe_load_all(f))
    return manifest