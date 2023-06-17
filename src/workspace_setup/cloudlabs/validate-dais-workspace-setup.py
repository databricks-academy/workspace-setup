import os
import json
import requests
from dbacademy import common
from dbacademy.dbrest import DBAcademyRestClient

# cloud = "aws"
# cloud = "msa"
cloud = "gcp"

config_file = r"c:\users\JacobParr\.databrickscfg"
configs = common.load_databricks_cfg(config_file)

token = configs.get(f"dev-{cloud}").get("token")
endpoint = configs.get(f"dev-{cloud}").get("host")

client = DBAcademyRestClient(token=token, endpoint=endpoint)

job_def_text = requests.get("https://raw.githubusercontent.com/databricks-academy/workspace-setup/main/CloudLabs/dais-job-config.json").text

job_def_text = job_def_text.replace("{{ODL-ID}}", "odl-id-123")
job_def_text = job_def_text.replace("{{ODL-TITLE}}", "odl-title-some-random-course")
job_def_text = job_def_text.replace("{{ODL-TENANT}}", "odl-tenant-test")

job_def_text = job_def_text.replace("{{runtimeVersion}}", "11.3.x-scala2.12")

node_type = None

job_def = json.loads(job_def_text)
new_cluster = job_def.get("job_clusters")[0].get("new_cluster")
base_parameters = job_def.get("tasks")[0].get("notebook_task").get("base_parameters")

if cloud == "aws":
    node_type_id = new_cluster.get("aws:node_type_id")
    new_cluster["node_type_id"] = node_type_id
    new_cluster["aws_attributes"] = new_cluster.get("aws:aws_attributes")
    base_parameters["node_type_id"] = node_type_id
elif cloud == "msa":
    node_type_id = new_cluster.get("azure:node_type_id")
    new_cluster["node_type_id"] = node_type_id
    new_cluster["azure_attributes"] = new_cluster.get("azure:azure_attributes")
    base_parameters["node_type_id"] = node_type_id
elif cloud == "gcp":
    node_type_id = new_cluster.get("gcp:node_type_id")
    new_cluster["node_type_id"] = node_type_id
    new_cluster["gcp_attributes"] = new_cluster.get("gcp:gcp_attributes")
    base_parameters["node_type_id"] = node_type_id
else:
    raise Exception(f"Unsupported cloud, found {cloud}")

for c in ["aws", "azure", "gcp"]:
    del new_cluster[f"{c}:node_type_id"]
    del new_cluster[f"{c}:{c}_attributes"]

print(json.dumps(job_def, indent=4))

job_name = job_def.get("name")
client.jobs.delete_by_name(job_name, success_only=False)
job = client.jobs.create_from_dict(job_def)
