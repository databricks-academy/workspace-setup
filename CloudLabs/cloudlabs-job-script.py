import json
import re
import requests
import time
import typing
from typing import Literal, Any

from simplified_rest_client import SimpleRestClient


lab_config = {
    "jobURL":
        "https://raw.githubusercontent.com/databricks-academy/workspace-setup/main/CloudLabs/cloudlabs-job-config.json",
    "WORKSPACE-URL": "https://redacted.cloud.databricks.com/",
    "WORKSPACE-TOKEN": "REDACTED",
    "ODL-ID": "11111",
    "ODL-TITLE": "Data Engineering with Databricks - Test",
    "workerSize": "i3.xlarge",
    "runtimeVersion": "11.3.x-cpu-ml-scala2.12",
}

Cloud = Literal["aws", "azure", "gcp"]
known_clouds = typing.get_args(Cloud)


def strip_cloud_specific_keys(cloud: Cloud, data: Any):
    """
    Recursively search the parsed json tree, locating keys like "aws:key-name", removing the cloud prefix
    and keeping the key only if the prefix matches the specified cloud.

    >>> strip_cloud_specific_keys('aws', {
    ...     'key1': 'abc',
    ...     'aws:key2': "It's AWS",
    ...     'azure:key2': "It's Azure"
    ... })
    {'key1': 'abc', 'key2': "It's AWS"}
    """
    if isinstance(data, list):
        for item in data:
            strip_cloud_specific_keys(cloud, item)
    elif isinstance(data, dict):
        for key, value in list(data.items()):
            for cloud_key in known_clouds:
                prefix = cloud_key + ":"
                if key.startswith(prefix):
                    if cloud_key == cloud:
                        new_key = key[len(prefix):]  # Remove the "aws:" prefix
                        data[new_key] = value
                    del data[key]
            strip_cloud_specific_keys(cloud, value)
    return data


def run_workspace_setup(config: dict[str, str]):
    workspace_url = config["WORKSPACE-URL"]
    workspace_token = config["WORKSPACE-TOKEN"]
    job_spec_url = config["jobURL"]
    workspaces_api = SimpleRestClient(url=workspace_url, token=workspace_token)

    ###############################################################################################
    # Download the job specification.
    ###############################################################################################
    print(f"""Downloading job specification from {job_spec_url}.""")
    response = requests.get(job_spec_url)
    response.raise_for_status()  # Raise an exception only if we didn't get a successful response.
    job_spec_text = response.text
    # Replace all {{variables}} using the config.
    for key, value in config.items():
        job_spec_text = job_spec_text.replace("{{" + key + "}}", value)
    # Verify all {{tags}} have been replaced with actual values
    if match := re.search(r"{{[a-zA-Z-]+}}", job_spec_text):
        raise ValueError("Unbound variable: " + match[0])
    # Parse the JSON
    job_spec = json.loads(job_spec_text)
    # Remove cloud-specific attributes for other clouds
    if ".cloud.databricks.com" in workspace_url:
        workspace_cloud = "aws"
    elif ".gcp.databricks.com" in workspace_url:
        workspace_cloud = "gcp"
    elif ".azuredatabricks.net" in workspace_url:
        workspace_cloud = "azure"
    else:
        raise Exception(f"Unsupported cloud, found {workspace_url}")
    strip_cloud_specific_keys(workspace_cloud, job_spec)
    # Retrieve the job name
    job_name = job_spec["name"]

    ###############################################################################################
    # Delete the preexisting "DBAcademy Workspace-Setup" job if it exists
    # In theory there should never be an existing job, since you're running this only once per
    # workspace.  But during script development, you might be running it multiple times, in which case
    # we want to ensure the old one is deleted.  This ensures the new job gets recreated with the
    # settings parameters but destroys the history needed when diagnosing problems.
    ###############################################################################################
    print(f"""Looking for the job "{job_name}" in {workspace_url}.""")
    jobs = workspaces_api.call("GET", "/api/2.1/jobs/list").get("jobs", list())
    # Search the list of jobs for the one matching the job_name and grab its job_id
    job_id = next((j.get("job_id") for j in jobs if j.get("settings").get("name") == job_name), None)
    if job_id is not None:
        # TODO: We should be checking for active runs before deleting, since deleting an active run could break stuff.
        workspaces_api.call("POST", "/api/2.1/jobs/delete", {"job_id": job_id})

    ###############################################################################################
    # Create and run the "DBAcademy Workspace-Setup" job
    ###############################################################################################
    print(f"""Creating the job "{job_name}" in {workspace_url}.""")
    job = workspaces_api.call("POST", "/api/2.1/jobs/create", job_spec)             # Create the job
    job_id = job.get("job_id")                                                      # Get the job id from the job
    run = workspaces_api.call("POST", "/api/2.1/jobs/run-now", {"job_id": job_id})  # Start the job
    run_id = run.get("run_id")

    ###############################################################################################
    # Wait for the "DBAcademy Workspace-Setup" job to finish execution: ~30 minutes
    ###############################################################################################
    print(f"""Waiting for the job "{job_name}" to complete in {workspace_url}.""", end="...")
    start = time.time()

    while True:
        response = workspaces_api.call("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}")
        life_cycle_state = response.get("state").get("life_cycle_state")
        if life_cycle_state not in ["PENDING", "RUNNING", "TERMINATING"]:
            job_state = response.get("state", {})
            job_result = job_state.get("result_state")
            job_message = job_state.get("state_message", "Unknown")
            break
        time.sleep(5)  # Slow it down a bit so that we don't hammer the REST endpoints.
        print(".", end="")

    print(f"{int(time.time() - start)} seconds")

    if job_result != "SUCCESS":
        print(job_message)
        raise Exception(f"""Expected the final state of the job "{job_name}" to be "SUCCESS", """
                        f"""found "{job_result}" in {workspace_url}""")

    print(f"""Job "{job_name}" completed successfully for {workspace_url}.""")


if __name__ == "__main__":
    run_workspace_setup(lab_config)
