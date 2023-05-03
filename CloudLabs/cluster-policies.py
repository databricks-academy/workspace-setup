from simplified_rest_client import SimpleRestClient

cluster_config = {
    "workspaceUrl": "https://redacted.cloud.databricks.com/",
    "workspaceToken": "REDACTED",
    "odlId": "11111",
    "odlTitle": "Data Engineering with Databricks - Test",
    "deploymentId": "GET-DEPLOYMENT-ID",
    "aadEmail": "GET-AZUSER-UPN",
    "driverSize": "i3.xlarge",
    "workerSize": "i3.xlarge",
    "runtimeVersion": "11.3.x-cpu-ml-scala2.12",
    "userAccessMode": "SINGLE_USER",
    "autoTerminationTime": 180,
    "clusterName": "Lab-Cluster-GET-DEPLOYMENT-ID",
    "clusterPolicy": "dbacademy",  # This is the new template deployment parameter
}


def deploy_cluster(config: dict[str, str]):
    workspace_url = config["WORKSPACE-URL"]
    workspace_token = config["WORKSPACE-TOKEN"]
    workspaces_api = SimpleRestClient(url=workspace_url, token=workspace_token)

    ###############################################################################################
    # Lookup the cluster policy, if specified
    ###############################################################################################
    cluster_policy_name = config.get("clusterPolicy", default=None)
    cluster_policy_id = None
    if config.get("cluster_policy") is not None:
        policies = workspaces_api.call("GET", "/2.0/policies/clusters/list").get("policies", default=[])
        for policy in policies:
            if policy["name"] == cluster_policy_name:
                cluster_policy_id = policy["policy_id"]
                break
        if cluster_policy_id is None:
            raise Exception("Unable to find cluster_policy with name: " + cluster_policy_name)

    ###############################################################################################
    # Create the user cluster
    #
    # Note, you likely set other cluster values too.  You should continue to do so.
    # The point of this example is purely to show you how to look up and apply the cluster policy.
    ###############################################################################################
    cluster_spec = {
        "cluster_name": config["clusterName"],
        "spark_conf": {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*, 4]"
        },
        "custom_tags": {
            "ResourceClass": "SingleNode",
            "ODL_ID": config["odlId"],
            "ODL_Title": config["odlTitle"],
            "Deployment_ID": config["deploymentId"],
        },
        "data_security_mode": config["userAccessMode"],
        # And many other values you might already have as configurable.
    }
    if config["userAccessMode"] == "SINGLE_USER":
        cluster_spec["single_user_name"] = config["aadEmail"]
    if cluster_policy_id is not None:
        cluster_spec["policy_id"] = cluster_policy_id

    # IMPORTANT NOTE: Many parameters once required are now optional because the cluster_policy might also set them.
    if config.get("autoTerminationTime", default=None):
        cluster_spec["autotermination_minutes"] = config["autoTerminationTime"]
    if config.get("runtimeVersion", default=None) is not None:
        cluster_spec["spark_version"] = config["runtimeVersion"]
    if config.get("driverSize", default=None):
        cluster_spec["driver_node_type_id"] = config["driverSize"]
    if config.get("num_workers", default=None):
        cluster_spec["num_workers"] = config["num_workers"]

    workspaces_api.call("POST", "2.0/clusters/create", cluster_spec)


if __name__ == "__main__":
    deploy_cluster(cluster_config)
