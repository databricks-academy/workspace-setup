from simplified_rest_client import SimpleRestClient


def get_workspace_config(workspace: SimpleRestClient):
    clusters = workspace.call("GET", "/api/2.0/clusters/list")
    warehouses = workspace.call("GET", "/api/2.0/sql/warehouses")
    users = workspace.call("GET", "/api/2.0/preview/scim/v2/Users")
    groups = workspace.call("GET", "/api/2.0/preview/scim/v2/Groups")
    metastore_assignment = workspace.call("GET", "/api/2.1/unity-catalog/current-metastore-assignment")
    if metastore_assignment is not None:
        metastore_id = metastore_assignment["metastore_id"]
        metastore = workspace.call("GET", f"/api/2.1/unity-catalog/metastores/{metastore_id}")
        metastore_perms = workspace.call("GET", f"/api/2.1/unity-catalog/permissions/metastore/{metastore_id}")
    else:
        metastore = None
        metastore_perms = None
    sql_settings = workspace.call("GET", "/api/2.0/sql/config/endpoints")  # Get the current endpoint configuration
    serverless_enabled = sql_settings.get("enable_serverless_compute", False)
    workspace_settings = workspace.call("GET", "/api/2.0/workspace-conf")
    jobs = workspace.call("GET", "/api/2.1/jobs/list")
    job_runs = workspace.call("GET", "/api/2.1/jobs/runs/list")
    return {
        "workspace_url": workspace.url,
        "clusters": clusters,
        "warerhouses": warehouses,
        "users": users,
        "groups": groups,
        "metastore": metastore,
        "metastore_permissions": metastore_perms,
        "serverless_enabled": serverless_enabled,
        "workspace_settings": workspace_settings,
        "jobs": jobs,
        "job_runs": job_runs,
    }


def print_workspace_config(workspace: SimpleRestClient):
    import json
    config = get_workspace_config(workspace)
    print(json.dumps(config, indent=4))


workspace = SimpleRestClient(url="https://hostname.cloud.databricks.com",
                             token="REDACTED")
print_workspace_config(workspace)
