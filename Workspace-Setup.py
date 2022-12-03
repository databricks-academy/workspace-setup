# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Setup
# MAGIC This notebook should be run to prepare the workspace for a class.
# MAGIC 
# MAGIC The key changes this notebook makes includes:
# MAGIC * Updating user-specific grants such that they can create databases/schemas against the current catalog when they are not workspace-admins.
# MAGIC * Configures three cluster policies:
# MAGIC     * **DBAcademy** - which should be used on clusters running standard notebooks.
# MAGIC     * **DBAcademy Jobs-Only** - which should be used on workflows/jobs
# MAGIC     * **DBAcademy DLT-Only** - which should be used on DLT piplines (automatically applied)
# MAGIC * Create or update the shared **DBAcademy Warehouse** for use in Databricks SQL exercises
# MAGIC * Create the Instance Pool **DBAcademy** for use by students and the "student" and "jobs" policies.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Utility Methods
# MAGIC The following two utility methods work together ensure that the dbacademy library is reachable from this workspace and then define the pip command later used to attach the library to the current cluster..

# COMMAND ----------

# DBTITLE 1,validate_libraries()
def validate_libraries():
    import requests
    try:
        site = "https://github.com/databricks-academy/dbacademy"
        response = requests.get(site)
        error = f"Unable to access GitHub or PyPi resources (HTTP {response.status_code} for {site})."
        assert response.status_code == 200, "{error} Please see the \"Troubleshooting | {section}\" section of the \"Version Info\" notebook for more information.".format(error=error, section="Cannot Install Libraries")
    except Exception as e:
        if type(e) is AssertionError: raise e
        error = f"Unable to access GitHub or PyPi resources ({site})."
        raise AssertionError("{error} Please see the \"Troubleshooting | {section}\" section of the \"Version Info\" notebook for more information.".format(error=error, section="Cannot Install Libraries")) from e

# COMMAND ----------

# DBTITLE 1,build_pip_command()
def build_pip_command():
    version = spark.conf.get("dbacademy.library.version", "main")

    try:
        from dbacademy import dbgems

        installed_version = dbgems.lookup_current_module_version("dbacademy")
        if installed_version == version:
            pip_command = (
                "list --quiet"  # Skipping pip install of pre-installed python library
            )
        else:
            print(
                f"WARNING: The wrong version of dbacademy is attached to this cluster. Expected {version}, found {installed_version}."
            )
            print(f"Installing the correct version.")
            raise Exception("Forcing re-install")

    except Exception as e:
        # The import fails if library is not attached to cluster
        if not version.startswith("v"):
            library_url = (
                f"git+https://github.com/databricks-academy/dbacademy@{version}"
            )
        else:
            library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"

        default_command = f"install --quiet --disable-pip-version-check {library_url}"
        pip_command = spark.conf.get("dbacademy.library.install", default_command)

        if pip_command != default_command:
            print(
                f"WARNING: Using alternative library installation:\n| default: %pip {default_command}\n| current: %pip {pip_command}"
            )
        else:
            # We are using the default libraries; next we need to verify that we can reach those libraries.
            validate_libraries()
    
    return pip_command

# COMMAND ----------

# MAGIC %md 
# MAGIC # Install the dbacademy library
# MAGIC See also https://github.com/databricks-academy/dbacademy

# COMMAND ----------

pip_command = build_pip_command()
print(pip_command)

# COMMAND ----------

# MAGIC %pip $pip_command

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Required Parameters (e.g. Widgets)
# MAGIC The three variables defined by these widgets are used to configure our environment as a means of controlling class cost.

# COMMAND ----------

from dbacademy import dbgems
from dbacademy.dbhelper import WorkspaceHelper

# Start a timer so we can benchmark execution duration.
setup_start = dbgems.clock_start()

# Setup the widgets to collect required parameters.
dbutils.widgets.dropdown("configure_for", WorkspaceHelper.CONFIGURE_FOR_ALL_USERS, 
                         [WorkspaceHelper.CONFIGURE_FOR_ALL_USERS], "Configure For (required)")

# lab_id is the name assigned to this event/class or alternatively its class number
dbutils.widgets.text(WorkspaceHelper.PARAM_LAB_ID, "", "Lab/Class ID (optional)")

# a general purpose description of the class
dbutils.widgets.text(WorkspaceHelper.PARAM_DESCRIPTION, "", "Description (optional)")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Install Datasets
# MAGIC The main affect of this call is to pre-install the datasets.

# COMMAND ----------

from dbacademy.dbhelper import DBAcademyHelper
from dbacademy.dbhelper.dataset_manager_class import DatasetManager

course_config = {
    "apache-spark-programming-with-databricks": {},
    "data-analysis-with-databricks-sql": {
        "data_source_name": "data-analysis-with-databricks"
    },
    "data-engineer-learning-path": {},
    "data-engineering-with-databricks": {},
    "deep-learning-with-databricks": {},
    "introduction-to-python-for-data-science-and-data-engineering": {},
    "ml-in-production": {},
    "scalable-machine-learning-with-apache-spark": {},
}

for course, config in course_config.items():
    print(course)
    data_source_name = config.get("data_source_name", course)
    
    # TODO - parameterize default source
    datasets_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{data_source_name}"
    data_source_version = sorted([f.name[:-1] for f in dbutils.fs.ls(datasets_uri)])[-1]
    # TODO - parameterize default directory
    datasets_path = f"dbfs:/mnt/dbacademy-datasets/{data_source_name}/{data_source_version}"
    data_source_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{data_source_name}/{data_source_version}"

    print(f"| {data_source_uri}")
    print(f"| {datasets_path}")
    
    remote_files = DatasetManager.list_r(data_source_uri)
    
    dataset_manager = DatasetManager(data_source_uri=data_source_uri,
                                     staging_source_uri=None,
                                     datasets_path=datasets_path,
                                     remote_files=remote_files)
    
    dataset_manager.install_dataset(install_min_time=None,
                                    install_max_time=None,
                                    reinstall_datasets=False)
    
    print("-"*80)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Class Instance Pools
# MAGIC The following cell configures the instance pool used for this class

# COMMAND ----------

from dbacademy.dbhelper.clusters_helper_class import ClustersHelper
from dbacademy.dbrest import DBAcademyRestClient

client = DBAcademyRestClient()

instance_pool_id = ClustersHelper.create_named_instance_pool(
    client=client,
    name=ClustersHelper.POOL_DEFAULT_NAME,
    min_idle_instances=0,
    idle_instance_autotermination_minutes=15)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create The Three Class-Specific Cluster Policies
# MAGIC The following cells create the various cluster policies used by the class

# COMMAND ----------

ClustersHelper.create_all_purpose_policy(client=client, instance_pool_id=instance_pool_id)
ClustersHelper.create_jobs_policy(client=client, instance_pool_id=instance_pool_id)
ClustersHelper.create_dlt_policy(client=client, instance_pool_id=None)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Class-Shared Databricks SQL Warehouse/Endpoint
# MAGIC Creates a single wharehouse to be used by all students.
# MAGIC 
# MAGIC The configuration is derived from the number of students specified above.

# COMMAND ----------

from dbacademy.dbhelper.warehouses_helper_class import WarehousesHelper

WarehousesHelper.create_sql_warehouse(client=client,
                                      name=WarehousesHelper.WAREHOUSES_DEFAULT_NAME,
                                      auto_stop_mins=120,
                                      min_num_clusters=1,
                                      max_num_clusters=20,
                                      enable_serverless_compute=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Configure User Entitlements
# MAGIC 
# MAGIC Updates the entlitlements for the "**users**" group ensuring that they can access the Workspace and/or Databricks SQL view.

# COMMAND ----------

WorkspaceHelper.add_entitlement_workspace_access(client)
WorkspaceHelper.add_entitlement_databricks_sql_access(client)
WorkspaceHelper.add_entitlement_allow_cluster_create(client)
WorkspaceHelper.add_entitlement_allow_instance_pool_create(client)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Update Grants
# MAGIC This operation executes **`GRANT CREATE ON CATALOG TO users`** to ensure that students can create databases as required by this course when they are not admins.
# MAGIC 
# MAGIC Note: The implementation requires this to execute in another job and as such can take about three minutes to complete.

# COMMAND ----------

# Ensures that all users can create databases on the current catalog 
# for cases wherein the user/student is not an admin.
from dbacademy.dbhelper.databases_helper_class import DatabasesHelper
job_id = DatabasesHelper.configure_permissions(client, "Configure-Permissions", spark_version="10.4.x-scala2.12")
client.jobs().delete_by_id(job_id)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Define Workspace-Setup Job
# MAGIC Creates an unscheduled job referencing this specific notebook.

# COMMAND ----------

directory = "/Repos/DBAcademy"
if client.workspace().get_status(directory) is None:
    print(f"Creating: {directory}")
    client.workspace.mkdirs(directory)
    
repo_dir = f"{directory}/workspace-setup"
if client.workspace().get_status(repo_dir) is None:
    print(f"Importing to {repo_dir}")
    repo_url = "https://github.com/databricks-academy/workspace-setup.git"
    response = client.repos.create(path=repo_dir, url=repo_url)

# COMMAND ----------

job_name = "DBAcademy Workspace-Setup"

params = {
    "name": job_name,
    "timeout_seconds": 7200,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "Workspace-Setup",
            "notebook_task": {
                "notebook_path": "Workspace-Setup",
                "source": "GIT"
            },
            "job_cluster_key": "Workspace-Setup-Cluster",
            "timeout_seconds": 7200,
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "Workspace-Setup-Cluster",
            "new_cluster": {
                "spark_version": "11.3.x-scala2.12",
                "spark_conf": {
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode"
                },
                "custom_tags": {
                    "ResourceClass": "SingleNode"
                },
                "spark_env_vars": {
                    "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                },
                "data_security_mode": "SINGLE_USER",
                "runtime_engine": "STANDARD",
                "num_workers": 0
            }
        }
    ],
    "git_source": {
        "git_url": "https://github.com/databricks-academy/workspace-setup.git",
        "git_provider": "gitHub",
        "git_branch": "published"
    },
    "format": "MULTI_TASK"
}

cluster_params = params.get("job_clusters")[0].get("new_cluster")
if client.clusters().get_current_instance_pool_id() is not None:
    cluster_params["instance_pool_id"] = client.clusters().get_current_instance_pool_id()
else:
    cluster_params["node_type_id"] = client.clusters().get_current_node_type_id()

# COMMAND ----------

job = client.jobs.get_by_name(job_name)

if job is not None:
    print("Job already exists.")
    job_id = job.get("job_id")
else:
    print("Creating new job.")
    job_id = client.jobs().create_2_1(params)["job_id"]

dbgems.display_html(f"""
<html style="margin:0"><body style="margin:0"><div style="margin:0">
    See <a href="/#job/{job_id}" target="_blank">{job_name} ({job_id})</a>
</div></body></html>
""")

# COMMAND ----------

print(f"Setup completed {dbgems.clock_stopped(setup_start)}")
