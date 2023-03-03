# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Setup
# MAGIC This notebook should be run to prepare the workspace for a class.
# MAGIC 
# MAGIC The key changes this notebook makes includes:
# MAGIC * Updating user-specific grants such that they can create databases/schemas against the current catalog when they are not workspace-admins.
# MAGIC * Configures three cluster policies:
# MAGIC     * **DBAcademy** - which should be used on clusters running standard notebooks.
# MAGIC     * **DBAcademy Jobs** - which should be used on workflows/jobs
# MAGIC     * **DBAcademy DLT** - which should be used on DLT piplines (automatically applied)
# MAGIC * Create or update the shared **DBAcademy Warehouse** for use in Databricks SQL exercises
# MAGIC * Create the Instance Pool **DBAcademy** for use by students and the "student" and "jobs" policies.
# MAGIC 
# MAGIC See https://docs.google.com/document/d/1gb2uLE69eZamw_pzL5q3QZwCK0A0SMLjjTtfGIHrf8I/edit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Libraries and Build PIP Command
# MAGIC The following code works to ensure that the dbacademy library is reachable from this workspace and then define the pip command used to attach that library to the current cluster.

# COMMAND ----------

import requests

version = spark.conf.get("dbacademy.library.version", "v3.0.45")

try:
    from dbacademy import dbgems

    installed_version = dbgems.lookup_current_module_version("dbacademy")
    if installed_version == version:
        pip_command = "list --quiet"  # Skipping pip install of pre-installed python library
    else:
        print(f"WARNING: The wrong version of dbacademy is attached to this cluster. Expected {version}, found {installed_version}.")
        print(f"Installing the correct version.")
        raise Exception("Forcing re-install")

except Exception as e:
    # The import fails if library is not attached to cluster
    if not version.startswith("v"):
        library_url = f"git+https://github.com/databricks-academy/dbacademy@{version}"
    else:
        library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"

    default_command = f"install --quiet --disable-pip-version-check {library_url}"
    pip_command = spark.conf.get("dbacademy.library.install", default_command)

    if pip_command != default_command:
        print(f"WARNING: Using alternative library installation:\n| default: %pip {default_command}\n| current: %pip {pip_command}")
    else:
        # We are using the default libraries; next we need to verify that we can reach those libraries.
        try:
            site = "https://github.com/databricks-academy/dbacademy"
            response = requests.get(site)
            assert response.status_code == 200, f"Unable to access GitHub or PyPi resources (HTTP {response.status_code} for {site})."
        except Exception as e:
            if type(e) is AssertionError: raise e
            raise AssertionError(f"Unable to access GitHub or PyPi resources ({site}).") from e

# And print just for reference...
print(pip_command)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Install the dbacademy library
# MAGIC See also https://github.com/databricks-academy/dbacademy

# COMMAND ----------

# MAGIC %pip $pip_command

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Required Parameters (e.g. Widgets)
# MAGIC The three variables defined by these widgets are used to configure our environment as a means of controlling class cost.

# COMMAND ----------

from dbacademy import dbgems
from dbacademy.common import Cloud
from dbacademy.dbhelper import WorkspaceHelper

try:
    created_widgets=False
    dbutils.widgets.get(WorkspaceHelper.PARAM_LAB_ID)
    dbutils.widgets.get(WorkspaceHelper.PARAM_DESCRIPTION)
    dbutils.widgets.get(WorkspaceHelper.PARAM_NODE_TYPE_ID)
    dbutils.widgets.get(WorkspaceHelper.PARAM_SPARK_VERSION)
    dbutils.widgets.get(WorkspaceHelper.PARAM_DATASETS)
except:
    created_widgets=True
    
    # lab_id is the name assigned to this event/class or alternatively its class number
    dbutils.widgets.text(WorkspaceHelper.PARAM_LAB_ID, "Unknown", "1. Lab/Class ID (optional)")

    # a general purpose description of the class
    dbutils.widgets.text(WorkspaceHelper.PARAM_DESCRIPTION, "Unknown", "2. Event Description (optional)")
    
    # The node type id that the cluster pool will be bound too
    if Cloud.current_cloud() == "AWS":   default_node_type_id = "i3.xlarge"
    elif Cloud.current_cloud() == "MSA": default_node_type_id = "Standard_DS3_v2"
    elif Cloud.current_cloud() == "GCP": default_node_type_id = "n1-standard-4"
    else: raise Exception(f"The cloud {Cloud.current_cloud()} is not supported.")
    dbutils.widgets.text(WorkspaceHelper.PARAM_NODE_TYPE_ID, default_node_type_id, "3. Node Type ID (required)")
    
    # A comma seperated list of spark versions to preload in the pool
    dbutils.widgets.text(WorkspaceHelper.PARAM_SPARK_VERSION, "11.3.x-cpu-ml-scala2.12", "4. Spark Versions (required)")
    
    # A comma seperated list of spark versions to preload in the pool
    dbutils.widgets.text(WorkspaceHelper.PARAM_DATASETS, "", "4. Datasets (defaults to all)")

# COMMAND ----------

if created_widgets:
    # This has to exist in a different cell or the widgets won't be created.
    raise Exception("Please fill out widgets at the top and then reexecute \"Run All\"")
else:
    # Start a timer so we can benchmark execution duration.
    setup_start = dbgems.clock_start()
    
    lab_id = dbgems.get_parameter(WorkspaceHelper.PARAM_LAB_ID, None)
    print("Lab ID:        ", lab_id or "None")
    
    workspace_description = dbgems.get_parameter(WorkspaceHelper.PARAM_DESCRIPTION, None)
    print("Description:   ", workspace_description or "None")
    
    node_type_id = dbgems.get_parameter(WorkspaceHelper.PARAM_NODE_TYPE_ID, None)
    assert node_type_id is not None, f"The parameter \"Node Type ID\" must be specified."
    print("Node Type ID:  ", node_type_id or "None")
    
    spark_version = dbgems.get_parameter(WorkspaceHelper.PARAM_SPARK_VERSION, None)
    assert spark_version is not None, f"The parameter \"Spark Version\" must be specified."
    print("Spark Versions:", spark_version or "None")
    
    installed_datasets = dbgems.get_parameter(WorkspaceHelper.PARAM_DATASETS, None)
    print("Datasets:      ", installed_datasets or "All")    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Install Datasets
# MAGIC The main affect of this call is to pre-install the datasets.

# COMMAND ----------

from dbacademy.dbhelper import DBAcademyHelper
from dbacademy.dbhelper.dataset_manager_class import DatasetManager

if installed_datasets is not None and installed_datasets.strip() not in ("", "null", "None"):
    datasets = installed_datasets.split(",")
else:
    datasets = [
        "example-course",
        "apache-spark-programming-with-databricks",
        "data-analysis-with-databricks",
        "data-engineer-learning-path",
        "data-engineering-with-databricks",
        "deep-learning-with-databricks",
        "introduction-to-python-for-data-science-and-data-engineering",
        "ml-in-production",
        "scalable-machine-learning-with-apache-spark",
    ]
print(f"Installing {datasets}")


for dataset in datasets:
    if ":" in dataset:
        dataset, data_source_version = dataset.split(":")
    else:
        data_source_version = None

    if not data_source_version:
        datasets_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{dataset}"
        data_source_version = sorted([f.name[:-1] for f in dbutils.fs.ls(datasets_uri)])[-1]
    
    datasets_path = f"dbfs:/mnt/dbacademy-datasets/{dataset}/{data_source_version}"
    data_source_uri = f"wasbs://courseware@dbacademy.blob.core.windows.net/{dataset}/{data_source_version}"

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
    
    print("\n"+("-"*100)+"\n")

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
    idle_instance_autotermination_minutes=15,
    lab_id=lab_id,
    workspace_description=workspace_description,
    workspace_name=WorkspaceHelper.get_workspace_name(),
    org_id=dbgems.get_org_id(),
    node_type_id=dbgems.get_parameter(WorkspaceHelper.PARAM_NODE_TYPE_ID, None),
    preloaded_spark_version=spark_version)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create The Three Class-Specific Cluster Policies
# MAGIC The following cells create the various cluster policies used by the class

# COMMAND ----------

ClustersHelper.create_all_purpose_policy(client=client, 
                                         instance_pool_id=instance_pool_id, 
                                         spark_version=spark_version,
                                         autotermination_minutes_max=180,
                                         autotermination_minutes_default=120)

ClustersHelper.create_jobs_policy(client=client, 
                                  instance_pool_id=instance_pool_id, 
                                  spark_version=spark_version)

ClustersHelper.create_dlt_policy(client=client, 
                                lab_id=lab_id, 
                                workspace_description=workspace_description, 
                                workspace_name=WorkspaceHelper.get_workspace_name(),
                                org_id=dbgems.get_org_id())

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Class-Shared Databricks SQL Warehouse/Endpoint
# MAGIC Creates a single wharehouse to be used by all students.
# MAGIC 
# MAGIC The configuration is derived from the number of students specified above.

# COMMAND ----------

from dbacademy.dbhelper.warehouses_helper_class import WarehousesHelper

# Remove the existing Starter Warehouse
client.sql.endpoints.delete_by_name("Starter Warehouse")
client.sql.endpoints.delete_by_name("Serverless Starter Warehouse")

# Create the new DBAcademy Warehouse
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

from dbacademy.dbrest.jobs import JobConfig
from dbacademy.dbrest.clusters import JobClusterConfig

job_name = WorkspaceHelper.WORKSPACE_SETUP_JOB_NAME
job_config = JobConfig(job_name=job_name, timeout_seconds=15*60)

job_config.git_branch(provider="gitHub", url="https://github.com/databricks-academy/workspace-setup.git", branch="published")

task_config = job_config.add_task(task_key="Workspace-Setup", description="This job is used to configure the workspace per Databricks Academy's courseware requirements")
task_config.task.notebook("Workspace-Setup", source="GIT", base_parameters={
    WorkspaceHelper.PARAM_LAB_ID: lab_id,
    WorkspaceHelper.PARAM_DESCRIPTION: workspace_description,
    WorkspaceHelper.PARAM_NODE_TYPE_ID: node_type_id,
    WorkspaceHelper.PARAM_SPARK_VERSION: spark_version,
    WorkspaceHelper.PARAM_DATASETS: installed_datasets
})
task_config.cluster.new(JobClusterConfig(cloud=Cloud.current_cloud(),
                                         spark_version="11.3.x-scala2.12",
                                         node_type_id="i3.xlarge",
                                         num_workers=0,
                                         autotermination_minutes=None))

# COMMAND ----------

if dbgems.get_tag("jobName") == WorkspaceHelper.BOOTSTRAP_JOB_NAME:
    # Create the real job, deleting it if it already exists.
    print(f"Deleting {job_name}")
    client.jobs.delete_by_name(job_name, success_only=False)
    
    print(f"Creating {job_name}")
    job_id = client.jobs.create_from_config(job_config)
    dbgems.display_html(f"""
    <html style="margin:0"><body style="margin:0"><div style="margin:0">
        See <a href="/#job/{job_id}" target="_blank">{job_name} ({job_id})</a>
    </div></body></html>
    """)
else:
    print(f"Deleting {WorkspaceHelper.BOOTSTRAP_JOB_NAME}")
    client.jobs.delete_by_name(WorkspaceHelper.BOOTSTRAP_JOB_NAME, success_only=False)

# COMMAND ----------

print(f"Setup completed {dbgems.clock_stopped(setup_start)}")
