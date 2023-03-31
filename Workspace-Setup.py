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

version = spark.conf.get("dbacademy.library.version", "v3.0.68")

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
from dbacademy.dbrest import DBAcademyRestClient

# Used throughout for different operations
client = DBAcademyRestClient(throttle_seconds=1)

try:
    created_widgets=False
    dbutils.widgets.get(WorkspaceHelper.PARAM_LAB_ID)
    dbutils.widgets.get(WorkspaceHelper.PARAM_DESCRIPTION)
    dbutils.widgets.get(WorkspaceHelper.PARAM_NODE_TYPE_ID)
    dbutils.widgets.get(WorkspaceHelper.PARAM_SPARK_VERSION)
    dbutils.widgets.get(WorkspaceHelper.PARAM_DATASETS)
    dbutils.widgets.get(WorkspaceHelper.PARAM_COURSES)
except:
    created_widgets=True
    
    # lab_id is the name assigned to this event/class or alternatively its class number
    dbutils.widgets.text(WorkspaceHelper.PARAM_LAB_ID, "Unknown", "1. Lab/Class ID (optional)")

    # a general purpose description of the class
    dbutils.widgets.text(WorkspaceHelper.PARAM_DESCRIPTION, "Unknown", "2. Event Description (optional)")
    
    # The node type id that the cluster pool will be bound too
    if Cloud.current_cloud() == Cloud.AWS:   default_node_type_id = "i3.xlarge"
    elif Cloud.current_cloud() == Cloud.MSA: default_node_type_id = "Standard_DS3_v2"
    elif Cloud.current_cloud() == Cloud.GCP: default_node_type_id = "n1-standard-4"
    else: raise Exception(f"The cloud {Cloud.current_cloud()} is not supported.")
    dbutils.widgets.text(WorkspaceHelper.PARAM_NODE_TYPE_ID, default_node_type_id, "3. Node Type ID (required)")
    
    # A comma seperated list of spark versions to preload in the pool
    dbutils.widgets.text(WorkspaceHelper.PARAM_SPARK_VERSION, "11.3.x-cpu-ml-scala2.12", "4. Spark Versions (required)")
    
    # A comma seperated list of spark versions to preload in the pool
    dbutils.widgets.text(WorkspaceHelper.PARAM_DATASETS, "", "5. Datasets (defaults to all)")
        
    # A comma seperated list of courseware URLs
    dbutils.widgets.text(WorkspaceHelper.PARAM_COURSES, "", "6. DBC URLs (defaults to none)")

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
        
    installed_courses = dbgems.get_parameter(WorkspaceHelper.PARAM_COURSES, None)
    print("Courses:       ", installed_courses or "None")    

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Install Datasets
# MAGIC The main affect of this call is to pre-install the datasets.

# COMMAND ----------

WorkspaceHelper.install_datasets(installed_datasets)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC 
# MAGIC # Install Courses
# MAGIC The main affect of this call is to pre-install the specified courseware.
# MAGIC 
# MAGIC This parameter is expressed as a comma seperated list of courseware defintions.
# MAGIC 
# MAGIC Each courseware defintion consists of the following parameters:
# MAGIC * **course** The name of the course, lower cased, hyphenated, **required**.
# MAGIC * **version** The version of the specified course, optional, default is **vCURRENT**.
# MAGIC * **artifact** The specific file name of the DBC, optional, defaults to the one and only DBC in the CDS for the specified version.
# MAGIC * **token** The vender-specific API token to the CDS, **required**.
# MAGIC * **url** The URL from which the course will be installed, optional, defaults to **https&colon;//dev.training.databricks.com/api/v1/courses/download.dbc**
# MAGIC 
# MAGIC Examples:
# MAGIC * **course=<span style="color:blue">welcome</span>**
# MAGIC * **course=<span style="color:blue">example-course</span>&version=<span style="color:red">v1.1.6</span>**
# MAGIC * **course=<span style="color:blue">template-course</span>&version=<span style="color:red">v1.0.0</span>&artifact=<span style="color:green">template-course.dbc</span>**
# MAGIC * **<span style="color:orange">https&colon;//dev.training.databricks.com/api/v1/courses/download.dbc</span>?course=<span style="color:blue">ml-in-production</span>&version=<span style="color:red">v3.4.5</span>&artifact=<span style="color:green">ml-in-production-v3.4.5-notebooks.dbc</span>&token=<span style="color:brown">asfd123</span>**

# COMMAND ----------

# WorkspaceHelper.uninstall_courseware(client, installed_courses, subdirectory="dbacademy")
WorkspaceHelper.install_courseware(client, installed_courses, subdirectory="dbacademy")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Class Instance Pools
# MAGIC The following cell configures the instance pool used for this class

# COMMAND ----------

from dbacademy.dbhelper.clusters_helper_class import ClustersHelper

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
warehouse_id = WarehousesHelper.create_sql_warehouse(client=client,
                                                     name=WarehousesHelper.WAREHOUSES_DEFAULT_NAME,
                                                     auto_stop_mins=None,
                                                     min_num_clusters=1,
                                                     max_num_clusters=20,
                                                     enable_serverless_compute=True)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Validate UC Configuration.
# MAGIC This operation attempts to create a catalog and a table in that catalog.
# MAGIC 
# MAGIC If UC is not configured properly then this operation would be expected to fail.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS WORKSPACE_SETUP ;
# MAGIC SHOW DATABASES IN WORKSPACE_SETUP;
# MAGIC CREATE TABLE IF NOT EXISTS WORKSPACE_SETUP.default.test AS SELECT true AS test_passed;
# MAGIC SELECT * FROM WORKSPACE_SETUP.default.test;

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP CATALOG WORKSPACE_SETUP CASCADE;

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
    WorkspaceHelper.PARAM_DATASETS: installed_datasets,
    WorkspaceHelper.PARAM_COURSES: installed_courses
})
task_config.cluster.new(JobClusterConfig(cloud=Cloud.current_cloud(),
                                         spark_version="11.3.x-scala2.12",
                                         node_type_id="i3.xlarge",
                                         num_workers=0,
                                         autotermination_minutes=None))
None # Suppress output

# COMMAND ----------

if dbgems.get_tag("jobName") in [None, WorkspaceHelper.BOOTSTRAP_JOB_NAME]:
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
