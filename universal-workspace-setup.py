# Databricks notebook source
# MAGIC %md
# MAGIC # Workspace Setup
# MAGIC This notebook should be run to prepare the workspace for a class.
# MAGIC
# MAGIC The key changes this notebook makes includes:
# MAGIC * Create the Instance Pool **DBAcademy** for use by students and the "student" and "jobs" policies.
# MAGIC * Configures three cluster policies:
# MAGIC     * **DBAcademy** - which should be used on clusters running standard notebooks.
# MAGIC     * **DBAcademy Jobs** - which should be used on workflows/jobs.
# MAGIC     * **DBAcademy DLT** - which should be used on DLT piplines.
# MAGIC * Create or update the shared **DBAcademy Warehouse** for use in Databricks SQL exercises.
# MAGIC * Updating workspace-specific grants in the **main** and **hive_metastore** catalogs.

# COMMAND ----------

# MAGIC %md 
# MAGIC # Install the dbacademy library
# MAGIC See also https://github.com/databricks-academy/dbacademy

# COMMAND ----------

version = "v4.0.7"
library_url = f"https://github.com/databricks-academy/dbacademy/releases/download/{version}/dbacademy-{version[1:]}-py3-none-any.whl"
pip_command = f"install --quiet --disable-pip-version-check {library_url}"

# And print just for reference...
print(pip_command)

# COMMAND ----------

# MAGIC %pip $pip_command

# COMMAND ----------

# MAGIC %md
# MAGIC # Define Required Parameters (e.g. Widgets)
# MAGIC The three variables defined by these widgets are used to configure our environment as a means of controlling class cost.

# COMMAND ----------

# # Removes all widgets while testing
# dbutils.widgets.removeAll()

# COMMAND ----------

from dbacademy import dbgems
from dbacademy.common import Cloud
from dbacademy.dbhelper import WorkspaceHelper
from dbacademy.dbrest import DBAcademyRestClient

# Used throughout for different operations
client = DBAcademyRestClient(throttle_seconds=1)

try:
    created_widgets=False
    dbutils.widgets.get(WorkspaceHelper.PARAM_EVENT_ID)
    dbutils.widgets.get(WorkspaceHelper.PARAM_EVENT_DESCRIPTION)
    dbutils.widgets.get(WorkspaceHelper.PARAM_POOLS_NODE_TYPE_ID)
    dbutils.widgets.get(WorkspaceHelper.PARAM_DEFAULT_SPARK_VERSION)
    dbutils.widgets.get(WorkspaceHelper.PARAM_DATASETS)
    dbutils.widgets.get(WorkspaceHelper.PARAM_COURSES)
except:
    created_widgets = False if dbgems.is_job() else True
    
    # lab_id is the name assigned to this event/class or alternatively its class number
    dbutils.widgets.text(WorkspaceHelper.PARAM_EVENT_ID, "", "1. Class/Event/Lab ID (optional)")
    dbutils.widgets.text(WorkspaceHelper.PARAM_EVENT_DESCRIPTION, "", "2. Event Description (optional)")
    dbutils.widgets.text(WorkspaceHelper.PARAM_POOLS_NODE_TYPE_ID, "", "3. Pool's Node Type ID (required)")
    dbutils.widgets.text(WorkspaceHelper.PARAM_DEFAULT_SPARK_VERSION, "", "4. Default Spark Versions (required)")
    dbutils.widgets.text(WorkspaceHelper.PARAM_DATASETS, "", "5. Datasets (defaults to all)")
    dbutils.widgets.text(WorkspaceHelper.PARAM_COURSES, "", "6. DBC URLs (defaults to none)")


# COMMAND ----------

if created_widgets:
    # This has to exist in a different cell or the widgets won't be created.
    raise Exception("Please fill out widgets at the top and then reexecute \"Run All\"")
else:
    # Start a timer so we can benchmark execution duration.
    setup_start = dbgems.clock_start()
    
    lab_id = dbgems.get_parameter(WorkspaceHelper.PARAM_EVENT_ID, None)
    print("Lab ID:        ", lab_id or "None")
    
    workspace_description = dbgems.get_parameter(WorkspaceHelper.PARAM_EVENT_DESCRIPTION, None)
    print("Description:   ", workspace_description or "None")
    
    node_type_id = dbgems.get_parameter(WorkspaceHelper.PARAM_POOLS_NODE_TYPE_ID, None)
    assert node_type_id is not None, f"The parameter \"Node Type ID\" must be specified."
    print("Node Type ID:  ", node_type_id or "None")
    
    spark_version = dbgems.get_parameter(WorkspaceHelper.PARAM_DEFAULT_SPARK_VERSION, None)
    assert spark_version is not None, f"The parameter \"Spark Version\" must be specified."
    print("Spark Versions:", spark_version or "None")
    
    datasets = dbgems.get_parameter(WorkspaceHelper.PARAM_DATASETS, None)
    datasets = None if datasets is None or datasets.lower().strip() in ["", "none", "null"] else datasets
    print("Datasets:      ", datasets or "All ILT Datasets")
        
    courses = dbgems.get_parameter(WorkspaceHelper.PARAM_COURSES, None)
    courses = None if courses is None or courses.lower().strip() in ["", "none", "null"] else courses
    print("Courses:       ", courses or "None")

    workspace_name = WorkspaceHelper.get_workspace_name()
    print("Workspace Name:", workspace_name)


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
    workspace_name=workspace_name,
    org_id=dbgems.get_org_id(),
    node_type_id=node_type_id,
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
                                 workspace_name=workspace_name,
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
# MAGIC ## Configure Permissions
# MAGIC This is required for the DAWD class. While we thought it was addressed with a courseware update, testing prior to DAIS indicated that it was still required.  

# COMMAND ----------

endpoint = client.sql.endpoints.get_by_name(WarehousesHelper.WAREHOUSES_DEFAULT_NAME)
warehouse_id = endpoint.get("id")
statements = [
    "GRANT SELECT ON ANY FILE TO `users`"
]
for catalog in ["main", "hive_metastore"]:
    for statement in statements:
        client.sql.statements.execute(warehouse_id=warehouse_id, catalog=catalog, schema="default", statement=statement)

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
# MAGIC # Install Datasets
# MAGIC If a specific dataset is not specified, all datasets will be installed.
# MAGIC
# MAGIC This includes the latest and latest-1 datasets to account for courses where-in to datasets are being used two versions of the same course in any given season of development.

# COMMAND ----------

WorkspaceHelper.install_datasets(datasets)

# COMMAND ----------

print(f"Setup completed {dbgems.clock_stopped(setup_start)}")

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

WorkspaceHelper.uninstall_courseware(client, courses, subdirectory=None)

# COMMAND ----------

WorkspaceHelper.install_courseware(client, courses, subdirectory=None)
