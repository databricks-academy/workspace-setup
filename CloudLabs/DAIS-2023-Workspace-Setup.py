# Databricks notebook source
# MAGIC %md
# MAGIC # DAIS 2023 Workspace Setup
# MAGIC This notebook should be run to prepare the workspace for a class.
# MAGIC
# MAGIC The key changes this notebook makes includes:
# MAGIC * Install the specified datasets.
# MAGIC * Updating user-specific grants such that they can create databases/schemas against the current catalog when they are not workspace-admins.
# MAGIC * Create the Instance Pool **DBAcademy** for use by students and the "student" and "jobs" policies.
# MAGIC * Configures three cluster policies:
# MAGIC     * **DBAcademy** - which should be used on clusters running standard notebooks.
# MAGIC     * **DBAcademy Jobs** - which should be used on workflows/jobs
# MAGIC     * **DBAcademy DLT** - which should be used on DLT piplines (automatically applied)
# MAGIC
# MAGIC See https://docs.google.com/document/d/1gb2uLE69eZamw_pzL5q3QZwCK0A0SMLjjTtfGIHrf8I/edit

# COMMAND ----------

# MAGIC %md 
# MAGIC # Install the dbacademy library
# MAGIC See also https://github.com/databricks-academy/dbacademy

# COMMAND ----------

version = "v3.0.74"

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

# dbutils.widgets.removeAll()

# COMMAND ----------


import sys
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
    # dbutils.widgets.get(WorkspaceHelper.PARAM_DATASETS)
    # dbutils.widgets.get(WorkspaceHelper.PARAM_COURSES)
except:
  
    # lab_id is the name assigned to this event/class or alternatively its class number
    dbutils.widgets.text(WorkspaceHelper.PARAM_LAB_ID, "", "1. Lab/Class ID (optional)")

    # a general purpose description of the class
    dbutils.widgets.text(WorkspaceHelper.PARAM_DESCRIPTION, "", "2. Event Description (optional)")
    
    # The node type id that the cluster pool will be bound too
    dbutils.widgets.text(WorkspaceHelper.PARAM_NODE_TYPE_ID, "", "3. Node Type ID (required)")
    
    # A comma seperated list of spark versions to preload in the pool
    dbutils.widgets.text(WorkspaceHelper.PARAM_SPARK_VERSION, "", "4. Spark Versions (required)")
    
    # A comma seperated list of spark versions to preload in the pool
    # dbutils.widgets.text(WorkspaceHelper.PARAM_DATASETS, "", "5. Datasets (defaults to all)")
        
    # A comma seperated list of courseware URLs
    # dbutils.widgets.text(WorkspaceHelper.PARAM_COURSES, "", "5. Courses (defaults to none)")

# COMMAND ----------

if created_widgets:
    # This has to exist in a different cell or the widgets won't be created.
    raise Exception("Please fill out widgets at the top and then reexecute \"Run All\"")
else:
    # Start a timer so we can benchmark execution duration.
    setup_start = dbgems.clock_start()
    
    lab_id = dbgems.get_parameter(WorkspaceHelper.PARAM_LAB_ID, None)
    assert lab_id is not None, f"""The parameter "{WorkspaceHelper.PARAM_LAB_ID}" must be specified."""
    print("Lab ID:        ", lab_id or "None")
    
    workspace_description = dbgems.get_parameter(WorkspaceHelper.PARAM_DESCRIPTION, None)
    assert workspace_description is not None, f"""The parameter "{WorkspaceHelper.PARAM_DESCRIPTION}" must be specified."""
    print("Description:   ", workspace_description or "None")
    
    node_type_id = dbgems.get_parameter(WorkspaceHelper.PARAM_NODE_TYPE_ID, None)
    assert node_type_id is not None, f"""The parameter "{WorkspaceHelper.PARAM_NODE_TYPE_ID}" must be specified."""
    print("Node Type ID:  ", node_type_id or "None")
    
    spark_version = dbgems.get_parameter(WorkspaceHelper.PARAM_SPARK_VERSION, None)
    assert spark_version is not None, f"""The parameter "{WorkspaceHelper.PARAM_SPARK_VERSION}" must be specified."""
    print("Spark Version:", spark_version or "None")

    # datasets = dbgems.get_parameter(WorkspaceHelper.PARAM_DATASETS, None)
    # print("Datasets:      ", datasets or "All")    
        
    # courses = dbgems.get_parameter(WorkspaceHelper.PARAM_COURSES, None)
    # print("Courses:       ", courses or "None")        

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Create Class Instance Pools
# MAGIC The following cell configures the instance pool used for this class

# COMMAND ----------

from dbacademy.dbhelper.clusters_helper_class import ClustersHelper

# instance_pool_id = None  # Don't use a pool for DAIS

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
# MAGIC # Create The Three Class-Specific Cluster Policies
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
# MAGIC # Validate UC Configuration.
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

# WorkspaceHelper.uninstall_courseware(client, courses, subdirectory=None)
# WorkspaceHelper.install_courseware(client, courses, subdirectory=None)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # Install Datasets
# MAGIC If a specific dataset is not specified, all datasets will be installed.
# MAGIC
# MAGIC This includes the latest and latest-1 datasets to account for courses where-in to datasets are being used two versions of the same course in any given season of development.

# COMMAND ----------

# Not required for the quota test, but we will use the hard coded array for DAIS.

# They convention doesn't work because we have courses that are
# N-1 versions behind and subsequently may install the wrong dataset.
# datasets = courses  
# WorkspaceHelper.install_datasets(datasets)

# # WorkspaceHelper.install_datasets([
#     "example-course",
#     "apache-spark-programming-with-databricks",
#     "advanced-data-engineering-with-databricks",
#     "building-and-deploying-large-language-models-on-databricks",
#     "data-analysis-with-databricks",
#     "data-engineer-learning-path",
#     "data-engineering-with-databricks",
#     "deep-learning-with-databricks",
#     "introduction-to-python-for-data-science-and-data-engineering",
#     "ml-in-production",
#     "scalable-machine-learning-with-apache-spark",
# ])

# COMMAND ----------

print(f"Setup completed {dbgems.clock_stopped(setup_start)}")
