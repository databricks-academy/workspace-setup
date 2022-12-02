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

lab_id = dbutils.widgets.get(WorkspaceHelper.PARAM_LAB_ID),
workspace_description = dbutils.widgets.get(WorkspaceHelper.PARAM_DESCRIPTION),
workspace_name = spark.conf.get("spark.databricks.workspaceUrl", default=dbgems.get_notebooks_api_endpoint()),
org_id = dbgems.get_tag("orgId", "unknown")

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
    workspace_name=workspace_name,
    org_id=org_id)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create The Three Class-Specific Cluster Policies
# MAGIC The following cells create the various cluster policies used by the class

# COMMAND ----------

ClustersHelper.create_all_purpose_policy(client, instance_pool_id)
ClustersHelper.create_jobs_policy(client, instance_pool_id)
ClustersHelper.create_dlt_policy(client, 
                                 instance_pool_id=None,
                                 lab_id=lab_id,
                                 workspace_description=workspace_description, 
                                 workspace_name=workspace_name,
                                 org_id=org_id)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Create Class-Shared Databricks SQL Warehouse/Endpoint
# MAGIC Creates a single wharehouse to be used by all students.
# MAGIC 
# MAGIC The configuration is derived from the number of students specified above.

# COMMAND ----------

# Not required for this course
# DA.workspace.warehouses.create_shared_sql_warehouse(name="DBAcademy Warehouse")

# COMMAND ----------

# MAGIC %md --i18n-a382c82f-6e5a-453c-b612-946e184d576c
# MAGIC 
# MAGIC ## Configure User Entitlements
# MAGIC 
# MAGIC Updates the entlitlements for the "**users**" group ensuring that they can access the Workspace and/or Databricks SQL view.

# COMMAND ----------

# DA.workspace.add_entitlement_workspace_access()
# DA.workspace.add_entitlement_databricks_sql_access()

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
# job_id = DA.workspace.databases.configure_permissions("Configure-Permissions")

# COMMAND ----------

# DA.client.jobs().delete_by_id(job_id)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Define Workspace-Setup Job
# MAGIC Creates an unscheduled job referencing this specific notebook.

# COMMAND ----------

# Create Job
# Bla, bla, bla

# COMMAND ----------

print(f"Setup completed {dbgems.clock_stopped(setup_start)}")
