# Databricks notebook source
# TODO for later, preheat the VMs in the classroom-setup script
# This one operation is unique to the hive_metastore, giving users the ability to create user-scoped schemas/databases
spark.sql("GRANT CREATE ON CATALOG hive_metastore TO users")
