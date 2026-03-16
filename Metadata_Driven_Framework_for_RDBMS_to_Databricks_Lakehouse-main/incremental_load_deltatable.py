# Databricks notebook source
# MAGIC %run "/Workspace/MDFs/RDBMS to Lakehouse/functions/Functions"

# COMMAND ----------

tableConfigs = spark.read.format("delta").table("mdf1.bronze.meta_dataloadingmetrics").filter("IncrementalLoadFlag").collect()

for config in tableConfigs:
    if not table_exists(
        config["DestinationCatalogName"],
        config["DestinationSchemaName"],
        config["DestinationTableName"],
    ):
        create_delta_table(
            config["DestinationCatalogName"],
            config["DestinationSchemaName"],
            config["DestinationTableName"],
        )
        
    print(
        load_delta_table(
            config["SourceDatabaseName"],
            config["SourceSchemaName"],
            config["SourceTableName"],
            config["DestinationCatalogName"],
            config["DestinationSchemaName"],
            config["DestinationTableName"],
            config["IncrementalLoadFlag"],
            config["ReRunFlag"],
            config["RunDays"],
            config["WatermarkColumn"],
        )
    )

# COMMAND ----------


