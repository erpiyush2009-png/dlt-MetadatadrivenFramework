# Databricks notebook source
# MAGIC %md
# MAGIC # NOTEBOOK PURPOSE:
# MAGIC - This notebook tracks high watermark values from target tables and updates a central logging table
# MAGIC - High watermarks are used to track data processing progress and enable incremental loading
# MAGIC - Set up input parameters through widgets
# MAGIC
# MAGIC ##### This Notebook Assumes `data_integation_logs` table is created

# COMMAND ----------


from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, StringType


# COMMAND ----------


dbutils.widgets.text('Metadata_Catalog',defaultValue='oncare_integrated_data_hub_ist')  # Catalog containing metadata tables

dbutils.widgets.text('Metadata_Schema',defaultValue='audit_idh_cpin')  # Schema for metadata tables

dbutils.widgets.text('checkpoint_volume',defaultValue='watermark_checkpoint')  # Volume for streaming checkpoints

dbutils.widgets.text('Metadata_Table',defaultValue='bronze_dataflowspec_table')  # Table containing dataflow configurations

dbutils.widgets.text('integration_logs_table',defaultValue='data_integration_logs')  # Target table for logging watermarks

dbutils.widgets.text('dataFlowGroup',defaultValue='B1')  # Filter for specific dataflow group

dbutils.widgets.dropdown('streaming',choices=['true','false'],defaultValue='false')  # Filter for specific dataflow group

# COMMAND ----------


meta_catalog =dbutils.widgets.get("Metadata_Catalog")

meta_schema = dbutils.widgets.get("Metadata_Schema")

checkpoint_volume = dbutils.widgets.get("checkpoint_volume")

meta_table = dbutils.widgets.get("Metadata_Table")

dataFlowGroup = dbutils.widgets.get("dataFlowGroup")

integration_logs_table = dbutils.widgets.get("integration_logs_table")

stream_from_tables = dbutils.widgets.get("streaming")

# COMMAND ----------


def upsertToDelta(microBatchOutputDF,batchId):
    """
    Function to merge streaming watermark updates into the integration logs table.

    Args:
        microBatchOutputDF: DataFrame containing the batch of new watermark values
        batchId: ID of the current microbatch
    """
    microBatchOutputDF.createOrReplaceTempView("updates")
    print("Merging new watermark values into the integration logs table...")
    microBatchOutputDF.sparkSession.sql(
        f"""
        MERGE INTO {meta_catalog}.{meta_schema}.{integration_logs_table} t
        USING updates s
        ON s.contract_id = t.contract_id
        WHEN MATCHED 
        AND md5(CONCAT(
            coalesce(CAST(s.watermark_column AS STRING),""),
            coalesce(CAST(s.watermark_current_value AS STRING),""),
            coalesce(CAST(s.watermark_next_value AS STRING),""),
            coalesce(CAST(s.source_file AS STRING),"")
            )) != 
            md5(CONCAT(
                coalesce(CAST(t.watermark_column AS STRING),""),
                coalesce(CAST(t.watermark_current_value AS STRING),""),
                coalesce(CAST(t.watermark_next_value AS STRING),""),
                coalesce(CAST(t.source_file AS STRING),"")
                ))
                THEN UPDATE SET 
                watermark_column = s.watermark_column,
                watermark_current_value = s.watermark_current_value,
                watermark_next_value = s.watermark_next_value,
                source_file = s.source_file,
                `__insert_ts` = current_timestamp()

        WHEN NOT MATCHED THEN 
        INSERT 
        (contract_id,
        contract_version,
        contract_major_version,
        watermark_column,
        watermark_current_value,
        watermark_next_value,
        target_table,
        source_file,
        `__insert_ts`) 
        VALUES (s.contract_id,
        s.contract_version,
        s.contract_major_version,
        s.watermark_column,
        s.watermark_current_value,
        s.watermark_next_value,
        s.target_table,
        s.source_file,
        current_timestamp())
        """
    )


# Read the metadata table filtered by dataflow group
df = spark.read.table(f"{meta_catalog}.{meta_schema}.{meta_table}").where(
    (col("dataFlowGroup") == dataFlowGroup) & (col("highWaterMark").isNotNull())
)

# Extract target details and high watermark configuration
targets = df.select("targetDetails", "sourceFormat", "highWaterMark")

# Process each target table to track its watermark
for row in targets.collect():
    # Extract target table information
    catalog = row["targetDetails"]["database"].split(".")[0]
    schema = row["targetDetails"]["database"].split(".")[1]
    table = row["targetDetails"]["table"]
    full_target_table = f"{catalog}.{schema}.{table}"

    if (
        spark.sql(
            f"""
                 select count(*) from {full_target_table}
                 """
        ).collect()[0][0]
        == 0
    ):
        continue

    print(f"Tracking watermark for {full_target_table}")
    # Extract high watermark tracking information
    contract_id = row["highWaterMark"]["contract_id"]
    contract_version = row["highWaterMark"]["contract_version"]
    contract_major_version = row["highWaterMark"]["contract_major_version"]
    watermark_column = row["highWaterMark"]["watermark_column"]


    if stream_from_tables == 'false':
        # Define the schema based on the result DataFrame
        schema = StructType([
            StructField("contract_id", StringType(), True),
            StructField("contract_version", StringType(), True),
            StructField("contract_major_version", StringType(), True),
            StructField("watermark_column", StringType(), True),
            StructField("watermark_current_value", StringType(), True),
            StructField("watermark_next_value", StringType(), True),
            StructField("target_table", StringType(), True),
            StructField("source_file", StringType(), True)
        ])
        # Create an empty DataFrame with the defined schema
        results = spark.createDataFrame([], schema)
        result = spark.sql(f"""
                       select '{contract_id}' as contract_id,
                       '{contract_version}' as contract_version,
                       '{contract_major_version}' as contract_major_version,
                       '{watermark_column}' as watermark_column,
                       cast(max({watermark_column}) as string) as watermark_current_value,
                       concat("([{watermark_column}] > ",cast(max({watermark_column}) as string), ")") as watermark_next_value,
                       '{full_target_table}' as target_table,
                       max(source_file_path) as source_file
                       from {full_target_table}
                       group by all
                       """)
    
        results = results.union(result)
    if stream_from_tables == 'true':
        result = (
        spark.readStream.option("readChangeFeed", "true")
        .format("delta")
        .table(full_target_table)
        .groupBy()
        .agg(
            # Create tracking columns for the watermark log
            expr(f"'{contract_id}' as contract_id"),
            expr(f"'{contract_version}' as contract_version"),
            expr(f"'{contract_major_version}' as contract_major_version"),
            expr(f"'{watermark_column}' as watermark_column"),
            expr(f"cast(max({watermark_column}) as string) as watermark_current_value"),
            # Format the watermark as a SQL expression for future use (e.g., "[dtEvent] > '2024-12-23 11:59:25.713000'")
            expr(
                f"""concat("([{watermark_column}] > '",cast(max({watermark_column}) as string), "')") as watermark_next_value"""
            ),
            expr(f"'{full_target_table}' as target_table"),
            expr("max(file_path) as source_file"),
        )
        )
        #Write the watermark tracking information to the integration logs table
        #Uses foreachBatch to apply the upsert logic
        (
            result.writeStream.foreachBatch(upsertToDelta)
            .outputMode("update")
            .trigger(availableNow=True)  # Process available data and stop
            .option(
                "checkpointLocation",
                f"/Volumes/{meta_catalog}/{meta_schema}/{checkpoint_volume}/data_integrationg_logs_checkpoints/contract_id/{contract_id}",
            )
            .start()
        )

    if stream_from_tables == 'false':
        upsertToDelta(results,None)