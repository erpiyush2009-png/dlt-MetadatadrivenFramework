# Databricks notebook source
# MAGIC %md
# MAGIC This notebook prepare matadata tables 

# COMMAND ----------

dbutils.widgets.text('env',defaultValue='')

dbutils.widgets.text('Metadata_Catalog',defaultValue='')

dbutils.widgets.text('Metadata_Schema',defaultValue='_meta')


# COMMAND ----------

meta_catalog =dbutils.widgets.get("Metadata_Catalog")

meta_schema = dbutils.widgets.get("Metadata_Schema")

env =dbutils.widgets.get("env")

# COMMAND ----------

# Create Bronze Metadata Table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {meta_catalog}.{meta_schema}.bronze_dataflowspec_table (
    dataFlowId STRING,
    dataFlowGroup STRING,
    sourceFormat STRING,
    sourceDetails MAP < STRING,STRING >,
    highWaterMark MAP < STRING, STRING >,
    readerConfigOptions MAP < STRING, STRING >,
    cloudFileNotificationsConfig MAP < STRING,STRING >,
    targetFormat STRING,
    targetDetails MAP < STRING, STRING >,
    tableProperties MAP < STRING, STRING >,    
    schema STRING,
    partitionColumns ARRAY < STRING >,
    liquidClusteringColumns ARRAY < STRING >,
    cdcApplyChanges STRING,
    dataQualityExpectations STRING,
    quarantineTargetDetails MAP < STRING, STRING >,
    quarantineTableProperties MAP < STRING,STRING >,
    version STRING,
    createDate TIMESTAMP,
    createdBy STRING,
    updateDate TIMESTAMP,
    updatedBy STRING
    )
    TBLPROPERTIES (
    'delta.checkpoint.writeStatsAsJson' = 'false',
    'delta.checkpoint.writeStatsAsStruct' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.enableDeletionVectors' = 'false',
    'delta.feature.allowColumnDefaults' = 'supported',
    'delta.feature.appendOnly' = 'supported',
    'delta.feature.changeDataFeed' = 'supported',
    'delta.feature.checkConstraints' = 'supported',
    'delta.feature.columnMapping' = 'supported',
    'delta.feature.generatedColumns' = 'supported',
    'delta.feature.identityColumns' = 'supported',
    'delta.feature.invariants' = 'supported',
    'delta.minReaderVersion' = '3',
    'delta.minWriterVersion' = '7') """
)

# COMMAND ----------

# Create Silver Metadata Table
spark.sql(f'CREATE TABLE IF NOT EXISTS {meta_catalog}.{meta_schema}.silver_dataflowspec_table ( \
    dataFlowId STRING, \
    dataFlowGroup STRING, \
    sourceFormat STRING, \
    sourceDetails MAP < STRING, STRING >, \
    readerConfigOptions MAP < STRING, STRING >, \
    targetFormat STRING, \
    targetDetails MAP < STRING,STRING >, \
    tableProperties MAP < STRING,STRING >, \
    schema STRING, \
    selectExp ARRAY < STRING >, \
    whereClause ARRAY < STRING >, \
    partitionColumns ARRAY < STRING >,\
    liquidClusteringColumns ARRAY < STRING >,\
    cdcApplyChanges STRING, \
    materializedView STRING, \
    dataQualityExpectations STRING, \
    version STRING, \
    createDate TIMESTAMP, \
    createdBy STRING, \
    updateDate TIMESTAMP, \
    updatedBy STRING)'
    )

# COMMAND ----------

# Create Fhir Metadata Table
spark.sql(f'CREATE TABLE IF NOT EXISTS {meta_catalog}.{meta_schema}.fhir_dataflowspec_table ( \
    dataFlowId STRING, \
    dataFlowGroup STRING, \
    sourceFormat STRING, \
    sourceDetails MAP < STRING, STRING >, \
    readerConfigOptions MAP < STRING, STRING >, \
    targetFormat STRING, \
    targetDetails MAP < STRING,STRING >, \
    tableProperties MAP < STRING,STRING >, \
    schema STRING, \
    selectExp ARRAY < STRING >, \
    whereClause ARRAY < STRING >, \
    partitionColumns ARRAY < STRING >,\
    liquidClusteringColumns ARRAY < STRING >,\
    cdcApplyChanges STRING, \
    materializedView STRING, \
    dataQualityExpectations STRING, \
    version STRING, \
    createDate TIMESTAMP, \
    createdBy STRING, \
    updateDate TIMESTAMP, \
    updatedBy STRING)'
    )

# COMMAND ----------

# Create Data Integration Logs Table to hold High Watermarks
spark.sql(f"""CREATE TABLE IF NOT EXISTS {meta_catalog}.{meta_schema}.data_integration_logs (
  contract_id STRING NOT NULL,
  contract_version DECIMAL(7,3) NOT NULL DEFAULT 1.0,
  contract_major_version INT NOT NULL DEFAULT 1,
  watermark_column STRING,
  watermark_current_value STRING,
  watermark_next_value STRING,
  target_table STRING,
  source_file STRING,
  __insert_ts TIMESTAMP NOT NULL DEFAULT current_timestamp())
  TBLPROPERTIES (
  'delta.checkpoint.writeStatsAsJson' = 'false',
  'delta.checkpoint.writeStatsAsStruct' = 'true',
  'delta.columnMapping.mode' = 'name',
  'delta.enableDeletionVectors' = 'false',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.changeDataFeed' = 'supported',
  'delta.feature.checkConstraints' = 'supported',
  'delta.feature.columnMapping' = 'supported',
  'delta.feature.generatedColumns' = 'supported',
  'delta.feature.identityColumns' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7') """)
