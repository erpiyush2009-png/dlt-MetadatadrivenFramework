# Databricks notebook source
# MAGIC %md
# MAGIC # DLT pipeline to read GG parquet files and load to bronze tables (Type-2)
# MAGIC
# MAGIC This Delta Live Tables (DLT) definition is executed using a pipeline defined in resources/idh_dab.pipeline.yml.

# COMMAND ----------


from dlt_helpers.populate_md import populate_fhir
import datetime
from pyspark.sql.functions import current_user
import json
import sys

# COMMAND ----------

dbutils.widgets.text('Source_System',defaultValue='idh_cpin')

dbutils.widgets.text('env',defaultValue='')

dbutils.widgets.text('Target_Catalog',defaultValue='')

dbutils.widgets.text('Source_Schema',defaultValue='')

dbutils.widgets.text('Target_Schema',defaultValue='')

dbutils.widgets.text('Metadata_Catalog',defaultValue='')

dbutils.widgets.text('Metadata_Schema',defaultValue='')

dbutils.widgets.text('Dataflow_Group',defaultValue='IDH')

# COMMAND ----------

source_system = dbutils.widgets.get("Source_System")

target_catalog =dbutils.widgets.get("Target_Catalog")

source_catalog = target_catalog

source_schema = dbutils.widgets.get("Source_Schema")

target_schema = dbutils.widgets.get("Target_Schema")

meta_catalog =dbutils.widgets.get("Metadata_Catalog")

meta_schema = dbutils.widgets.get("Metadata_Schema")

env =dbutils.widgets.get("env")

dataflow_group = dbutils.widgets.get("Dataflow_Group")

# COMMAND ----------

# Map silver view function with table name
# silver_table_wt_view_definitions = {
#    "PROVIDER": get_silver_table_view("provider")
# }

# silver_table_wt_keys = {    
#    "PROVIDER": ["PROVIDER_CONCERN_ROLE_KEY"],
# }

# get list of silver tables to process load
table_df = spark.sql(f'''
                        select distinct Schema_Name, Table_Name, PK_Name, Table_Type, FHIR_Scope, FHIR_URL, FHIR_Resource_Type from {meta_catalog}.idh_config.ref_fhir_tables 
                        where source_system='{source_system}' and is_enabled = 1 
                        ''')
table_list = table_df.collect()

# COMMAND ----------

schema_map = {
    "ORGANIZATION": """{
        "fields": [
            { "metadata": {}, "name": "ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "LAST_UPDATE_DATETIME", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "FHIR_SYNC_STATUS", "nullable": true, "type": "string" }
        ],
        "type": "struct"
    }"""
}

# COMMAND ----------


# for silver_table, keys in silver_table_wt_keys.items():
for row in table_list:
    fhir_table = row['Table_Name'].strip()  
    fhir_url = row['FHIR_URL'].strip()
    fhir_scope = row['FHIR_Scope'].strip()
    fhir_resource = row['FHIR_Resource_Type'].strip()
    keys = row['PK_Name'].split(',') if row['PK_Name'] else []  # Get the primary keys from the table definition, if not defined, it will be empty list.
    load_type = row['Table_Type'].lower().replace('type_','') if row['Table_Type'] else '2' # Load Type, if not defined, it will be SCD Type 2

    dataFlowId = f'3000-{fhir_table}' # Unique ID for the dataflow -- PK, 2000 is the prefix for Silver Dataflows
    dataFlowGroup = dataflow_group
    sourceFormat = "delta" # Reading from Bronze Layer Delta Table
    sourceDetails = {"database" : f"{source_catalog}.{source_schema}","schema": source_schema, "table": fhir_table} # Source Table Details
    readerConfigOptions = None 
    targetFormat = 'delta' # Writing to Silver Layer Delta Table
    targetDetails = {"database": f"{target_catalog}.{target_schema}", "schema":target_schema, "table":fhir_table, "fhir_url": fhir_url, "fhir_scope": fhir_scope, "fhir_resource": fhir_resource} # Target Table Details
    tableProperties = {"delta.enableChangeDataFeed": "true"}
    schema = schema_map.get(fhir_table.upper(), None)
    ## Select Expression to be used for eliminating columns, change column names, add new columns, change data types etc. here we are also generating a surrogate key using MD5 hash of all columns except the ones mentioned in the except clause and converting it to decimal(32,0)
    selectExp = None # ["MD5(CONCAT_WS('', id, client, status)) AS surrogate_key", "* EXCEPT (_rescued_data,processing_time)","current_timestamp() as processing_time"] #(REQUIRED)
    whereClause = None
    partitionColumns = None #Example: #['customer_id','operation_date'] Databricks Recommends to use Liquid Clustering instead of Partitioning
    liquidClusteringColumns = None #Example: #['customer_id','operation_date'] Databricks Highly Recommends using Liquid Clustering Doc: https://docs.databricks.com/aws/en/delta/clustering#choose-clustering-keys
    cdcApplyChanges = None
    materiazedView = None
    dataQualityExpectations = None
    quarantineTargetDetails = None
    quarantineTableProperties = None
    createDate = datetime.datetime.now()
    updateDate = datetime.datetime.now()
    createdBy = spark.range(1).select(current_user()).head()[0]
    updatedBy = spark.range(1).select(current_user()).head()[0]
    FHIR_MD_TABLE = SILVER_MD_TABLE = f"{meta_catalog}.{meta_schema}.fhir_dataflowspec_table" # type: ignore

    populate_fhir(FHIR_MD_TABLE,dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties, schema, selectExp,whereClause,partitionColumns,liquidClusteringColumns, cdcApplyChanges, materiazedView, dataQualityExpectations,createDate, createdBy,updateDate, updatedBy,spark)


