# Databricks notebook source
# MAGIC %pip install azure-identity

# COMMAND ----------

# Import DLT and src/idh_dab
import sys
import dlt
from pyspark.sql.functions import col, udf, struct, expr,lit, cast, concat, concat_ws, collect_list, when, current_timestamp, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, StringType, LongType
from datetime import datetime
import json
import requests
from azure.identity import ClientSecretCredential
from azure.core.exceptions import ClientAuthenticationError
from cdc_function.fhir_cdc import FHIRChangeFunctionRegistry

sys.path.append("./fhir_functions")

from fhir_functions import fhir_cdc_config

# COMMAND ----------

fhir_resource_map = fhir_cdc_config.fhir_resource_map


# COMMAND ----------

source_system = spark.conf.get("source_system", "idh_cpin")
dataflow_group = spark.conf.get("fhir.group", "IDH_FHIR")
tenantId = spark.conf.get("tenant_id", "cddc1229-ac2a-4b97-b78a-0e5cacb5865c")
fhir_base_scope = spark.conf.get("fhir_scope","https://cwridhhealthdataservice-cwidhhealth-ist.fhir.azurehealthcareapis.com/.default")
# fhir_base_url = spark.conf.get("fhir_url","https://cwridhhealthdataservice-cwidhhealth-ist.azurehealthcareapis.com")

# COMMAND ----------

#################### set env variables ####################
clientId = dbutils.secrets.get(scope="oncare-secrets", key="oncareappClientId")
clientSecret = dbutils.secrets.get(scope="oncare-secrets", key="oncareappClientSecret")


def get_token(tenantId, fhir_base_scope):
    # Create a credential object using the client credentials

    credential = ClientSecretCredential(tenantId, clientId, clientSecret)

    try:
        # Obtain a token
        token = credential.get_token(fhir_base_scope)
        return token.token        
    except ClientAuthenticationError as e:
        print(f"Authentication failed: {e}")
        

def create_view(query, view_name):
  @dlt.view(name=view_name)
  def _create_view():
    df = spark.sql (query)\
        .withColumn('Last_Update_DT', from_utc_timestamp(current_timestamp(), "America/New_York"))       
    return df

def create_change_view(table_name, silver_database, silver_schema_name, fhir_view, view_name):
    @dlt.view(name=view_name)
    def _create_change_view():
        # df = fhir_function_map[table_name](spark, silver_schema_name)
        df = FHIRChangeFunctionRegistry.run(spark, table_name, silver_database, fhir_view)
        return df
    

def create_fhir_table(target_database, table_name, access_token, fhir_base_url, resource_type=None):

    fhirTable = f"{target_database}.{table_name}"

    @dlt.table(
        name=fhirTable,
        comment="Logs the results of the FHIR API calls"
    )
    def sync_fhir_api():
        # Read the change data feed from the silver table
        # This captures only the latest changes
        cdc_df = dlt.readStream("vw_fhir_change_" + table_name)

        # Use a UDF to make the API call for each record
        # This must be done carefully to avoid performance issues
        # A more robust solution might use a foreachBatch with a queue
        # update_udf = udf(fhir_resource_map[table_name])
        update_udf = fhir_resource_map[table_name]

        return (
            cdc_df
            .withColumn("fhir_sync_status", update_udf(col("id"), lit(fhir_base_url), struct(col("*")), lit(access_token)))
            .select("id", "Last_Update_DT", "fhir_sync_status")
        )
    
def create_fhir_table_upd(target_database, table_name, tenant_id, client_id, client_secret, fhir_base_scope, fhir_base_url, resource_type=None):

    fhirTable = f"{target_database}.{table_name}"

    @dlt.table(
        name=fhirTable,
        comment="Logs the results of the FHIR API calls"
    )
    def sync_fhir_api():
        # Read the change data feed from the silver table
        # This captures only the latest changes
        cdc_df = dlt.readStream("vw_fhir_change_" + table_name)

        # Use a UDF to make the API call for each record
        # This must be done carefully to avoid performance issues
        # A more robust solution might use a foreachBatch with a queue
        # update_udf = udf(fhir_resource_map[table_name])
        update_udf = fhir_resource_map[table_name]

        return (
            cdc_df
            .withColumn("fhir_sync_status", update_udf(col("id"), lit(fhir_base_url), lit(fhir_base_scope), struct(col("*")), lit(tenant_id), lit(client_id), lit(client_secret)))
            .select("id", "Last_Update_DT", "fhir_sync_status")
        )    


sql_query = spark.sql(f'''
                        select sourceDetails, targetDetails, tableProperties, schema from audit_idh.fhir_dataflowspec_table 
                        where dataFlowGroup='{dataflow_group}' 
                        ''')
table_list = sql_query.collect()
# table_list = [row.Table_Name for row in table_list] 

access_token = get_token(tenantId, fhir_base_scope)

for row in table_list:
    # get PK from config table
    source_database = row.sourceDetails['database']  # Extract database name from 'database.schema'
    source_schema = row.sourceDetails['schema']
    schemaName = row.targetDetails['schema']
    target_database = row.targetDetails['database']  # Extract database name from 'database.schema'
    target_schema = row.targetDetails['schema']
    tableName = row.targetDetails['table']
    resource_type = row.targetDetails['fhir_resource']
    fhir_base_scope = row.targetDetails['fhir_scope']
    fhir_base_url = row.targetDetails['fhir_url']

    # Read source query file
    with open(f"./fhir_queries/query_{tableName}.sql", 'r') as file:
        query = file.read().replace('{silver_schema}.', f"{source_database}.")

    create_view(query, f"vw_fhir_{tableName}")

    create_change_view(tableName, source_database, source_schema, f"vw_fhir_{tableName}", f"vw_fhir_change_{tableName}")
   
    # create_fhir_table(target_database, tableName, f"`{access_token}`", f"`{fhir_base_url}`", resource_type)
    create_fhir_table_upd(target_database, tableName, tenantId, clientId, clientSecret, fhir_base_scope, fhir_base_url, resource_type)

