import dlt
import json
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, expr,lit, struct, to_date, when, current_timestamp, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
import inspect

class FHIRChangeFunctionRegistry:
    _registry = {}

    @classmethod
    def register(cls, name):
        def decorator(func):
            cls._registry[name.upper()] = func
            return func
        return decorator

    @classmethod
    def run(cls, spark, target_table, silver_database, fhir_view):
        if target_table.upper() not in cls._registry:
            raise ValueError(f"Unsupported target_table: {target_table}")

        try:
            return cls._registry[target_table.upper()](
                spark, silver_database, fhir_view
            )
        except Exception as e:
            raise RuntimeError(
                f"Error running change function for {target_table}"
            ) from e

@FHIRChangeFunctionRegistry.register("ORGANIZATION")
def create_fhir_provider_change_view(spark, silver_database, fhir_view):    

    df_view= dlt.read(fhir_view)
    df_provider_changes = spark.readStream.option("readChangeData", "true").table(f"{silver_database}.provider")\
            .filter("_change_type in ('update_postimage', 'insert', 'delete')")\
            .select(col('PROVIDER_CONCERN_ROLE_ID').cast(LongType()).alias('ORGANIZATION_ID'), col('Source_Update_Type').alias('change_type'))

    df_org_unit_changes = spark.readStream.option("readChangeData", "true").table(f"{silver_database}.org_unit")\
            .filter("_change_type in ('update_postimage', 'insert', 'delete')")\
            .filter("ORGANISATION_NAME is not null")\
            .select(col('ORGANISATION_UNIT_ID').cast(LongType()).alias('ORGANIZATION_ID'), col('Source_Update_Type').alias('change_type'))

    df_view = df_view.withColumnRenamed("ORGANIZATION_ID", "View_ORGANIZATION_ID")
    columns = [col for col in df_view.columns if col not in ['View_ORGANIZATION_ID']]
    columns.append('ORGANIZATION_ID')

    df_change = df_provider_changes.union(df_org_unit_changes)
    
    df_update = df_change.join(df_view, df_view.View_ORGANIZATION_ID == df_provider_changes.ORGANIZATION_ID, how="inner")\
            .select(*columns, col("change_type"))\
            .withColumn('op_type', col("change_type"))\
            .withColumn('id', col("ORGANIZATION_ID"))\
            .withColumn('Last_Update_DT', from_utc_timestamp(current_timestamp(), "America/New_York"))\
            .filter(col("ORGANIZATION_ID").isNotNull())
    return df_update

# register as udf
@udf(returnType=StringType())
def update_fhir_provider_record(provider_id, data, fhir_base_url, access_token):
    """Sends a  request to update a patient record in FHIR."""
    
    # Set the headers for the FHIR request
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/fhir+json"
    }

    op_type = data["op_type"]

    # Construct the payload
    payload = {
        "resourceType": "Organization",
        "id": f'{data["id"]}',

        "identifier": [{
                'system': 'http://idh.mcys.gov.on.ca/fhir/cpin-org-provider',
                'value': data["REFERENCE_NUMBER"]
        }],


        # Add other fields to update based on the data
        "name": data["PROVIDER_NAME"],
        "address": data["addresses"],
        "active": data["LATEST_STATUS_EN_DESC"] == 'Open' or data["LATEST_STATUS_EN_DESC"] == 'Approved',

        "type":[{
            "coding":[{
                "system":"http://example.org/fhir/organization-type",
                "code":"prov",
                "display":"Healthcare Provider"
            }]
        }],          

        # Example of updating an extension or other FHIR field
        "extension": [
            {
                "url": 'http://idh.mcys.gov.on.ca/fhir/StructureDefinition/latestStatus',
                "valueString": data["LATEST_STATUS_EN_DESC"]
            }
        ]
    }
    try:
        
        url = f"{fhir_base_url}/Organization/{data["PROVIDER_CONCERN_ROLE_ID"]}"
        
        if op_type == 'D':
            url = f"{fhir_base_url}/Organization?identifier={data["id"]}&_count=100"
            response = requests.delete(url, headers=headers)
        else:
            response = requests.put(url, data=json.dumps(payload), headers=headers)
        
        # response = requests.get(url, headers=headers)

        response.raise_for_status()  # Raise an error for bad responses
        if response.status_code == 200 or response.status_code == 201:
            response_text = f"Successfully updated provider {provider_id} and response details: {response.text}"
        elif response.status_code == 204:
            response_text = f"Successfully deleted provider {provider_id}"
        else:
            response_text = f"Received unexpected status code {response.status_code} for provider {provider_id} with response details: {response.text}"

        return response_text
    except Exception as e:
        return f"Error send request {url} body {payload} with exception: {e}"
