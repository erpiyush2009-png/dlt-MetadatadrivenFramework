import json
import logging
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, expr,lit, struct, to_date, when, current_timestamp, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
import requests
from pyspark.sql import SparkSession

# Get or create a SparkSession
# In a DLT environment, getOrCreate() will generally return the existing SparkSession managed by DLT.
spark = SparkSession.builder \
    .appName("My DLT Spark Application") \
    .getOrCreate()  


class fhir_udf:

    _registry = {}

    @classmethod
    def register(cls, name):
        def decorator(func):
            cls._registry[name.upper()] = func
            return func
        return decorator

    @classmethod
    def run(cls, spark, target_table, bronze_database, silver_view):
        if target_table.upper() not in cls._registry:
            raise ValueError(f"Unsupported target_table: {target_table}")

        try:
            return cls._registry[target_table.upper()](
                spark, bronze_database, silver_view
            )
        except Exception as e:
            raise RuntimeError(
                f"Error running change function for {target_table}"
            ) from e
    @classmethod
    def get_udf(cls, name):
        if name.upper() not in cls._registry:
            raise ValueError(f"Unsupported UDF: {name}")
        return cls._registry[name.upper()]

@fhir_udf.register("ORGANIZATION")
def update_fhir_provider_record(fhir_base_url, provider_id, data, access_token):

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
   
    # fhir_base_url = "https://cwridhhealthdataservice-cwidhhealth-ist.fhir.azurehealthcareapis.com"  # Replace with your actual FHIR base URL
     
   
# Register the static method as a UDF
spark.udf.register("update_fhir_provider_record", fhir_udf.get_udf('organization'), StringType())


