import json
import logging
import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, expr,lit, struct, to_date, when, current_timestamp, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType
import requests
from azure.identity import ClientSecretCredential
from azure.core.exceptions import ClientAuthenticationError


logger = logging.getLogger('idh.silver_cdc')
logger.setLevel(logging.INFO)


# fhir_base_url = "https://cwridhhealthdataservice-cwidhhealth-ist.fhir.azurehealthcareapis.com"  # Replace with your actual FHIR base URL

# register as udf
@udf(returnType=StringType())
def update_fhir_organization_record(provider_id, fhir_base_url, fhir_base_scope, data, tenant_id, client_id, client_secret):
    """Sends a  request to update a patient record in FHIR."""
    
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)

    try:
        # Obtain a token
        token = credential.get_token(fhir_base_scope)
        access_token = token.token        
    except ClientAuthenticationError as e:
        access_token = None
        print(f"Authentication failed: {e}")


    # Set the headers for the FHIR request
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/fhir+json"
    }

    op_type = data["op_type"]

    # Construct the payload
    payload_prov = {
        "resourceType": "Organization",
        "id": f'{data["id"]}',

        "identifier": [{
                'system': 'http://idh.mcys.gov.on.ca/fhir/cpin-org-provider',
                'value': data["REFERENCE_NUMBER"]
        }],


        # Add other fields to update based on the data
        "name": data["ORGANIZATION_NAME"],
        "address": [{          
            'line': data["address_line"],
            'city': data["City_Name"],  
            'state': data["Province_Name"],
            'country': data["Coutry_EN_Name"],
            'postalCode': data["Postal_Code"],
            'use': data["address_use"],
            'type': data["address_type"]
        }],
        "active": data["LATEST_STATUS_EN_DESC"] == 'Open' or data["LATEST_STATUS_EN_DESC"] == 'Approved',

        "type":[{
            "coding":[{
                "system":"http://example.org/fhir/organization-type",
                "code":"prov",
                "display":"Provider"
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

    payload_gov = {
        "resourceType": "Organization",
        "id": f'{data["id"]}',

        "identifier": [{
                'system': 'http://idh.mcys.gov.on.ca/fhir/cpin-org-agency',
                'value': data["REFERENCE_NUMBER"]
        }],


        # Add other fields to update based on the data
        "name": data["ORGANIZATION_NAME"],
        "address": [{          
            'line': data["address_line"],
            'city': data["City_Name"],  
            'state': data["Province_Name"],
            'country': data["Coutry_EN_Name"],
            'postalCode': data["Postal_Code"],
            'use': data["address_use"],
            'type': data["address_type"]
        }],
        "active": True,

        "type":[{
            "coding":[{
                "system":"http://example.org/fhir/organization-type",
                "code":"govt",
                "display":"Government"
            }]
        }]         

    }

    payload = payload_prov if data["ORGANZATION_TYPE"] == 'prov' else payload_gov
    request_text = json.dumps(payload)

    try:
        
        url = f"{fhir_base_url}/Organization/{data["ORGANIZATION_ID"]}"
        
        if op_type == 'D':
            url = f"{fhir_base_url}/Organization?identifier={data["id"]}&_count=100"
            response = requests.delete(url, headers=headers)
        else:
            response = requests.put(url, data=json.dumps(payload), headers=headers)
        
        # response = requests.get(url, headers=headers)

        response.raise_for_status()  # Raise an error for bad responses
        if response.status_code == 200 or response.status_code == 201:
            response_text = f"Successfully updated provider {provider_id} and response details: {response.text}. payload: {payload}"
        elif response.status_code == 204:
            response_text = f"Successfully deleted provider {provider_id}. payload: {request_text}"
        else:
            response_text = f"Received unexpected status code {response.status_code} for provider {provider_id} with response details: {response.text}. payload: {request_text}"

        return response_text
    except Exception as e:
        return f"Error send request {url} body {request_text} with exception: {e}"
