from fhir_functions import fhir_cdc

fhir_resource_map = {
    "organization": fhir_cdc.update_fhir_organization_record,
}

