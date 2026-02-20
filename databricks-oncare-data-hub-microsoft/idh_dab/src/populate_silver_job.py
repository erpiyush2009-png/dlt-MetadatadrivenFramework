# Databricks notebook source
# MAGIC %md
# MAGIC # DLT pipeline to read GG parquet files and load to bronze tables (Type-2)
# MAGIC
# MAGIC This Delta Live Tables (DLT) definition is executed using a pipeline defined in resources/idh_dab.pipeline.yml.

# COMMAND ----------


from dlt_helpers.populate_md import populate_silver
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

# sql query for silver table, e.x. Provider, it will look up the query file in ./silver_queries folder and file of query_<<table>>.sql

def get_silver_table_view(table_name):
    # Read source query file
    table_name = table_name.lower()

    try:
        with open(f"./silver_queries/query_{table_name}.sql", 'r') as file:
            sql_query = file.read()

    except FileNotFoundError:
        sql_query = None
        print(f"SQL query file for silver table {table_name} not found.")
    
    return sql_query  

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
                        select distinct Schema_Name, Table_Name, PK_Name, Execution_Path, Table_Type from {meta_catalog}.idh_config.ref_silver_tables 
                        where source_system='{source_system}' and is_enabled = 1 
                        ''')
table_list = table_df.collect()

# COMMAND ----------

schema_map = {
    "PROVIDER": """
    {
        "fields": [
            {"metadata": {},"name": "PROVIDER_CONCERN_ROLE_KEY","nullable": true,"type": "long"},
            {"metadata": {},"name": "AREA_SSVD_INFO_TXT_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "CLIENT_INFO_TEXT_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "CONCERN_ROLE_KEY","nullable": true,"type": "long"},
            {"metadata": {},"name": "CONSENT_FOR_CAPTURE_IND","nullable": true,"type": "string"},
            {"metadata": {},"name": "CONSENT_FOR_SEARCH_IND","nullable": true,"type": "string"},
            {"metadata": {},"name": "CURRENCY_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "EMAIL_ADDRESS_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "END_DATETIME","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "ENQUIRY_KEY","nullable": true,"type": "long"},
            {"metadata": {},"name": "ENROLMENT_DATETIME","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "LATEST_STATUS_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "LATEST_STATUS_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "LATEST_STATUS_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PAYMENT_FREQUENCY_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "PAYMENT_METHOD_CODE","nullable": true,"type": "long"},
            {"metadata": {},"name": "PHYSICAL_CAPACITY_VALUE","nullable": true,"type": "integer"},
            {"metadata": {},"name": "PREF_SE_METHOD_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "PREF_SE_METHOD_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PREF_SE_METHOD_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PRIM_PROVIDER_TYPE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "PROVIDER_CONCERN_ROLE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "PROVIDER_ENQUIRY_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "PROVIDER_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "PROVIDER_UPPER_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "REFERENCE_NUMBER","nullable": false,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "ALLEGATION_ROLE": """
    {
        "fields": [
            {"metadata": {},"name": "CASE_PARTICIPANT_ROLE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "CONCERN_ROLE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "ROLE_TYPE_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "ROLE_TYPE_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "CURRENCY_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "ADMINISTRATION_CONCERN_ROLE": """
    {
        "fields": [
            {"metadata": {},"name": "ADMINISTRATION_CONCERN_ROLE_KEY","nullable": true,"type": "long"},
            {"metadata": {},"name": "CONCERN_ROLE_KEY","nullable": true,"type": "long"},
            {"metadata": {},"name": "ADMINISTRATION_CONCERN_ROLE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "ADMINISTRATION_ROLE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "CONCERNROLE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "END_DATE","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "START_DATE","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "TYPE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "TYPE_CODE_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "TYPE_CODE_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "ORG_OBJECT_LINK_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "STATUS_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "STATUS_CODE_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "STATUS_CODE_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "USER_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "ALTERNATE_NAME": """
    {
        "fields": [
            {"metadata": {},"name": "ALTERNATE_NAME_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "COMMENTS","nullable": true,"type": "string"},
            {"metadata": {},"name": "CONCERN_ROLE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "FIRST_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "FIRSTNAME_PHONETIC_ENC","nullable": true,"type": "string"},
            {"metadata": {},"name": "FULL_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "INITIALS","nullable": true,"type": "string"},
            {"metadata": {},"name": "LAST_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "NAME_STATUS_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "NAME_SUFFIX_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "NAME_TYPE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "NAME_TYPE_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "NAME_TYPE_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "OTHER_FIRST_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "PHONETIC_ENCODING_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "RECORD_STATUS_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "RECORD_STATUS_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "SINGLE_NAME_IND","nullable": true,"type": "string"},
            {"metadata": {},"name": "TITLE_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "UPPER_FIRST_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "UPPER_FULL_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "UPPER_LAST_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "ATTACHMENT": """
    {
        "fields": [
            {"metadata": {},"name": "ATTACHMENT_KEY","nullable": true,"type": "long"},
            {"metadata": {},"name": "ATTACHMENT_FILE_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "ATTACHMENT_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "ATTACHMENT_STATUS_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "ATTACHMENT_STATUS_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "ATTACHMENT_STATUS_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "DOCUMENT_TYPE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "DOCUMENT_TYPE_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "DOCUMENT_TYPE_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "FILE_LOCATION_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "FILE_REFERENCE_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "RECEIPT_DATE","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "ADDRESS": """
    {
        "fields": [
            {"metadata": {},"name": "Address_Key","nullable": true,"type": "long"},
            {"metadata": {},"name": "Address_Id","nullable": true,"type": "long"},
            {"metadata": {},"name": "Address_Layout_Type","nullable": true,"type": "string"},
            {"metadata": {},"name": "Country_Code","nullable": true,"type": "string"},
            {"metadata": {},"name": "Modifiable_Ind","nullable": true,"type": "string"},
            {"metadata": {},"name": "Address1_Text","nullable": true,"type": "string"},
            {"metadata": {},"name": "Address2_Text","nullable": true,"type": "string"},
            {"metadata": {},"name": "APT_Number","nullable": true,"type": "string"},
            {"metadata": {},"name": "Coutry_EN_Name","nullable": true,"type": "string"},
            {"metadata": {},"name": "Coutry_FR_Name","nullable": true,"type": "string"},
            {"metadata": {},"name": "City_Name","nullable": true,"type": "string"},
            {"metadata": {},"name": "PO_Box_Number","nullable": true,"type": "string"},
            {"metadata": {},"name": "Postal_Code","nullable": true,"type": "string"},
            {"metadata": {},"name": "POSTAL_FSA_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "POSTAL_LDU_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "Province_Name","nullable": true,"type": "string"},
            {"metadata": {},"name": "State_Name","nullable": true,"type": "string"},
            {"metadata": {},"name": "Zip_Code","nullable": true,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "CAMS_USER_PROFILE_ROLE": """
    {
        "fields": [
            {"metadata": {},"name": "AGENCY_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "APP_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "APP_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "APP_TYPE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "APPLICATION_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "FIRST_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "GUID_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "IS_ACTIVE_IND","nullable": true,"type": "string"},
            {"metadata": {},"name": "IS_PRESENT_FLAG","nullable": true,"type": "string"},
            {"metadata": {},"name": "LAST_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "LOCATION_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "LOCATION_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "ROLE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "Description","nullable": true,"type": "string"},
            {"metadata": {},"name": "ROLE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "USER_ACCESS_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "USER_NAME_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "USER_PROFILE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "WORK_EMAIL_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "CASE_USER_ROLE": """
    {
        "fields": [
            {"metadata": {},"name": "USER_KEY","nullable": false,"type": "integer"},
            {"metadata": {},"name": "USER_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "CU_ROLE_TYPE_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "CU_ROLE_TYPE_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "REASSIGN_REASON_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "REASSIGN_REASON_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "RECORD_STATUS_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "RECORD_STATUS_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "CONCERN_ROLE": """
    {
        "fields": [
            {"metadata": {},"name": "CONCERN_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "CONCERN_ROLE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "CONCERN_ROLE_KEY","nullable": true,"type": "long"},
            {"metadata": {},"name": "CONCERN_ROLE_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "CONCERN_ROLE_TYPE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "CONCERN_ROLE_TYPE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "CONCERN_ROLE_TYPE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "CR_COMMENTS_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "CR_PRIM_ALTERNATE_ID","nullable": true,"type": "string"},
            {"metadata": {},"name": "CREATION_DATE","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "END_DATE","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "PREF_COMM_FROM_DATE","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "PREF_COMM_METHOD_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "PREF_COMM_METHOD_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PREF_COMM_METHOD_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PREF_COMM_TO_DATE","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "PREF_LANGUAGE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "PREF_LANGUAGE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PREF_LANGUAGE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PREF_PUBLIC_OFFICE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "PRIM_EMAIL_ADDRESS_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "PRIM_EMAIL_ADDRESS_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "PRIM_PHONE_NUMBER_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "PRIMARY_ADDRESS_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "REG_USER_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "REGISTRATION_DATE","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "SENSITIVITY_IND","nullable": true,"type": "string"},
            {"metadata": {},"name": "START_DATE","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "STATUS_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "STATUS_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "STATUS_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "CONCERN_ROLE_EMAIL": """
    {
        "fields": [
            {"metadata": {},"name": "CR_EMAIL_ADDRESS_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "EMAIL_ADDRESS_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "CONCERN_ROLE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "COMMENT_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "EMAIL_ADDRESS_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "STATUS_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "STATUS_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "STATUS_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "TYPE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "TYPE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "TYPE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "CONCERN_ROLE_PHONE": """
    {
        "fields": [
            {"metadata": {},"name": "CONCERN_ROLE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "CR_PHONE_NUMBER_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "END_DATE","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "COMMENTS_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "PHONE_AREA_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "PHONE_COUNTRY_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "PHONE_EXTENSION_NUM","nullable": true,"type": "string"},
            {"metadata": {},"name": "PHONE_NUMBER","nullable": true,"type": "string"},
            {"metadata": {},"name": "PHONE_NUMBER_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "START_DATE","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "STATUS_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "STATUS_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "STATUS_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "TYPE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "TYPE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "TYPE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "CONTACT_LOG_CONCERNING": """
    {
        "fields": [
            {"metadata": {},"name": "CONTACTLOGCONCERNID","nullable": true,"type": "long"},
            {"metadata": {},"name": "CL_CONCERNING_STATUS_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "CL_CONCERNING_STATUS_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "CONCERN_ROLE_TYPE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "CONCERN_ROLE_TYPE_EN_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "CONCERN_ROLE_TYPE_FR_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "ENQUIRY": """
    {
        "fields": [
            {"metadata": {},"name": "ENQUIRY_KEY","nullable": true,"type": "long"},
            {"metadata": {},"name": "ABORIGINAL_ANCESTRY_LIST","nullable": true,"type": "string"},
            {"metadata": {},"name": "ADDITIONAL_INFORMATION_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "ADDITIONAL_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "ATTENDED_MEETING_IND","nullable": true,"type": "string"},
            {"metadata": {},"name": "AVAILABILITY_FOR_CONTACT_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "CATEGORY_PERIOD_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "CLOSURE_COMMENTS_TEXT","nullable": true,"type": "string"},
            {"metadata": {},"name": "CLOSURE_REASON_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "CLOSURE_REASON_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "CLOSURE_REASON_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "CONFIRMED_MEETING_DETAILS_IND","nullable": true,"type": "string"},
            {"metadata": {},"name": "CONSENT_FOR_CAPTURE_IND","nullable": true,"type": "string"},
            {"metadata": {},"name": "CONSENT_FOR_SEARCH_IND","nullable": true,"type": "string"},
            {"metadata": {},"name": "EMAIL_ADDRESS_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "ENQUIRIED_BY_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "ENQUIRY_END_DATE","nullable": true,"type": "string"},
            {"metadata": {},"name": "ENQUIRY_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "ENQUIRY_REFERENCE_NUMBER","nullable": true,"type": "string"},
            {"metadata": {},"name": "ENQUIRY_START_DATE","nullable": true,"type": "string"},
            {"metadata": {},"name": "HOME_ADDRESS_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "HOME_PHONE_NUMBER_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "HOUSEHOLD_CHILDREN_COUNT","nullable": true,"type": "long"},
            {"metadata": {},"name": "INDIGENOUS_GROUP_LIST","nullable": true,"type": "string"},
            {"metadata": {},"name": "INDIGENOUS_PERSON_IND","nullable": true,"type": "string"},
            {"metadata": {},"name": "MOBILE_PHONE_NUMBER_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "OBTAINED_APPL_FORM_IND","nullable": true,"type": "string"},
            {"metadata": {},"name": "OLD_ADDITIONAL_INFORMATION_TEXT","nullable": false,"type": "string"},
            {"metadata": {},"name": "ORG_OBJECT_LINK_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "OWNER_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "PREF_COMMUNICATION_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "PREF_COMMUNICATION_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PREF_COMMUNICATION_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PREF_LANGUAGE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "PREF_LANGUAGE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PREF_LANGUAGE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PREF_TRAINING_SESSION_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "REASON_FOR_ENQUIRY_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "REASON_FOR_ENQUIRY_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "REASON_FOR_ENQUIRY_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "RECORD_STATUS_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "RECORD_STATUS_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "RECORD_STATUS_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "SCHEDULED_MEETING_DESC","nullable": true,"type": "string"},
            {"metadata": {},"name": "UPPER_NAME","nullable": true,"type": "string"},
            {"metadata": {},"name": "WORK_ADDRESS_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "WORK_PHONE_NUMBER_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "IDBD": """
    {
        "fields": [
            {"metadata": {},"name": "CONCERNROLEID","nullable": true,"type": "long"},
            {"metadata": {},"name": "ABORIG_IDENTITY_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "ABORIG_IDENTITY_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "ABORIG_TYPE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "ABORIG_TYPE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "BIRTH_SEX_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "BIRTH_SEX_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "BIRTH_YEAR_TYPE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "BIRTH_YEAR_TYPE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "BORN_CANADA_PROV_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "BORN_CANADA_PROVI_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "BORN_COUNTRY_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "BORN_COUNTRY_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "BORN_IN_CANADA_TYPE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "BORN_IN_CANADA_TYPE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "CHANGE_REASON_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "CHANGE_REASON_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "CONSENT_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "CONSENT_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "DISABILITY_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "DISABILITY_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "FAMILY_STATUS_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "FAMILY_STATUS_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "MARITAL_STATUS_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "MARITAL_STATUS_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "POSTAL_CODE_TYPE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "POSTAL_CODE_TYPE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "ORG_OBJECT_LINK": """
    {
        "fields": [
            {"metadata": {},"name": "ORG_OBJECT_LINK_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "ORG_OBJECT_REFERENCE_ID","nullable": true,"type": "long"},
            {"metadata": {},"name": "ORG_OBJECT_TYPE_CODE","nullable": true,"type": "string"},
            {"metadata": {},"name": "ORG_OBJECT_TYPE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "ORG_OBJECT_TYPE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "PERSON": """
    {
        "fields": [
            {"metadata": {},"name": "CONCERNROLEID","nullable": true,"type": "long"},
            {"metadata": {},"name": "CREATION_DATETIME","nullable": true,"type": "timestamp"},
            {"metadata": {},"name": "COUNTRY_BIRTH_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "COUNTRY_BIRTH_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "ETHNIC_ORIGIN_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "ETHNIC_ORIGIN_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "ETHNIC_ORIGIN_TAB_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "ETHNIC_ORIGIN_TAB_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "FIRST_LANGUAGE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "FIRST_LANGUAGE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "FNIM_IDENTITY_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "FNIM_IDENTITY_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "GENDER_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "GENDER_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "INDIGENOUS_GROUP_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "INDIGENOUS_GROUP_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "INDIGENOUS_GROUP2_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "INDIGENOUS_GROUP2_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "LIVING_OFF_RESERVE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "LIVING_OFF_RESERVE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "MARITAL_STATUS_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "MARITAL_STATUS_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "NATIONALITY_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "NATIONALITY_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "NATIVE_STATUS_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "NATIVE_STATUS_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PERSON_TYPE_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "PERSON_TYPE_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "POPULATION_GROUP_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "POPULATION_GROUP_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "RELIGION_TAB_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "RELIGION_TAB_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "SPECIAL_INTEREST_EN_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "SPECIAL_INTEREST_FR_DESC","nullable": false,"type": "string"},
            {"metadata": {},"name": "Source_Update_Type","nullable": false,"type": "string"},
            {"metadata": {},"name": "Effective_Start_DT","nullable": false,"type": "timestamp"},
            {"metadata": {},"name": "Last_Update_DT","nullable": false,"type": "timestamp"}
        ],
        "type": "struct"
    }
    """,
    "PROVIDER_OFFERING": """
    {
        "fields": [
            { "metadata": {}, "name": "PROVIDER_OFFERING_KEY", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "CLIENT_FEE_INFO_TEXT_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "COMMENTS_TEXT", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "DENIAL_REASON_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "DENIAL_REASON_EN_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "DENIAL_REASON_FR_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "DOCS_REQD_INFO_TEXT_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "ELIGIBILITY_INFO_TEXT_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "END_DATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "END_REASON_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "END_REASON_EN_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "END_REASON_FR_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "INTK_PROC_INFO_TEXT_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PROVIDER_CONCERN_ROLE_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PROVIDER_OFFERING_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "RECORD_STATUS_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_EN_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_FR_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SERVICE_OFFERING_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "START_DATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "Source_Update_Type", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Effective_Start_DT", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Last_Update_DT", "nullable": false, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,
    "SERVICE_OFFERING": """
    {
        "fields": [
            { "metadata": {}, "name": "SERVICE_OFFERING_KEY", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "AVAIL_CHECK_EXCL_IND", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "COMMENTS_TEXT", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "DELIVERY_FREQ_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "DELIVERY_FREQ_EN_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "DELIVERY_FREQ_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "DELIVERY_TYPE_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "DELIVERY_TYPE_EN_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "DELIVERY_TYPE_FR_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "DESCRIPTION_TEXT_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "END_DATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "MAX_UNITS_QUANTITY", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "NAME_TEXT_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PLACEMENT_PMT_IND", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PROVISION_METHOD_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PROVISION_METHOD_EN_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "PROVISION_METHOD_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_EN_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_FR_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "REFERENCE_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "REFERENCE_UPPER_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SA_PLACEMENT_INHIBIT_IND", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SERVICE_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SERVICE_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SERVICE_OFFERING_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "SERVICE_UPPER_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SINGLE_MULT_CLIENTS_FLAG", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SPECIAL_COND_TYPE_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SPECIAL_COND_TYPE_EN_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "SPECIAL_COND_TYPE_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "START_DATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "TRAINING_IND", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "UNIT_FREQ_PERIOD_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "UNIT_FREQ_PERIOD_EN_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "UNIT_FREQ_PERIOD_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "UOM_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "UOM_EN_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "UOM_FR_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "Source_Update_Type", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Effective_Start_DT", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Last_Update_DT", "nullable": false, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,
    "USERS": """
    {
        "fields": [
            { "metadata": {}, "name": "USER_KEY", "nullable": false, "type": "integer" },
            { "metadata": {}, "name": "BUSINESS_EMAIL_ADDRESS", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "BUSINESS_PHONE", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "CELL_PHONE", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "CREATION_DATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "END_DATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "FIRST_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "FULL_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "GUID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "GUID_TEXT", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "LAST_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "LOCATION_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "LOCATION_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "ORG_OBJECT_LINK_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "RECORD_STATUS_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_EN_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "ROLE_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SENSITIVITY_IND", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "TITLE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "UPPER_FIRST_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "UPPER_LAST_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "UPPER_ROLE_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "UPPER_USER_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "USER_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "Source_Update_Type", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Effective_Start_DT", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Last_Update_DT", "nullable": false, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,
    "ORG_STRUCTURE_FLAT": """
    {
        "fields": [
            { "metadata": {}, "name": "ORG_UNIT_PARENT_LINK_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "ORGANISATION_UNIT_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PARENT_ORG_UNIT_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "POSITION_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "STRUCTURE_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "TEAM_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "Source_Update_Type", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Effective_Start_DT", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Last_Update_DT", "nullable": false, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,
    "PROVIDER_PARTY": """
    {
        "fields": [
            { "metadata": {}, "name": "PROVIDER_PARTY_KEY", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "CATEGORY_EN_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "CATEGORY_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "DOB_DATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "POSITION_EN_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "POSITION_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "PROVIDER_PARTY_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_EN_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "REFERENCE_NUMBER", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "ROLE_EN_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "ROLE_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Source_Update_Type", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Effective_Start_DT", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Last_Update_DT", "nullable": false, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,
    "LOCATION": """
    {
        "fields": [
            { "metadata": {}, "name": "ARCHIVE_TEAM_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "DESCRIPTION", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "IS_ACTIVE_FLAG", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "LOCATION_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "LOCATION_CODE_NUM", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "LOCATION_CODE_NUM2", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "LOCATION_TYPE_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PROGRAM_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "Source_Update_Type", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Effective_Start_DT", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Last_Update_DT", "nullable": false, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,
    "CODE_TABLE_ITEM": """
    {
        "fields": [
            { "metadata": {}, "name": "TABLE_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "ITEM_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PARENT_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "IS_ENABLED", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "COMMENT_TEXT", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SORT_ORDER", "nullable": true, "type": "integer" },
            { "metadata": {}, "name": "ANNOTATION_EN_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "ANNOTATION_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "EN_DESCRIPTION", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "FR_DESCRIPTION", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Source_Update_Type", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Effective_Start_DT", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Last_Update_DT", "nullable": false, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,
    "POSITION_HOLDER_LINK": """
    {
        "fields": [
            { "metadata": {}, "name": "USER_NAME_KEY", "nullable": true, "type": "integer" },
            { "metadata": {}, "name": "RECORD_STATUS_EN_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_FR_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "Source_Update_Type", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Effective_Start_DT", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Last_Update_DT", "nullable": false, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,
    "CONCERN_ROLE_ADDRESS": """
    {
        "fields": [
            { "metadata": {}, "name": "CONCERN_ROLE_ADDRESS_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "ADDRESS_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "CONCERN_ROLE_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "CONCERN_ROLE_KEY", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "ADDRESS_TYPE_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "ADDRESS_TYPE_EN_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "ADDRESS_TYPE_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "COMMENTS_TEXT", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "STATUS_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "STATUS_EN_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "STATUS_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "START_DATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "END_DATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "Source_Update_Type", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Effective_Start_DT", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Last_Update_DT", "nullable": false, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,
    "ORG_UNIT": """
    {
        "fields": [
            { "metadata": {}, "name": "ORGANISATION_UNIT_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "ORGANISATION_NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "ADDRESS_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "STATUS_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "STATUS_EN_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "STATUS_FR_DESC", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_CODE", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_EN_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RECORD_STATUS_FR_DESC", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "Source_Update_Type", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Effective_Start_DT", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Last_Update_DT", "nullable": false, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,    
    "CONCERNROLE_ATTACHMENT_LINK": """
    {
        "fields": [
            { "metadata": {}, "name": "ATTACHMENT_LINK_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "ATTACHMENT_LINK_CONCERNROLE_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "ATTACHMENT_ID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "STATUS_CODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "DESCRIPTION", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "DATE_RECEIVED", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Source_Update_Type", "nullable": false, "type": "string" },
            { "metadata": {}, "name": "Effective_Start_DT", "nullable": false, "type": "timestamp" },
            { "metadata": {}, "name": "Last_Update_DT", "nullable": false, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,        
}

# COMMAND ----------


# for silver_table, keys in silver_table_wt_keys.items():
for row in table_list:
    silver_table = row['Table_Name'].strip()  
    keys = row['PK_Name'].split(',') if row['PK_Name'] else []  # Get the primary keys from the table definition, if not defined, it will be empty list.
    load_type = row['Table_Type'].lower().replace('type_','') if row['Table_Type'] else '2' # Load Type, if not defined, it will be SCD Type 2

    dataFlowId = f'2000-{silver_table}' # Unique ID for the dataflow -- PK, 2000 is the prefix for Silver Dataflows
    dataFlowGroup = dataflow_group
    sourceFormat = "delta" # Reading from Bronze Layer Delta Table
    sourceDetails = {"database" : f"{source_catalog}.{source_schema}","table": silver_table} # Source Table Details
    readerConfigOptions = None 
    targetFormat = 'delta' # Writing to Silver Layer Delta Table
    targetDetails = {"database": f"{target_catalog}.{target_schema}", "schema":target_schema, "table":silver_table}
    tableProperties = {"delta.enableChangeDataFeed": "true"}
    schema = schema_map.get(silver_table.upper(), None)
    ## Select Expression to be used for eliminating columns, change column names, add new columns, change data types etc. here we are also generating a surrogate key using MD5 hash of all columns except the ones mentioned in the except clause and converting it to decimal(32,0)
    selectExp = None # ["MD5(CONCAT_WS('', id, client, status)) AS surrogate_key", "* EXCEPT (_rescued_data,processing_time)","current_timestamp() as processing_time"] #(REQUIRED)
    whereClause = None
    partitionColumns = None #Example: #['customer_id','operation_date'] Databricks Recommends to use Liquid Clustering instead of Partitioning
    liquidClusteringColumns = None #Example: #['customer_id','operation_date'] Databricks Highly Recommends using Liquid Clustering Doc: https://docs.databricks.com/aws/en/delta/clustering#choose-clustering-keys
    
    if load_type in ['1','2']:
        cdcApplyChanges = json.dumps({
            "keys": keys, 
            "scd_type": f"{load_type}", 
            "sequence_by": ["Effective_Start_DT"]
        })
    else:
        cdcApplyChanges = None
    
    if load_type in ['mv']:
        # Read source query file
        with open(f"./silver_queries/query_{silver_table.lower()}.sql", 'r') as file:
            materiazedView = file.read().replace('LIVE.',f'{source_catalog}.{source_schema}.')
    else:
        materiazedView = None
        
    dataQualityExpectations = None
    quarantineTargetDetails = None
    quarantineTableProperties = None
    createDate = datetime.datetime.now()
    updateDate = datetime.datetime.now()
    createdBy = spark.range(1).select(current_user()).head()[0]
    updatedBy = spark.range(1).select(current_user()).head()[0]
    SILVER_MD_TABLE = BRONZE_MD_TABLE = f"{meta_catalog}.{meta_schema}.silver_dataflowspec_table" # type: ignore

    populate_silver(SILVER_MD_TABLE,dataFlowId, dataFlowGroup, sourceFormat, sourceDetails, readerConfigOptions, targetFormat, targetDetails, tableProperties, schema, selectExp,whereClause,partitionColumns,liquidClusteringColumns, cdcApplyChanges, materiazedView, dataQualityExpectations,createDate, createdBy,updateDate, updatedBy,spark)


