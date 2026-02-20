# Databricks notebook source
# MAGIC %md
# MAGIC # DLT pipeline to read GG parquet files and load to bronze tables (Type-2)
# MAGIC
# MAGIC This Delta Live Tables (DLT) definition is executed using a pipeline defined in resources/idh_dab.pipeline.yml.

# COMMAND ----------


from dlt_helpers.populate_md import populate_bronze
import datetime
from pyspark.sql.functions import current_user
import json
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DateType
)


# COMMAND ----------

dbutils.widgets.text('Source_System',defaultValue='idh_cpin')

dbutils.widgets.text('env',defaultValue='')

dbutils.widgets.text('subscription',defaultValue='')

dbutils.widgets.text('tenant',defaultValue='')

dbutils.widgets.text('resource_group',defaultValue='')

dbutils.widgets.text('queue_name',defaultValue='')

dbutils.widgets.text('Target_Catalog',defaultValue='')

dbutils.widgets.text('Source_Schema',defaultValue='raw_cpin')

dbutils.widgets.text('Target_Schema',defaultValue='bronze_cpin')

dbutils.widgets.text('Metadata_Catalog',defaultValue='')

dbutils.widgets.text('Metadata_Schema',defaultValue='audit_idh')

dbutils.widgets.text('Dataflow_Group',defaultValue='IDH')

dbutils.widgets.text('Raw_Path',defaultValue='/raw_cpin/cpin-raw-vol/')

dbutils.widgets.text('Schema_Path',defaultValue='/bronze_cpin/Schema/')

dbutils.widgets.text('Raw_Checkpoint_Path',defaultValue='/bronze_cpin/Checkpoint/')



# COMMAND ----------

source_system = dbutils.widgets.get("Source_System")

target_catalog =dbutils.widgets.get("Target_Catalog")

source_schema = dbutils.widgets.get("Source_Schema")

target_schema = dbutils.widgets.get("Target_Schema")

meta_catalog =dbutils.widgets.get("Metadata_Catalog")

meta_schema = dbutils.widgets.get("Metadata_Schema")

env = dbutils.widgets.get("env")

subscription = dbutils.widgets.get("subscription")

tenant = dbutils.widgets.get("tenant")

resource_group = dbutils.widgets.get("resource_group")

queue_name = dbutils.widgets.get("queue_name")

dataflow_group = dbutils.widgets.get("Dataflow_Group")

raw_path = dbutils.widgets.get("Raw_Path")

schema_path = dbutils.widgets.get("Schema_Path")

raw_checkpoint_path = dbutils.widgets.get("Raw_Checkpoint_Path")

# COMMAND ----------

source_volume_catalog = target_catalog
raw_volume_path = raw_path

# COMMAND ----------

source_schema_map = {
    "CPIN_CURAM.PROVIDER": """
    {
        "fields": [
            { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PROVIDERCONCERNROLEID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "PROVIDERENQUIRYID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "PHYSICALCAPACITY", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "PAYMENTFREQUENCY", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "METHODOFPAYMENT", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "CURRENCYTYPE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RECORDSTATUS", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RESERVATIONGRACEPERIOD", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "OVERRIDEMDRIND", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PREFERREDSEMETHOD", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "AREASSVDINFOTXTID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "CLIENTINFOTEXTID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "NAMEUPPER", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "ENROLLMENTDATETIME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "ENDDATETIME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "ACCEPTCWREFERRAL", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "VERSIONNO", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "LASTWRITTEN", "nullable": true, "type": "string" }
        ],
        "type": "struct"
    }
    """,
    "CPIN_CURAM.CONCERNROLE": """
    {
        "fields": [
            { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "CONCERNID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "CONCERNROLEID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "CONCERNROLETYPE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "CREATIONDATE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "REGISTRATIONDATE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "STARTDATE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "ENDDATE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "STATUSCODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "CONCERNROLENAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PRIMARYADDRESSID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "PRIMARYALTERNATEID", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "COMMENTS", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PRIMARYPHONENUMBERID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "PRIMARYEMAILADDRESSID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "PRIMARYBANKACCOUNTID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "REGUSERNAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PREFPUBLICOFFICEID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "PREFERREDLANGUAGE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SENSITIVITY", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PREFCOMMMETHOD", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PREFCOMMFROMDATE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PREFCOMMTODATE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PRIMARYWEBADDRESSID", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "VERSIONNO", "nullable": true, "type": "decimal(19,0)" },
            { "metadata": {}, "name": "LASTWRITTEN", "nullable": true, "type": "string" }
        ],
        "type": "struct"
    }
    """,
    "CPIN_CURAM.MCYSPROVIDEREXT": """
    {
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "PROVIDERCONCERNROLEID", "nullable": true, "type": "decimal(19,0)" },
        { "metadata": {}, "name": "PROVIDERCONSENTFORCAPTURE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "PROVIDERNATIVESTATUS", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "INDIGENOUSPERSONIND", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "INDIGENOUSGROUPCODE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "EMAILADDRESSID", "nullable": true, "type": "decimal(19,0)" },
        { "metadata": {}, "name": "PROVIDERCONSENTFORSEARCH", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "PRIMARYPROVIDERTYPEID", "nullable": true, "type": "decimal(19,0)" },
        { "metadata": {}, "name": "VERSIONNO", "nullable": true, "type": "decimal(19,0)" },
        { "metadata": {}, "name": "LASTWRITTEN", "nullable": true, "type": "string" }
    ],
    "type": "struct"
    }
    """,
"CPIN_CURAM.PROVIDERENQUIRY": """
    {
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
        {"metadata": {}, "name": "PROVIDERENQUIRYID", "type":	"decimal(19,0)", "nullable": true},
        {"metadata": {}, "name": "REFERENCENUMBER", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "NAME", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "HOMEADDRESSID", "type":	"decimal(19,0)", "nullable": true},
        {"metadata": {}, "name": "WORKADDRESSID", "type":	"decimal(19,0)", "nullable": true},
        {"metadata": {}, "name": "HOMEPHONENUMBERID", "type":	"decimal(19,0)", "nullable": true},
        {"metadata": {}, "name": "WORKPHONENUMBERID", "type":	"decimal(19,0)", "nullable": true},
        {"metadata": {}, "name": "MOBILEPHONENUMBERID", "type":	"decimal(19,0)", "nullable": true},
        {"metadata": {}, "name": "PROVIDERCATEGORYPERIODID", "type":	"decimal(19,0)", "nullable": true},
        {"metadata": {}, "name": "ADDITIONALNAME", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "PREFERREDSESSION", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "REASONFORENQUIRY", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "CONFIRMEDMEETINGDETAILS", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "ATTENDEDMEETING", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "OBTAINEDAPPLICATIONFORM", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "SCHEDULEDMEETING", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "NOOFCHILDREN", "type":	"decimal(19,0)", "nullable": true},
        {"metadata": {}, "name": "AVAILABILITYFORCONTACT", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "ENQUIRYDATE", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "ENDDATE", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "OWNERNAME", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "RECORDSTATUS", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "PREFERREDLANGUAGE", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "PREFERREDCOMMUNICATION", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "NAMEUPPER", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "ORGOBJECTLINKID", "type":	"decimal(19,0)", "nullable": true},
        {"metadata": {}, "name": "VERSIONNO", "type":	"decimal(19,0)", "nullable": true},
        {"metadata": {}, "name": "LASTWRITTEN", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "ADDITIONALINFORMATION", "type":	"string", "nullable": true}
],
"type": "struct"
}
""",
"CPIN_CURAM.CONCERNROLEALTERNATEID": """
    {
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "CONCERNROLEID", "nullable": true, "type": "decimal(19,0)" },
        { "metadata": {}, "name": "ALTERNATEID", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "TYPECODE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "STARTDATE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "ENDDATE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "STATUSCODE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "CONCERNROLEALTERNATEID", "nullable": true, "type": "decimal(19,0)" },
        { "metadata": {}, "name": "COMMENTS", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "VERSIONNO", "nullable": true, "type": "decimal(19,0)" },
        { "metadata": {}, "name": "LASTWRITTEN", "nullable": true, "type": "string" }
    ],
    "type": "struct"
    }
    """,
    "CPIN_CURAM.CODETABLEITEM": """
    {
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "TABLENAME", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "CODE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "DESCRIPTION", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "ANNOTATION", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "ISENABLED", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "SORTORDER", "nullable": true, "type": "decimal(19,0)" },
        { "metadata": {}, "name": "LOCALEIDENTIFIER", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "PARENTCODE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "COMMENTS", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "VERSIONNO", "nullable": true, "type": "decimal(19,0)" },
        { "metadata": {}, "name": "LASTWRITTEN", "nullable": true, "type": "string" }
    ],
    "type": "struct"
    }
    """,
    "CPIN_CURAM.ADDRESS": """{
        "type": "struct",
        "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
        {"name": "ADDRESSID", "type": "decimal(19,0)", "nullable": false},
        {"name": "COUNTRYCODE", "type": "string", "nullable": true},
        {"name": "MODIFIABLEIND", "type": "string", "nullable": false},
        {"name": "ADDRESSDATA", "type": "string", "nullable": true},
        {"name": "ADDRESSLAYOUTTYPE", "type": "string", "nullable": true},
        {"name": "GEOCODE", "type": "string", "nullable": true},
        {"name": "LATITUDE", "type": "decimal(32,8)", "nullable": true},
        {"name": "LONGITUDE", "type": "decimal(32,8)", "nullable": true},
        {"name": "VERSIONNO", "type": "decimal(19,0)", "nullable": false},
        {"name": "LASTWRITTEN", "type": "string", "nullable": true}
        ]
    }""",
    "CPIN_CURAM.ADDRESSELEMENT": """{
        "type": "struct",
        "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
        {"name": "ADDRESSELEMENTID", "type": "decimal(19,0)", "nullable": false},
        {"name": "ADDRESSID", "type": "decimal(19,0)", "nullable": false},
        {"name": "ELEMENTTYPE", "type": "string", "nullable": true},
        {"name": "ELEMENTVALUE", "type": "string", "nullable": true},
        {"name": "UPPERELEMENTVALUE", "type": "string", "nullable": true},
        {"name": "LASTWRITTEN", "type": "string", "nullable": true}
        ]
    }""",
    "CPIN_CURAM.ADMINISTRATIONCONCERNROLE": """{
        "type": "struct",
        "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
        {"name": "ADMINISTRATIONCONCERNROLEID", "type": "decimal(19,0)", "nullable": false},
        {"name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": false},
        {"name": "ADMINISTRATIONROLEID", "type": "decimal(19,0)", "nullable": false},
        {"name": "STARTDATE", "type": "string", "nullable": true},
        {"name": "ENDDATE", "type": "string", "nullable": true},
        {"name": "TYPECODE", "type": "string", "nullable": true},
        {"name": "VERSIONNO", "type": "decimal(19,0)", "nullable": false},
        {"name": "LASTWRITTEN", "type": "string", "nullable": true}
        ]
    }""",
    "CPIN_CURAM.ADMINISTRATIONROLE": """{
        "type": "struct",
        "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
        {"name": "ADMINISTRATIONROLEID", "type": "decimal(19,0)", "nullable": false},
        {"name": "USERNAME", "type": "string", "nullable": true},
        {"name": "STATUSCODE", "type": "string", "nullable": true},
        {"name": "ORGOBJECTLINKID", "type": "decimal(19,0)", "nullable": true},
        {"name": "VERSIONNO", "type": "decimal(19,0)", "nullable": false},
        {"name": "LASTWRITTEN", "type": "string", "nullable": true}
        ]
    }""",
    "CPIN_CURAM.ALLEGATIONROLE": """{
        "type": "struct",
        "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
        {"name": "ALLEGATIONROLEID", "type": "decimal(19,0)", "nullable": false},
        {"name": "ALLEGATIONID", "type": "decimal(19,0)", "nullable": false},
        {"name": "CASEPARTICIPANTROLEID", "type": "decimal(19,0)", "nullable": true},
        {"name": "ROLETYPE", "type": "string", "nullable": true},
        {"name": "VERSIONNO", "type": "decimal(19,0)", "nullable": false},
        {"name": "LASTWRITTEN", "type": "string", "nullable": true}
        ]
    }""",
    "CPIN_CURAM.ALTERNATENAME": """{
    "type": "struct",
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
      {"name": "ALTERNATENAMEID", "type": "decimal(19,0)", "nullable": false},
      {"name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": false},
      {"name": "TITLE", "type": "string", "nullable": true},
      {"name": "FIRSTFORENAME", "type": "string", "nullable": true},
      {"name": "OTHERFORENAME", "type": "string", "nullable": true},
      {"name": "SURNAME", "type": "string", "nullable": true},
      {"name": "NAMESUFFIX", "type": "string", "nullable": true},
      {"name": "NAMETYPE", "type": "string", "nullable": true},
      {"name": "NAMESTATUS", "type": "string", "nullable": true},
      {"name": "FULLNAME", "type": "string", "nullable": true},
      {"name": "COMMENTS", "type": "string", "nullable": true},
      {"name": "INITIALS", "type": "string", "nullable": true},
      {"name": "UPPERFIRSTFORENAME", "type": "string", "nullable": true},
      {"name": "UPPERSURNAME", "type": "string", "nullable": true},
      {"name": "PHONETICENCODING", "type": "string", "nullable": true},
      {"name": "VERSIONNO", "type": "decimal(19,0)", "nullable": false},
      {"name": "LASTWRITTEN", "type": "string", "nullable": true}
    ]
  }""",
    "CPIN_CAMS.APPLICATION": """{
    "type": "struct",
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
      {"name": "ID", "type": "decimal(19,0)", "nullable": true},
      {"name": "APP_CODE", "type": "string", "nullable": true},
      {"name": "APP_TYPE_CODE", "type": "string", "nullable": true},
      {"name": "PROGRAM_ID", "type": "decimal(19,0)", "nullable": true},
      {"name": "DESCRIPTION", "type": "string", "nullable": true},
      {"name": "LOGIN_URL_KEY_TEXT", "type": "string", "nullable": true},
      {"name": "IS_ACTIVE_FLAG", "type": "string", "nullable": true}
    ]
  }""",
    "CPIN_CURAM.ATTACHMENT": """{
    "type": "struct",
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
      {"name": "ATTACHMENTID", "type": "decimal(19,0)", "nullable": false},
      {"name": "ATTACHMENTCONTENTS", "type": "string", "nullable": true},
      {"name": "ATTACHMENTNAME", "type": "string", "nullable": true},
      {"name": "ATTACHMENTSTATUS", "type": "string", "nullable": true},
      {"name": "FILELOCATION", "type": "string", "nullable": true},
      {"name": "FILEREFERENCE", "type": "string", "nullable": true},
      {"name": "DOCUMENTTYPE", "type": "string", "nullable": true},
      {"name": "RECEIPTDATE", "type": "string", "nullable": true},
      {"name": "STATUSCODE", "type": "string", "nullable": false},
      {"name": "ATTACHEDFILEIND", "type": "string", "nullable": false},
      {"name": "VERSIONNO", "type": "decimal(19,0)", "nullable": false},
      {"name": "LASTWRITTEN", "type": "string", "nullable": true}
    ]
  }""",
    "CPIN_CURAM.CASEPARTICIPANTROLE": """{
    "type": "struct",
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
      {"name": "CASEPARTICIPANTROLEID", "type": "decimal(19,0)", "nullable": false},
      {"name": "PARTICIPANTROLEID", "type": "decimal(19,0)", "nullable": false},
      {"name": "CASEID", "type": "decimal(19,0)", "nullable": false},
      {"name": "FROMDATE", "type": "string", "nullable": true},
      {"name": "TODATE", "type": "string", "nullable": true},
      {"name": "TYPECODE", "type": "string", "nullable": true},
      {"name": "RECORDSTATUS", "type": "string", "nullable": true},
      {"name": "ENDREASON", "type": "string", "nullable": true},
      {"name": "COMMENTS", "type": "string", "nullable": true},
      {"name": "TRANSLATIONREQUIREDIND", "type": "string", "nullable": true},
      {"name": "VERSIONNO", "type": "decimal(19,0)", "nullable": false},
      {"name": "LASTWRITTEN", "type": "string", "nullable": true}
    ]
  }""",
"CPIN_CURAM.CASEUSERROLE": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CASEUSERROLEID", "type": "decimal(19,0)", "nullable": false },
    { "name": "CASEID", "type": "decimal(19,0)", "nullable": false },
    { "name": "FROMDATE", "type": "string", "nullable": true },
    { "name": "TODATE", "type": "string", "nullable": true },
    { "name": "TYPECODE", "type": "string", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "REASONCODE", "type": "string", "nullable": true },
    { "name": "ORGOBJECTLINKID", "type": "decimal(19,0)", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": false },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONCERNROLEALTERNATEID": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ALTERNATEID", "type": "string", "nullable": true },
    { "name": "TYPECODE", "type": "string", "nullable": true },
    { "name": "STARTDATE", "type": "string", "nullable": true },
    { "name": "ENDDATE", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "CONCERNROLEALTERNATEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONCERNROLEATTACHMENTLINK": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ATTACHMENTLINKID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ATTACHMENTID", "type": "decimal(19,0)", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "DESCRIPTION", "type": "string", "nullable": true },
    { "name": "DATERECEIVED", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONCERNROLEEMAILADDRESS": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CONCERNROLEEMAILADDRESSID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "EMAILADDRESSID", "type": "decimal(19,0)", "nullable": true },
    { "name": "TYPECODE", "type": "string", "nullable": true },
    { "name": "STARTDATE", "type": "string", "nullable": true },
    { "name": "ENDDATE", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONCERNROLEPHONENUMBER": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CONCERNROLEPHONENUMBERID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PHONENUMBERID", "type": "decimal(19,0)", "nullable": true },
    { "name": "TYPECODE", "type": "string", "nullable": true },
    { "name": "STARTDATE", "type": "string", "nullable": true },
    { "name": "ENDDATE", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONTACTLOG": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CONTACTLOGID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CONTACTLOGTYPE", "type": "string", "nullable": true },
    { "name": "AUTHOR", "type": "string", "nullable": true },
    { "name": "CREATEDDATETIME", "type": "string", "nullable": true },
    { "name": "CREATEDBY", "type": "string", "nullable": true },
    { "name": "PURPOSE", "type": "string", "nullable": true },
    { "name": "STARTDATETIME", "type": "string", "nullable": true },
    { "name": "ENDDATETIME", "type": "string", "nullable": true },
    { "name": "METHOD", "type": "string", "nullable": true },
    { "name": "LOCATION", "type": "string", "nullable": true },
    { "name": "LOCATIONDESCRIPTION", "type": "string", "nullable": true },
    { "name": "ADDENDUMIND", "type": "string", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "NOTEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONTACTLOGCONCERN": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CONTACTLOGCONCERNID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CONTACTLOGID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.EMAILADDRESS": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "EMAILADDRESSID", "type": "decimal(19,0)", "nullable": true },
    { "name": "EMAILADDRESS", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.LOCATION": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "LOCATIONID", "type": "decimal(19,0)", "nullable": true },
    { "name": "NAME", "type": "string", "nullable": true },
    { "name": "ADDRESSID", "type": "decimal(19,0)", "nullable": true },
    { "name": "DESCRIPTION", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "LOCATIONSTATUS", "type": "string", "nullable": true },
    { "name": "CREATIONDATE", "type": "string", "nullable": true },
    { "name": "LOCATIONTYPE", "type": "string", "nullable": true },
    { "name": "PARENTLOCATIONID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ORGANISATIONID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ADDRESSCOMMENTS", "type": "string", "nullable": true },
    { "name": "CREATELOCATIONSID", "type": "string", "nullable": true },
    { "name": "MAINTAINSID", "type": "string", "nullable": true },
    { "name": "READSID", "type": "string", "nullable": true },
    { "name": "LOCATIONSTRUCTUREID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PUBLICOFFICEINDICATOR", "type": "string", "nullable": true },
    { "name": "PHONENUMBERID", "type": "decimal(19,0)", "nullable": true },
    { "name": "FAXNUMBERID", "type": "decimal(19,0)", "nullable": true },
    { "name": "EMAILADDRESSID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ROOTLOCATIONINDICATOR", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true },
    { "name": "UPPERNAME", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSALTERNATENAMEEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ALTERNATENAMEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "FORENAMEPHONETICENC", "type": "string", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true },
    { "name": "UPPERFULLNAME", "type": "string", "nullable": true },
    { "name": "SINGLENAMEIND", "type": "string", "nullable": true },
    { "name": "UPPERFULLNAMESEARCH", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSATTACHMENTLINKEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ATTACHMENTLINKID", "type": "decimal(19,0)", "nullable": true },
    { "name": "RELATEDTYPE", "type": "string", "nullable": true },
    { "name": "RELATEDID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ATTACHMENTTYPECODE", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSIDBD": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CITIZENSHIPSTATUSTABLIST", "type": "string", "nullable": true },
    { "name": "CITIZENSHIPOTHERCNTRYTABLIST", "type": "string", "nullable": true },
    { "name": "CITIZENSHIPOTHERCOUNTRY", "type": "string", "nullable": true },
    { "name": "ABORIGINALIND", "type": "string", "nullable": true },
    { "name": "ABORIGINALIDENTITY", "type": "string", "nullable": true },
    { "name": "ABORIGINALIDENTITYOTHER", "type": "string", "nullable": true },
    { "name": "BORNINCANADAIND", "type": "string", "nullable": true },
    { "name": "BORNCANADAPROVINCE", "type": "string", "nullable": true },
    { "name": "BORNCOUNTRY", "type": "string", "nullable": true },
    { "name": "BORNCOUNTRYOTHER", "type": "string", "nullable": true },
    { "name": "RACIALGROUPTABLIST", "type": "string", "nullable": true },
    { "name": "RACIALGROUPOTHER", "type": "string", "nullable": true },
    { "name": "ETHNICORIGINTABLIST", "type": "string", "nullable": true },
    { "name": "ETHNICORIGINOTHER", "type": "string", "nullable": true },
    { "name": "RELIGIONTABLIST", "type": "string", "nullable": true },
    { "name": "RELIGIONOTHER", "type": "string", "nullable": true },
    { "name": "HOMELANGTABLIST", "type": "string", "nullable": true },
    { "name": "HOMEINDIGLANGTABLIST", "type": "string", "nullable": true },
    { "name": "HOMEINDIGLANGOTHER", "type": "string", "nullable": true },
    { "name": "HOMEADDITIONALLANGTABLIST", "type": "string", "nullable": true },
    { "name": "HOMEADDITIONALLANGOTHER", "type": "string", "nullable": true },
    { "name": "BIRTHYEARIND", "type": "string", "nullable": true },
    { "name": "BIRTHYEAR", "type": "string", "nullable": true },
    { "name": "BIRTHSEX", "type": "string", "nullable": true },
    { "name": "LIVEDGENDERIDENTITYTABLIST", "type": "string", "nullable": true },
    { "name": "LIVEGENDERIDENTITYOTHER", "type": "string", "nullable": true },
    { "name": "SEXUALORIENTATIONTABLIST", "type": "string", "nullable": true },
    { "name": "SEXUALORIENTATIONOTHER", "type": "string", "nullable": true },
    { "name": "MARITALSTATUS", "type": "string", "nullable": true },
    { "name": "MARITALSTATUSOTHER", "type": "string", "nullable": true },
    { "name": "FAMILYSTATUS", "type": "string", "nullable": true },
    { "name": "FAMILYSTATUSOTHER", "type": "string", "nullable": true },
    { "name": "DISABILITYIND", "type": "string", "nullable": true },
    { "name": "POSTALCODEIND", "type": "string", "nullable": true },
    { "name": "POSTALCODE", "type": "string", "nullable": true },
    { "name": "POSTALCODE2", "type": "string", "nullable": true },
    { "name": "CHANGEREASON", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true },
    { "name": "CONSENTIND", "type": "string", "nullable": true },
    { "name": "IDBDVERSION", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSORGANISATIONUNITEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ORGANISATIONUNITID", "type": "decimal(19,0)", "nullable": true },
    { "name": "AGENCYNUMBER", "type": "string", "nullable": true },
    { "name": "AGENCYCODE", "type": "string", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true },
    { "name": "PAYMENTEFFECTIVEDATE", "type": "string", "nullable": true },
    { "name": "NAMETEXTID", "type": "decimal(19,0)", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSPERSONEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ETHINICORIGINTABCODES", "type": "string", "nullable": true },
    { "name": "RELIGION", "type": "string", "nullable": true },
    { "name": "ELIGFORFRENCHSERVICEIND", "type": "string", "nullable": true },
    { "name": "LIFEBOOKAVAILABLEIND", "type": "string", "nullable": true },
    { "name": "INTERPRETERREQIND", "type": "string", "nullable": true },
    { "name": "ABORIGINALANCESTRYCODE", "type": "string", "nullable": true },
    { "name": "NATIVESTATUSCODE", "type": "string", "nullable": true },
    { "name": "BANDNUMBER", "type": "string", "nullable": true },
    { "name": "LIVINGOFFRESERVECODE", "type": "string", "nullable": true },
    { "name": "PROVIDERMEMBERROLE", "type": "string", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true },
    { "name": "FIRSTLANGUAGE", "type": "string", "nullable": true },
    { "name": "OTHERRACEDESCRIPTION", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPCODETWO", "type": "string", "nullable": true },
    { "name": "OTHERRELIGIONDESCRIPTION", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPDESCRIPTION", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSPROSPECTPERSONEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "RELIGION", "type": "string", "nullable": true },
    { "name": "ELIGFORFRENCHSERVICEIND", "type": "string", "nullable": true },
    { "name": "INTERPRETERREQIND", "type": "string", "nullable": true },
    { "name": "ABORIGINALANCESTRYCODE", "type": "string", "nullable": true },
    { "name": "NATIVESTATUSCODE", "type": "string", "nullable": true },
    { "name": "BANDNUMBER", "type": "string", "nullable": true },
    { "name": "LIVINGOFFRESERVECODE", "type": "string", "nullable": true },
    { "name": "ETHNICORIGINTABCODES", "type": "string", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true },
    { "name": "FIRSTLANGUAGE", "type": "string", "nullable": true },
    { "name": "OTHERRACEDESCRIPTION", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPCODETWO", "type": "string", "nullable": true },
    { "name": "OTHERRELIGIONDESCRIPTION", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPDESCRIPTION", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSPROVIDERENQUIRYEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "PROVIDERENQUIRYID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PROVIDERCONSENTFORCAPTURE", "type": "string", "nullable": true },
    { "name": "ABORIGINALANCESTRYCODE", "type": "string", "nullable": true },
    { "name": "INDIGENOUSPERSONIND", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPCODE", "type": "string", "nullable": true },
    { "name": "CLOSUREREASON", "type": "string", "nullable": true },
    { "name": "EMAILADDRESSID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PROVIDERCONSENTFORSEARCH", "type": "string", "nullable": true },
    { "name": "PRIMARYPROVIDERTYPEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true },
    { "name": "CLOSURECOMMENTS", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSPROVIDERPARTYEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "PROVIDERPARTYID", "type": "decimal(19,0)", "nullable": true },
    { "name": "DATEOFBIRTH", "type": "string", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.ORGANISATIONSTRUCTURE": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ORGANISATIONSTRUCTUREID", "type": "decimal(19,0)", "nullable": true },
    { "name": "NAME", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "ACTIVATIONDATE", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "ORGANISATIONID", "type": "decimal(19,0)", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.ORGANISATIONUNIT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "BUSINESSTYPECODE", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "CREATIONDATE", "type": "string", "nullable": true },
    { "name": "DEFAULTPRINTERID", "type": "decimal(19,0)", "nullable": true },
    { "name": "NAME", "type": "string", "nullable": true },
    { "name": "ORGANISATIONUNITID", "type": "decimal(19,0)", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "WEBADDRESS", "type": "string", "nullable": true },
    { "name": "READSID", "type": "string", "nullable": true },
    { "name": "MAINTAINSID", "type": "string", "nullable": true },
    { "name": "CREATEUNITSID", "type": "string", "nullable": true },
    { "name": "LOCATIONID", "type": "decimal(19,0)", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true },
    { "name": "UPPERNAME", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.ORGOBJECTLINK": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ORGOBJECTLINKID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ORGOBJECTREFERENCE", "type": "decimal(19,0)", "nullable": true },
    { "name": "USERNAME", "type": "string", "nullable": true },
    { "name": "ORGOBJECTTYPE", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.ORGUNITPARENTLINK": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ORGUNITPARENTLINKID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ORGANISATIONSTRUCTUREID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ORGANISATIONUNITID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PARENTORGANISATIONUNITID", "type": "decimal(19,0)", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.ORGUNITPOSITIONLINK": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ORGUNITPOSITIONLINKID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ORGANISATIONSTRUCTUREID", "type": "decimal(19,0)", "nullable": true },
    { "name": "POSITIONID", "type": "decimal(19,0)", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "ORGANISATIONUNITID", "type": "decimal(19,0)", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.PERSON": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PERSONBIRTHNAME", "type": "string", "nullable": true },
    { "name": "DATEOFBIRTH", "type": "string", "nullable": true },
    { "name": "DATEOFDEATH", "type": "string", "nullable": true },
    { "name": "GENDER", "type": "string", "nullable": true },
    { "name": "SPECIALINTERESTCODE", "type": "string", "nullable": true },
    { "name": "MARITALSTATUSCODE", "type": "string", "nullable": true },
    { "name": "NATIONALITYCODE", "type": "string", "nullable": true },
    { "name": "RESIDENCYABROADIND", "type": "string", "nullable": true },
    { "name": "MOTHERBIRTHSURNAME", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "TYPE", "type": "string", "nullable": true },
    { "name": "DATEOFBIRTHVERIND", "type": "string", "nullable": true },
    { "name": "DATEOFDEATHVERIND", "type": "string", "nullable": true },
    { "name": "PRIMARYALTERNATEID", "type": "string", "nullable": true },
    { "name": "COUNTRYOFBIRTH", "type": "string", "nullable": true },
    { "name": "PLACEOFBIRTH", "type": "string", "nullable": true },
    { "name": "PINNUMBER", "type": "string", "nullable": true },
    { "name": "PRIMARYALTERNATENAMEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ETHNICORIGINCODE", "type": "string", "nullable": true },
    { "name": "PAYMENTFREQUENCY", "type": "string", "nullable": true },
    { "name": "NEXTPAYMENTDATE", "type": "string", "nullable": true },
    { "name": "CURRENCYTYPE", "type": "string", "nullable": true },
    { "name": "METHODOFPMTCODE", "type": "string", "nullable": true },
    { "name": "UPPERPERSONBIRTHNAME", "type": "string", "nullable": true },
    { "name": "UPPERMOTHERBIRTHSURNAME", "type": "string", "nullable": true },
    { "name": "RACE", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPCODE", "type": "string", "nullable": true },
    { "name": "INDIGENOUSPERSONIND", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.PHONENUMBER": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "PHONENUMBERID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PHONECOUNTRYCODE", "type": "string", "nullable": true },
    { "name": "PHONEAREACODE", "type": "string", "nullable": true },
    { "name": "PHONENUMBER", "type": "string", "nullable": true },
    { "name": "PHONEEXTENSION", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.PROSPECTPERSON": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PERSONBIRTHNAME", "type": "string", "nullable": true },
    { "name": "DATEOFBIRTH", "type": "string", "nullable": true },
    { "name": "DATEOFDEATH", "type": "string", "nullable": true },
    { "name": "GENDER", "type": "string", "nullable": true },
    { "name": "SPECIALINTERESTCODE", "type": "string", "nullable": true },
    { "name": "MARITALSTATUSCODE", "type": "string", "nullable": true },
    { "name": "NATIONALITYCODE", "type": "string", "nullable": true },
    { "name": "RESIDENCYABROADIND", "type": "string", "nullable": true },
    { "name": "MOTHERBIRTHSURNAME", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "TYPE", "type": "string", "nullable": true },
    { "name": "DATEOFBIRTHVERIND", "type": "string", "nullable": true },
    { "name": "DATEOFDEATHVERIND", "type": "string", "nullable": true },
    { "name": "PRIMARYALTERNATEID", "type": "string", "nullable": true },
    { "name": "COUNTRYOFBIRTH", "type": "string", "nullable": true },
    { "name": "PLACEOFBIRTH", "type": "string", "nullable": true },
    { "name": "PINNUMBER", "type": "string", "nullable": true },
    { "name": "PRIMARYALTERNATENAMEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ETHNICORIGINCODE", "type": "string", "nullable": true },
    { "name": "PERSONCONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "FROMAGE", "type": "decimal(19,0)", "nullable": true },
    { "name": "TOAGE", "type": "decimal(19,0)", "nullable": true },
    { "name": "UPPERPERSONBIRTHNAME", "type": "string", "nullable": true },
    { "name": "UPPERMOTHERBIRTHSURNAME", "type": "string", "nullable": true },
    { "name": "RACE", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPCODE", "type": "string", "nullable": true },
    { "name": "INDIGENOUSPERSONIND", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.PROVIDEROFFERING":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "PROVIDEROFFERINGID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PROVIDERCONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "SERVICEOFFERINGID", "type": "decimal(19,0)", "nullable": true },
    { "name": "STARTDATE", "type": "string", "nullable": true },
    { "name": "ENDDATE", "type": "string", "nullable": true },
    { "name": "ENDREASON", "type": "string", "nullable": true },
    { "name": "DENIALREASON", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "INTKPROCINFOTEXTID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CLIENTFEEINFTEXTID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ELIGIBILITYINFOTEXTID", "type": "decimal(19,0)", "nullable": true },
    { "name": "DOCSREQDINFTEXTID", "type": "decimal(19,0)", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CURAM.PROVIDERPARTY":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "PROVIDERPARTYID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PROVIDERCONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PARTYCONCERNROLEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "STARTDATE", "type": "string", "nullable": true },
    { "name": "ENDDATE", "type": "string", "nullable": true },
    { "name": "CATEGORY", "type": "string", "nullable": true },
    { "name": "TYPE", "type": "string", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "POSITION", "type": "string", "nullable": true },
    { "name": "STARTDATETIME", "type": "string", "nullable": true },
    { "name": "ENDDATETIME", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CAMS.ROLE":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ROLE_CODE", "type": "string", "nullable": true },
    { "name": "APP_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "LOCATION_TYPE_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "DESCRIPTION", "type": "string", "nullable": true },
    { "name": "IS_ACTIVE_FLAG", "type": "string", "nullable": true },
    { "name": "SHORT_ROLE_CODE", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CURAM.SERVICEOFFERING":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "SERVICEOFFERINGID", "type": "decimal(19,0)", "nullable": true },
    { "name": "NAME", "type": "string", "nullable": true },
    { "name": "DESCRIPTION", "type": "string", "nullable": true },
    { "name": "STARTDATE", "type": "string", "nullable": true },
    { "name": "ENDDATE", "type": "string", "nullable": true },
    { "name": "UNITOFMEASURE", "type": "string", "nullable": true },
    { "name": "MAXIMUMUNITS", "type": "decimal(19,0)", "nullable": true },
    { "name": "UNITFREQUENCY", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "NAMEUPPER", "type": "string", "nullable": true },
    { "name": "PLACEMENTPMTIND", "type": "string", "nullable": true },
    { "name": "SAPLMTINHIBITIND", "type": "string", "nullable": true },
    { "name": "PAYBASEDONATTENDANCEIND", "type": "string", "nullable": true },
    { "name": "TRAININGIND", "type": "string", "nullable": true },
    { "name": "SPECIALCONDITIONTYPE", "type": "string", "nullable": true },
    { "name": "PROVISIONMETHOD", "type": "string", "nullable": true },
    { "name": "DELIVERYFREQUENCY", "type": "string", "nullable": true },
    { "name": "SINGLEORMULTIPLECLIENTS", "type": "string", "nullable": true },
    { "name": "REFERENCE", "type": "string", "nullable": true },
    { "name": "NAMETEXTID", "type": "decimal(19,0)", "nullable": true },
    { "name": "DESCRIPTIONTEXTID", "type": "decimal(19,0)", "nullable": true },
    { "name": "REFERENCEUPPER", "type": "string", "nullable": true },
    { "name": "DELIVERYTYPE", "type": "string", "nullable": true },
    { "name": "AVAILABILITYCHECKEXCLUDEIND", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true },
    { "name": "REFERREDBY", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CAMS.USER_ACCESS": """
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "USER_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CUID_TEXT", "type": "string", "nullable": true },
    { "name": "LOCATION_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "ROLE_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "USER_POSITION_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "REPORT_TO_POSITION_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "IS_TEAM_LEAD", "type": "string", "nullable": true },
    { "name": "IS_ACTIVE_FLAG", "type": "string", "nullable": true },
    { "name": "IS_PRESENT_FLAG", "type": "string", "nullable": true },
    { "name": "USER_POSITION_DATA", "type": "string", "nullable": true },
    { "name": "REPORT_TO_POSITION_DATA", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CAMS.USER_PROFILE":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "GUID_TEXT", "type": "string", "nullable": true },
    { "name": "USERNAME_TEXT", "type": "string", "nullable": true },
    { "name": "TITLE_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "FIRST_NAME", "type": "string", "nullable": true },
    { "name": "MIDDLE_NAME", "type": "string", "nullable": true },
    { "name": "LAST_NAME", "type": "string", "nullable": true },
    { "name": "DISPLAY_NAME", "type": "string", "nullable": true },
    { "name": "DEFAULT_LANGUAGE_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "WORK_EMAIL_TEXT", "type": "string", "nullable": true },
    { "name": "WORK_PHONE_TEXT", "type": "string", "nullable": true },
    { "name": "WORK_MOBILE_TEXT", "type": "string", "nullable": true },
    { "name": "HOME_EMAIL_TEXT", "type": "string", "nullable": true },
    { "name": "HOME_PHONE_TEXT", "type": "string", "nullable": true },
    { "name": "HOME_MOBILE_TEXT", "type": "string", "nullable": true },
    { "name": "GENDER_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "START_DATE", "type": "string", "nullable": true },
    { "name": "END_DATE", "type": "string", "nullable": true },
    { "name": "CREATED_DATETIME", "type": "string", "nullable": true },
    { "name": "MODIFIED_DATETIME", "type": "string", "nullable": true },
    { "name": "CREATED_BY_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "MODIFIED_BY_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CUID_SEQUENCE", "type": "decimal(19,0)", "nullable": true },
    { "name": "LEGAL_FIRST_NAME", "type": "string", "nullable": true },
    { "name": "LEGAL_LAST_NAME", "type": "string", "nullable": true },
    { "name": "LEGAL_MIDDLE_NAME", "type": "string", "nullable": true },
    { "name": "LEGACY_ID", "type": "string", "nullable": true },
    { "name": "WORK_FAX_TEXT", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CURAM.USERS":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ACCOUNTENABLED", "type": "string", "nullable": true },
    { "name": "APPLICATIONCODE", "type": "string", "nullable": true },
    { "name": "BUSINESSEMAILID", "type": "decimal(19,0)", "nullable": true },
    { "name": "BUSINESSPHONEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CREATIONDATE", "type": "string", "nullable": true },
    { "name": "CTIENABLED", "type": "string", "nullable": true },
    { "name": "DEFAULTPRINTERID", "type": "decimal(19,0)", "nullable": true },
    { "name": "FAXID", "type": "decimal(19,0)", "nullable": true },
    { "name": "FIRSTNAME", "type": "string", "nullable": true },
    { "name": "FULLNAME", "type": "string", "nullable": true },
    { "name": "GRADECODE", "type": "string", "nullable": true },
    { "name": "LASTSUCCESSLOGIN", "type": "string", "nullable": true },
    { "name": "LOCATIONID", "type": "decimal(19,0)", "nullable": true },
    { "name": "LOGINDAYFRI", "type": "string", "nullable": true },
    { "name": "LOGINDAYMON", "type": "string", "nullable": true },
    { "name": "LOGINDAYSAT", "type": "string", "nullable": true },
    { "name": "LOGINDAYSUN", "type": "string", "nullable": true },
    { "name": "LOGINDAYTHURS", "type": "string", "nullable": true },
    { "name": "LOGINDAYTUES", "type": "string", "nullable": true },
    { "name": "LOGINDAYWED", "type": "string", "nullable": true },
    { "name": "LOGINFAILURES", "type": "decimal(19,0)", "nullable": true },
    { "name": "LOGINRESTRICTIONS", "type": "string", "nullable": true },
    { "name": "LOGINTIMEFROM", "type": "string", "nullable": true },
    { "name": "LOGINTIMETO", "type": "string", "nullable": true },
    { "name": "LOGSSINCEPWDCHANGE", "type": "decimal(19,0)", "nullable": true },
    { "name": "MOBILEPHONEID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PAGERID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PASSWORD", "type": "string", "nullable": true },
    { "name": "PASSWORDCHANGED", "type": "string", "nullable": true },
    { "name": "PASSWORDEXPIRYDATE", "type": "string", "nullable": true },
    { "name": "PERSONALEMAILID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PERSONALPHONENUMBERID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PWDCHANGEAFTERXLOG", "type": "decimal(19,0)", "nullable": true },
    { "name": "PWDCHANGEEVERYXDAY", "type": "decimal(19,0)", "nullable": true },
    { "name": "ROLENAME", "type": "string", "nullable": true },
    { "name": "SENSITIVITY", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "SURNAME", "type": "string", "nullable": true },
    { "name": "TITLE", "type": "string", "nullable": true },
    { "name": "USERNAME", "type": "string", "nullable": true },
    { "name": "DEFAULTLOCALE", "type": "string", "nullable": true },
    { "name": "ENDDATE", "type": "string", "nullable": true },
    { "name": "USERPREFSETID", "type": "string", "nullable": true },
    { "name": "UPPERFIRSTNAME", "type": "string", "nullable": true },
    { "name": "UPPERSURNAME", "type": "string", "nullable": true },
    { "name": "UPPERUSERNAME", "type": "string", "nullable": true },
    { "name": "UPPERROLENAME", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CAMS.LOCATION": """
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "LOCATION_CODE_NUM", "type": "decimal(19,0)", "nullable": true },
    { "name": "LOCATION_CODE", "type": "string", "nullable": true },
    { "name": "LOCATION_TYPE_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "PROGRAM_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "DESCRIPTION", "type": "string", "nullable": true },
    { "name": "IS_ACTIVE_FLAG", "type": "string", "nullable": true },
    { "name": "ARCHIVE_TEAM_ID", "type": "decimal(19,0)", "nullable": true },
    { "name": "LOCATION_CODE_NUM2", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CURAM.CONCERNROLEADDRESS": """
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "string" },
    { "name": "CONCERNROLEADDRESSID", "type": "decimal(19,0)", "nullable": true },
    { "name": "CONCERNROLEID", "type": "decimal(19,0)", "nullable": true },    
    { "name": "ADDRESSID", "type": "decimal(19,0)", "nullable": true },        
    { "name": "TYPECODE", "type": "string", "nullable": true },
    { "name": "STARTDATE", "type": "string", "nullable": true },
    { "name": "ENDDATE", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "decimal(19,0)", "nullable": true },  
    { "name": "LASTWRITTEN", "type": "string", "nullable": true }
  ]
}

"""
}

# COMMAND ----------

schema_map = {
    "CPIN_CURAM.PROVIDER": """
    {
        "fields": [
            { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PROVIDERCONCERNROLEID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PROVIDERENQUIRYID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PHYSICALCAPACITY", "nullable": true, "type": "integer" },
            { "metadata": {}, "name": "PAYMENTFREQUENCY", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "METHODOFPAYMENT", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "CURRENCYTYPE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RECORDSTATUS", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "RESERVATIONGRACEPERIOD", "nullable": true, "type": "integer" },
            { "metadata": {}, "name": "OVERRIDEMDRIND", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PREFERREDSEMETHOD", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "AREASSVDINFOTXTID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "CLIENTINFOTEXTID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "NAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "NAMEUPPER", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "ENROLLMENTDATETIME", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "ENDDATETIME", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "ACCEPTCWREFERRAL", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "VERSIONNO", "nullable": true, "type": "integer" },
            { "metadata": {}, "name": "LASTWRITTEN", "nullable": true, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,
    "CPIN_CURAM.CONCERNROLE": """
    {
        "fields": [
            { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "CONCERNID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "CONCERNROLEID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "CONCERNROLETYPE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "CREATIONDATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "REGISTRATIONDATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "STARTDATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "ENDDATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "STATUSCODE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "CONCERNROLENAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PRIMARYADDRESSID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PRIMARYALTERNATEID", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "COMMENTS", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PRIMARYPHONENUMBERID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PRIMARYEMAILADDRESSID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PRIMARYBANKACCOUNTID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "REGUSERNAME", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PREFPUBLICOFFICEID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "PREFERREDLANGUAGE", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "SENSITIVITY", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PREFCOMMMETHOD", "nullable": true, "type": "string" },
            { "metadata": {}, "name": "PREFCOMMFROMDATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "PREFCOMMTODATE", "nullable": true, "type": "timestamp" },
            { "metadata": {}, "name": "PRIMARYWEBADDRESSID", "nullable": true, "type": "long" },
            { "metadata": {}, "name": "VERSIONNO", "nullable": true, "type": "integer" },
            { "metadata": {}, "name": "LASTWRITTEN", "nullable": true, "type": "timestamp" }
        ],
        "type": "struct"
    }
    """,
    "CPIN_CURAM.MCYSPROVIDEREXT": """
    {
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
        { "metadata": {}, "name": "PROVIDERCONCERNROLEID", "nullable": true, "type": "long" },
        { "metadata": {}, "name": "PROVIDERCONSENTFORCAPTURE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "PROVIDERNATIVESTATUS", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "INDIGENOUSPERSONIND", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "INDIGENOUSGROUPCODE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "EMAILADDRESSID", "nullable": true, "type": "long" },
        { "metadata": {}, "name": "PROVIDERCONSENTFORSEARCH", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "PRIMARYPROVIDERTYPEID", "nullable": true, "type": "long" },
        { "metadata": {}, "name": "VERSIONNO", "nullable": true, "type": "integer" },
        { "metadata": {}, "name": "LASTWRITTEN", "nullable": true, "type": "timestamp" }
    ],
    "type": "struct"
    }
    """,
"CPIN_CURAM.PROVIDERENQUIRY": """
    {
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
        {"metadata": {}, "name": "PROVIDERENQUIRYID", "type":	"long", "nullable": true},
        {"metadata": {}, "name": "REFERENCENUMBER", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "NAME", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "HOMEADDRESSID", "type":	"long", "nullable": true},
        {"metadata": {}, "name": "WORKADDRESSID", "type":	"long", "nullable": true},
        {"metadata": {}, "name": "HOMEPHONENUMBERID", "type":	"long", "nullable": true},
        {"metadata": {}, "name": "WORKPHONENUMBERID", "type":	"long", "nullable": true},
        {"metadata": {}, "name": "MOBILEPHONENUMBERID", "type":	"long", "nullable": true},
        {"metadata": {}, "name": "PROVIDERCATEGORYPERIODID", "type":	"long", "nullable": true},
        {"metadata": {}, "name": "ADDITIONALNAME", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "PREFERREDSESSION", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "REASONFORENQUIRY", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "CONFIRMEDMEETINGDETAILS", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "ATTENDEDMEETING", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "OBTAINEDAPPLICATIONFORM", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "SCHEDULEDMEETING", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "NOOFCHILDREN", "type":	"long", "nullable": true},
        {"metadata": {}, "name": "AVAILABILITYFORCONTACT", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "ENQUIRYDATE", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "ENDDATE", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "OWNERNAME", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "RECORDSTATUS", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "PREFERREDLANGUAGE", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "PREFERREDCOMMUNICATION", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "NAMEUPPER", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "ORGOBJECTLINKID", "type":	"long", "nullable": true},
        {"metadata": {}, "name": "VERSIONNO", "type":	"long", "nullable": true},
        {"metadata": {}, "name": "LASTWRITTEN", "type":	"string", "nullable": true},
        {"metadata": {}, "name": "ADDITIONALINFORMATION", "type":	"string", "nullable": true}
],
"type": "struct"
}
""",
"CPIN_CURAM.CONCERNROLEALTERNATEID": """
    {
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
        { "metadata": {}, "name": "CONCERNROLEID", "nullable": true, "type": "long" },
        { "metadata": {}, "name": "ALTERNATEID", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "TYPECODE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "STARTDATE", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "ENDDATE", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "STATUSCODE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "CONCERNROLEALTERNATEID", "nullable": true, "type": "long" },
        { "metadata": {}, "name": "COMMENTS", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "VERSIONNO", "nullable": true, "type": "integer" },
        { "metadata": {}, "name": "LASTWRITTEN", "nullable": true, "type": "timestamp" }
    ],
    "type": "struct"
    }
    """,
    "CPIN_CURAM.CODETABLEITEM": """
    {
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
        { "metadata": {}, "name": "TABLENAME", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "CODE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "DESCRIPTION", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "ANNOTATION", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "ISENABLED", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "SORTORDER", "nullable": true, "type": "integer" },
        { "metadata": {}, "name": "LOCALEIDENTIFIER", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "PARENTCODE", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "COMMENTS", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "VERSIONNO", "nullable": true, "type": "integer" },
        { "metadata": {}, "name": "LASTWRITTEN", "nullable": true, "type": "timestamp" }
    ],
    "type": "struct"
    }
    """,
    "CPIN_CURAM.ADDRESS": """{
        "type": "struct",
        "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
        {"name": "ADDRESSID", "type": "long", "nullable": false},
        {"name": "COUNTRYCODE", "type": "string", "nullable": true},
        {"name": "MODIFIABLEIND", "type": "string", "nullable": false},
        {"name": "ADDRESSDATA", "type": "string", "nullable": true},
        {"name": "ADDRESSLAYOUTTYPE", "type": "string", "nullable": true},
        {"name": "GEOCODE", "type": "string", "nullable": true},
        {"name": "LATITUDE", "type": "double", "nullable": true},
        {"name": "LONGITUDE", "type": "double", "nullable": true},
        {"name": "VERSIONNO", "type": "long", "nullable": false},
        {"name": "LASTWRITTEN", "type": "timestamp", "nullable": true}
        ]
    }""",
    "CPIN_CURAM.ADDRESSELEMENT": """{
        "type": "struct",
        "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
        {"name": "ADDRESSELEMENTID", "type": "long", "nullable": false},
        {"name": "ADDRESSID", "type": "long", "nullable": false},
        {"name": "ELEMENTTYPE", "type": "string", "nullable": true},
        {"name": "ELEMENTVALUE", "type": "string", "nullable": true},
        {"name": "UPPERELEMENTVALUE", "type": "string", "nullable": true},
        {"name": "LASTWRITTEN", "type": "timestamp", "nullable": true}
        ]
    }""",
    "CPIN_CURAM.ADMINISTRATIONCONCERNROLE": """{
        "type": "struct",
        "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
        {"name": "ADMINISTRATIONCONCERNROLEID", "type": "long", "nullable": false},
        {"name": "CONCERNROLEID", "type": "long", "nullable": false},
        {"name": "ADMINISTRATIONROLEID", "type": "long", "nullable": false},
        {"name": "STARTDATE", "type": "timestamp", "nullable": true},
        {"name": "ENDDATE", "type": "timestamp", "nullable": true},
        {"name": "TYPECODE", "type": "string", "nullable": true},
        {"name": "VERSIONNO", "type": "long", "nullable": false},
        {"name": "LASTWRITTEN", "type": "timestamp", "nullable": true}
        ]
    }""",
    "CPIN_CURAM.ADMINISTRATIONROLE": """{
        "type": "struct",
        "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
        {"name": "ADMINISTRATIONROLEID", "type": "long", "nullable": false},
        {"name": "USERNAME", "type": "string", "nullable": true},
        {"name": "STATUSCODE", "type": "string", "nullable": true},
        {"name": "ORGOBJECTLINKID", "type": "long", "nullable": true},
        {"name": "VERSIONNO", "type": "long", "nullable": false},
        {"name": "LASTWRITTEN", "type": "timestamp", "nullable": true}
        ]
    }""",
    "CPIN_CURAM.ALLEGATIONROLE": """{
        "type": "struct",
        "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
        {"name": "ALLEGATIONROLEID", "type": "long", "nullable": false},
        {"name": "ALLEGATIONID", "type": "long", "nullable": false},
        {"name": "CASEPARTICIPANTROLEID", "type": "long", "nullable": true},
        {"name": "ROLETYPE", "type": "string", "nullable": true},
        {"name": "VERSIONNO", "type": "long", "nullable": false},
        {"name": "LASTWRITTEN", "type": "timestamp", "nullable": true}
        ]
    }""",
    "CPIN_CURAM.ALTERNATENAME": """{
    "type": "struct",
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
      {"name": "ALTERNATENAMEID", "type": "long", "nullable": false},
      {"name": "CONCERNROLEID", "type": "long", "nullable": false},
      {"name": "TITLE", "type": "string", "nullable": true},
      {"name": "FIRSTFORENAME", "type": "string", "nullable": true},
      {"name": "OTHERFORENAME", "type": "string", "nullable": true},
      {"name": "SURNAME", "type": "string", "nullable": true},
      {"name": "NAMESUFFIX", "type": "string", "nullable": true},
      {"name": "NAMETYPE", "type": "string", "nullable": true},
      {"name": "NAMESTATUS", "type": "string", "nullable": true},
      {"name": "FULLNAME", "type": "string", "nullable": true},
      {"name": "COMMENTS", "type": "string", "nullable": true},
      {"name": "INITIALS", "type": "string", "nullable": true},
      {"name": "UPPERFIRSTFORENAME", "type": "string", "nullable": true},
      {"name": "UPPERSURNAME", "type": "string", "nullable": true},
      {"name": "PHONETICENCODING", "type": "string", "nullable": true},
      {"name": "VERSIONNO", "type": "long", "nullable": false},
      {"name": "LASTWRITTEN", "type": "timestamp", "nullable": true}
    ]
  }""",
    "CPIN_CAMS.APPLICATION": """{
    "type": "struct",
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
      {"name": "ID", "type": "long", "nullable": true},
      {"name": "APP_CODE", "type": "string", "nullable": true},
      {"name": "APP_TYPE_CODE", "type": "string", "nullable": true},
      {"name": "PROGRAM_ID", "type": "long", "nullable": true},
      {"name": "DESCRIPTION", "type": "string", "nullable": true},
      {"name": "LOGIN_URL_KEY_TEXT", "type": "string", "nullable": true},
      {"name": "IS_ACTIVE_FLAG", "type": "string", "nullable": true}
    ]
  }""",
    "CPIN_CURAM.ATTACHMENT": """{
    "type": "struct",
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
      {"name": "ATTACHMENTID", "type": "long", "nullable": false},
      {"name": "ATTACHMENTCONTENTS", "type": "string", "nullable": true},
      {"name": "ATTACHMENTNAME", "type": "string", "nullable": true},
      {"name": "ATTACHMENTSTATUS", "type": "string", "nullable": true},
      {"name": "FILELOCATION", "type": "string", "nullable": true},
      {"name": "FILEREFERENCE", "type": "string", "nullable": true},
      {"name": "DOCUMENTTYPE", "type": "string", "nullable": true},
      {"name": "RECEIPTDATE", "type": "timestamp", "nullable": true},
      {"name": "STATUSCODE", "type": "string", "nullable": false},
      {"name": "ATTACHEDFILEIND", "type": "string", "nullable": false},
      {"name": "VERSIONNO", "type": "long", "nullable": false},
      {"name": "LASTWRITTEN", "type": "timestamp", "nullable": true}
    ]
  }""",
    "CPIN_CURAM.CASEPARTICIPANTROLE": """{
    "type": "struct",
    "fields": [
        { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
        { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
        { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
      {"name": "CASEPARTICIPANTROLEID", "type": "long", "nullable": false},
      {"name": "PARTICIPANTROLEID", "type": "long", "nullable": false},
      {"name": "CASEID", "type": "long", "nullable": false},
      {"name": "FROMDATE", "type": "timestamp", "nullable": true},
      {"name": "TODATE", "type": "timestamp", "nullable": true},
      {"name": "TYPECODE", "type": "string", "nullable": true},
      {"name": "RECORDSTATUS", "type": "string", "nullable": true},
      {"name": "ENDREASON", "type": "string", "nullable": true},
      {"name": "COMMENTS", "type": "string", "nullable": true},
      {"name": "TRANSLATIONREQUIREDIND", "type": "string", "nullable": true},
      {"name": "VERSIONNO", "type": "long", "nullable": false},
      {"name": "LASTWRITTEN", "type": "timestamp", "nullable": true}
    ]
  }""",
"CPIN_CURAM.CASEUSERROLE": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CASEUSERROLEID", "type": "long", "nullable": false },
    { "name": "CASEID", "type": "long", "nullable": false },
    { "name": "FROMDATE", "type": "timestamp", "nullable": true },
    { "name": "TODATE", "type": "timestamp", "nullable": true },
    { "name": "TYPECODE", "type": "string", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "REASONCODE", "type": "string", "nullable": true },
    { "name": "ORGOBJECTLINKID", "type": "long", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": false },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONCERNROLEALTERNATEID": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true },
    { "name": "ALTERNATEID", "type": "string", "nullable": true },
    { "name": "TYPECODE", "type": "string", "nullable": true },
    { "name": "STARTDATE", "type": "timestamp", "nullable": true },
    { "name": "ENDDATE", "type": "timestamp", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "CONCERNROLEALTERNATEID", "type": "long", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONCERNROLEATTACHMENTLINK": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ATTACHMENTLINKID", "type": "long", "nullable": true },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true },
    { "name": "ATTACHMENTID", "type": "long", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "DESCRIPTION", "type": "string", "nullable": true },
    { "name": "DATERECEIVED", "type": "timestamp", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONCERNROLEEMAILADDRESS": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CONCERNROLEEMAILADDRESSID", "type": "long", "nullable": true },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true },
    { "name": "EMAILADDRESSID", "type": "long", "nullable": true },
    { "name": "TYPECODE", "type": "string", "nullable": true },
    { "name": "STARTDATE", "type": "timestamp", "nullable": true },
    { "name": "ENDDATE", "type": "timestamp", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONCERNROLEPHONENUMBER": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CONCERNROLEPHONENUMBERID", "type": "long", "nullable": true },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true },
    { "name": "PHONENUMBERID", "type": "long", "nullable": true },
    { "name": "TYPECODE", "type": "string", "nullable": true },
    { "name": "STARTDATE", "type": "timestamp", "nullable": true },
    { "name": "ENDDATE", "type": "timestamp", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONTACTLOG": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CONTACTLOGID", "type": "long", "nullable": true },
    { "name": "CONTACTLOGTYPE", "type": "string", "nullable": true },
    { "name": "AUTHOR", "type": "string", "nullable": true },
    { "name": "CREATEDDATETIME", "type": "timestamp", "nullable": true },
    { "name": "CREATEDBY", "type": "string", "nullable": true },
    { "name": "PURPOSE", "type": "string", "nullable": true },
    { "name": "STARTDATETIME", "type": "timestamp", "nullable": true },
    { "name": "ENDDATETIME", "type": "timestamp", "nullable": true },
    { "name": "METHOD", "type": "string", "nullable": true },
    { "name": "LOCATION", "type": "string", "nullable": true },
    { "name": "LOCATIONDESCRIPTION", "type": "string", "nullable": true },
    { "name": "ADDENDUMIND", "type": "string", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "NOTEID", "type": "long", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.CONTACTLOGCONCERN": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CONTACTLOGCONCERNID", "type": "long", "nullable": true },
    { "name": "CONTACTLOGID", "type": "long", "nullable": true },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.EMAILADDRESS": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "EMAILADDRESSID", "type": "long", "nullable": true },
    { "name": "EMAILADDRESS", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.LOCATION": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "LOCATIONID", "type": "long", "nullable": true },
    { "name": "NAME", "type": "string", "nullable": true },
    { "name": "ADDRESSID", "type": "long", "nullable": true },
    { "name": "DESCRIPTION", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "LOCATIONSTATUS", "type": "string", "nullable": true },
    { "name": "CREATIONDATE", "type": "timestamp", "nullable": true },
    { "name": "LOCATIONTYPE", "type": "string", "nullable": true },
    { "name": "PARENTLOCATIONID", "type": "long", "nullable": true },
    { "name": "ORGANISATIONID", "type": "long", "nullable": true },
    { "name": "ADDRESSCOMMENTS", "type": "string", "nullable": true },
    { "name": "CREATELOCATIONSID", "type": "string", "nullable": true },
    { "name": "MAINTAINSID", "type": "string", "nullable": true },
    { "name": "READSID", "type": "string", "nullable": true },
    { "name": "LOCATIONSTRUCTUREID", "type": "long", "nullable": true },
    { "name": "PUBLICOFFICEINDICATOR", "type": "string", "nullable": true },
    { "name": "PHONENUMBERID", "type": "long", "nullable": true },
    { "name": "FAXNUMBERID", "type": "long", "nullable": true },
    { "name": "EMAILADDRESSID", "type": "long", "nullable": true },
    { "name": "ROOTLOCATIONINDICATOR", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true },
    { "name": "UPPERNAME", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSALTERNATENAMEEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ALTERNATENAMEID", "type": "long", "nullable": true },
    { "name": "FORENAMEPHONETICENC", "type": "string", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true },
    { "name": "UPPERFULLNAME", "type": "string", "nullable": true },
    { "name": "SINGLENAMEIND", "type": "string", "nullable": true },
    { "name": "UPPERFULLNAMESEARCH", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSATTACHMENTLINKEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ATTACHMENTLINKID", "type": "long", "nullable": true },
    { "name": "RELATEDTYPE", "type": "string", "nullable": true },
    { "name": "RELATEDID", "type": "long", "nullable": true },
    { "name": "ATTACHMENTTYPECODE", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSIDBD": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true },
    { "name": "CITIZENSHIPSTATUSTABLIST", "type": "string", "nullable": true },
    { "name": "CITIZENSHIPOTHERCNTRYTABLIST", "type": "string", "nullable": true },
    { "name": "CITIZENSHIPOTHERCOUNTRY", "type": "string", "nullable": true },
    { "name": "ABORIGINALIND", "type": "string", "nullable": true },
    { "name": "ABORIGINALIDENTITY", "type": "string", "nullable": true },
    { "name": "ABORIGINALIDENTITYOTHER", "type": "string", "nullable": true },
    { "name": "BORNINCANADAIND", "type": "string", "nullable": true },
    { "name": "BORNCANADAPROVINCE", "type": "string", "nullable": true },
    { "name": "BORNCOUNTRY", "type": "string", "nullable": true },
    { "name": "BORNCOUNTRYOTHER", "type": "string", "nullable": true },
    { "name": "RACIALGROUPTABLIST", "type": "string", "nullable": true },
    { "name": "RACIALGROUPOTHER", "type": "string", "nullable": true },
    { "name": "ETHNICORIGINTABLIST", "type": "string", "nullable": true },
    { "name": "ETHNICORIGINOTHER", "type": "string", "nullable": true },
    { "name": "RELIGIONTABLIST", "type": "string", "nullable": true },
    { "name": "RELIGIONOTHER", "type": "string", "nullable": true },
    { "name": "HOMELANGTABLIST", "type": "string", "nullable": true },
    { "name": "HOMEINDIGLANGTABLIST", "type": "string", "nullable": true },
    { "name": "HOMEINDIGLANGOTHER", "type": "string", "nullable": true },
    { "name": "HOMEADDITIONALLANGTABLIST", "type": "string", "nullable": true },
    { "name": "HOMEADDITIONALLANGOTHER", "type": "string", "nullable": true },
    { "name": "BIRTHYEARIND", "type": "string", "nullable": true },
    { "name": "BIRTHYEAR", "type": "string", "nullable": true },
    { "name": "BIRTHSEX", "type": "string", "nullable": true },
    { "name": "LIVEDGENDERIDENTITYTABLIST", "type": "string", "nullable": true },
    { "name": "LIVEGENDERIDENTITYOTHER", "type": "string", "nullable": true },
    { "name": "SEXUALORIENTATIONTABLIST", "type": "string", "nullable": true },
    { "name": "SEXUALORIENTATIONOTHER", "type": "string", "nullable": true },
    { "name": "MARITALSTATUS", "type": "string", "nullable": true },
    { "name": "MARITALSTATUSOTHER", "type": "string", "nullable": true },
    { "name": "FAMILYSTATUS", "type": "string", "nullable": true },
    { "name": "FAMILYSTATUSOTHER", "type": "string", "nullable": true },
    { "name": "DISABILITYIND", "type": "string", "nullable": true },
    { "name": "POSTALCODEIND", "type": "string", "nullable": true },
    { "name": "POSTALCODE", "type": "string", "nullable": true },
    { "name": "POSTALCODE2", "type": "string", "nullable": true },
    { "name": "CHANGEREASON", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true },
    { "name": "CONSENTIND", "type": "string", "nullable": true },
    { "name": "IDBDVERSION", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSORGANISATIONUNITEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ORGANISATIONUNITID", "type": "long", "nullable": true },
    { "name": "AGENCYNUMBER", "type": "string", "nullable": true },
    { "name": "AGENCYCODE", "type": "string", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true },
    { "name": "PAYMENTEFFECTIVEDATE", "type": "timestamp", "nullable": true },
    { "name": "NAMETEXTID", "type": "long", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSPERSONEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true },
    { "name": "ETHINICORIGINTABCODES", "type": "string", "nullable": true },
    { "name": "RELIGION", "type": "string", "nullable": true },
    { "name": "ELIGFORFRENCHSERVICEIND", "type": "string", "nullable": true },
    { "name": "LIFEBOOKAVAILABLEIND", "type": "string", "nullable": true },
    { "name": "INTERPRETERREQIND", "type": "string", "nullable": true },
    { "name": "ABORIGINALANCESTRYCODE", "type": "string", "nullable": true },
    { "name": "NATIVESTATUSCODE", "type": "string", "nullable": true },
    { "name": "BANDNUMBER", "type": "string", "nullable": true },
    { "name": "LIVINGOFFRESERVECODE", "type": "string", "nullable": true },
    { "name": "PROVIDERMEMBERROLE", "type": "string", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true },
    { "name": "FIRSTLANGUAGE", "type": "string", "nullable": true },
    { "name": "OTHERRACEDESCRIPTION", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPCODETWO", "type": "string", "nullable": true },
    { "name": "OTHERRELIGIONDESCRIPTION", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPDESCRIPTION", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSPROSPECTPERSONEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true },
    { "name": "RELIGION", "type": "string", "nullable": true },
    { "name": "ELIGFORFRENCHSERVICEIND", "type": "string", "nullable": true },
    { "name": "INTERPRETERREQIND", "type": "string", "nullable": true },
    { "name": "ABORIGINALANCESTRYCODE", "type": "string", "nullable": true },
    { "name": "NATIVESTATUSCODE", "type": "string", "nullable": true },
    { "name": "BANDNUMBER", "type": "string", "nullable": true },
    { "name": "LIVINGOFFRESERVECODE", "type": "string", "nullable": true },
    { "name": "ETHNICORIGINTABCODES", "type": "string", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true },
    { "name": "FIRSTLANGUAGE", "type": "string", "nullable": true },
    { "name": "OTHERRACEDESCRIPTION", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPCODETWO", "type": "string", "nullable": true },
    { "name": "OTHERRELIGIONDESCRIPTION", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPDESCRIPTION", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSPROVIDERENQUIRYEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "PROVIDERENQUIRYID", "type": "long", "nullable": true },
    { "name": "PROVIDERCONSENTFORCAPTURE", "type": "string", "nullable": true },
    { "name": "ABORIGINALANCESTRYCODE", "type": "string", "nullable": true },
    { "name": "INDIGENOUSPERSONIND", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPCODE", "type": "string", "nullable": true },
    { "name": "CLOSUREREASON", "type": "string", "nullable": true },
    { "name": "EMAILADDRESSID", "type": "long", "nullable": true },
    { "name": "PROVIDERCONSENTFORSEARCH", "type": "string", "nullable": true },
    { "name": "PRIMARYPROVIDERTYPEID", "type": "long", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true },
    { "name": "CLOSURECOMMENTS", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.MCYSPROVIDERPARTYEXT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "PROVIDERPARTYID", "type": "long", "nullable": true },
    { "name": "DATEOFBIRTH", "type": "timestamp", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.ORGANISATIONSTRUCTURE": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ORGANISATIONSTRUCTUREID", "type": "long", "nullable": true },
    { "name": "NAME", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "ACTIVATIONDATE", "type": "timestamp", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "ORGANISATIONID", "type": "long", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.ORGANISATIONUNIT": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "BUSINESSTYPECODE", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "CREATIONDATE", "type": "timestamp", "nullable": true },
    { "name": "DEFAULTPRINTERID", "type": "long", "nullable": true },
    { "name": "NAME", "type": "string", "nullable": true },
    { "name": "ORGANISATIONUNITID", "type": "long", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "WEBADDRESS", "type": "string", "nullable": true },
    { "name": "READSID", "type": "string", "nullable": true },
    { "name": "MAINTAINSID", "type": "string", "nullable": true },
    { "name": "CREATEUNITSID", "type": "string", "nullable": true },
    { "name": "LOCATIONID", "type": "long", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true },
    { "name": "UPPERNAME", "type": "string", "nullable": true }
  ]
}""",
"CPIN_CURAM.ORGOBJECTLINK": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ORGOBJECTLINKID", "type": "long", "nullable": true },
    { "name": "ORGOBJECTREFERENCE", "type": "long", "nullable": true },
    { "name": "USERNAME", "type": "string", "nullable": true },
    { "name": "ORGOBJECTTYPE", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.ORGUNITPARENTLINK": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ORGUNITPARENTLINKID", "type": "long", "nullable": true },
    { "name": "ORGANISATIONSTRUCTUREID", "type": "long", "nullable": true },
    { "name": "ORGANISATIONUNITID", "type": "long", "nullable": true },
    { "name": "PARENTORGANISATIONUNITID", "type": "long", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.ORGUNITPOSITIONLINK": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ORGUNITPOSITIONLINKID", "type": "long", "nullable": true },
    { "name": "ORGANISATIONSTRUCTUREID", "type": "long", "nullable": true },
    { "name": "POSITIONID", "type": "long", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "ORGANISATIONUNITID", "type": "long", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.PERSON": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true },
    { "name": "PERSONBIRTHNAME", "type": "string", "nullable": true },
    { "name": "DATEOFBIRTH", "type": "timestamp", "nullable": true },
    { "name": "DATEOFDEATH", "type": "timestamp", "nullable": true },
    { "name": "GENDER", "type": "string", "nullable": true },
    { "name": "SPECIALINTERESTCODE", "type": "string", "nullable": true },
    { "name": "MARITALSTATUSCODE", "type": "string", "nullable": true },
    { "name": "NATIONALITYCODE", "type": "string", "nullable": true },
    { "name": "RESIDENCYABROADIND", "type": "string", "nullable": true },
    { "name": "MOTHERBIRTHSURNAME", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "TYPE", "type": "string", "nullable": true },
    { "name": "DATEOFBIRTHVERIND", "type": "string", "nullable": true },
    { "name": "DATEOFDEATHVERIND", "type": "string", "nullable": true },
    { "name": "PRIMARYALTERNATEID", "type": "string", "nullable": true },
    { "name": "COUNTRYOFBIRTH", "type": "string", "nullable": true },
    { "name": "PLACEOFBIRTH", "type": "string", "nullable": true },
    { "name": "PINNUMBER", "type": "string", "nullable": true },
    { "name": "PRIMARYALTERNATENAMEID", "type": "long", "nullable": true },
    { "name": "ETHNICORIGINCODE", "type": "string", "nullable": true },
    { "name": "PAYMENTFREQUENCY", "type": "string", "nullable": true },
    { "name": "NEXTPAYMENTDATE", "type": "timestamp", "nullable": true },
    { "name": "CURRENCYTYPE", "type": "string", "nullable": true },
    { "name": "METHODOFPMTCODE", "type": "string", "nullable": true },
    { "name": "UPPERPERSONBIRTHNAME", "type": "string", "nullable": true },
    { "name": "UPPERMOTHERBIRTHSURNAME", "type": "string", "nullable": true },
    { "name": "RACE", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPCODE", "type": "string", "nullable": true },
    { "name": "INDIGENOUSPERSONIND", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.PHONENUMBER": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "PHONENUMBERID", "type": "long", "nullable": true },
    { "name": "PHONECOUNTRYCODE", "type": "string", "nullable": true },
    { "name": "PHONEAREACODE", "type": "string", "nullable": true },
    { "name": "PHONENUMBER", "type": "string", "nullable": true },
    { "name": "PHONEEXTENSION", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.PROSPECTPERSON": """{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true },
    { "name": "PERSONBIRTHNAME", "type": "string", "nullable": true },
    { "name": "DATEOFBIRTH", "type": "timestamp", "nullable": true },
    { "name": "DATEOFDEATH", "type": "timestamp", "nullable": true },
    { "name": "GENDER", "type": "string", "nullable": true },
    { "name": "SPECIALINTERESTCODE", "type": "string", "nullable": true },
    { "name": "MARITALSTATUSCODE", "type": "string", "nullable": true },
    { "name": "NATIONALITYCODE", "type": "string", "nullable": true },
    { "name": "RESIDENCYABROADIND", "type": "string", "nullable": true },
    { "name": "MOTHERBIRTHSURNAME", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "TYPE", "type": "string", "nullable": true },
    { "name": "DATEOFBIRTHVERIND", "type": "string", "nullable": true },
    { "name": "DATEOFDEATHVERIND", "type": "string", "nullable": true },
    { "name": "PRIMARYALTERNATEID", "type": "string", "nullable": true },
    { "name": "COUNTRYOFBIRTH", "type": "string", "nullable": true },
    { "name": "PLACEOFBIRTH", "type": "string", "nullable": true },
    { "name": "PINNUMBER", "type": "string", "nullable": true },
    { "name": "PRIMARYALTERNATENAMEID", "type": "long", "nullable": true },
    { "name": "ETHNICORIGINCODE", "type": "string", "nullable": true },
    { "name": "PERSONCONCERNROLEID", "type": "long", "nullable": true },
    { "name": "FROMAGE", "type": "long", "nullable": true },
    { "name": "TOAGE", "type": "long", "nullable": true },
    { "name": "UPPERPERSONBIRTHNAME", "type": "string", "nullable": true },
    { "name": "UPPERMOTHERBIRTHSURNAME", "type": "string", "nullable": true },
    { "name": "RACE", "type": "string", "nullable": true },
    { "name": "INDIGENOUSGROUPCODE", "type": "string", "nullable": true },
    { "name": "INDIGENOUSPERSONIND", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}""",
"CPIN_CURAM.PROVIDEROFFERING":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "PROVIDEROFFERINGID", "type": "long", "nullable": true },
    { "name": "PROVIDERCONCERNROLEID", "type": "long", "nullable": true },
    { "name": "SERVICEOFFERINGID", "type": "long", "nullable": true },
    { "name": "STARTDATE", "type": "timestamp", "nullable": true },
    { "name": "ENDDATE", "type": "timestamp", "nullable": true },
    { "name": "ENDREASON", "type": "string", "nullable": true },
    { "name": "DENIALREASON", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "INTKPROCINFOTEXTID", "type": "long", "nullable": true },
    { "name": "CLIENTFEEINFTEXTID", "type": "long", "nullable": true },
    { "name": "ELIGIBILITYINFOTEXTID", "type": "long", "nullable": true },
    { "name": "DOCSREQDINFTEXTID", "type": "long", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}
""",
"CPIN_CURAM.PROVIDERPARTY":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "PROVIDERPARTYID", "type": "long", "nullable": true },
    { "name": "PROVIDERCONCERNROLEID", "type": "long", "nullable": true },
    { "name": "PARTYCONCERNROLEID", "type": "long", "nullable": true },
    { "name": "STARTDATE", "type": "timestamp", "nullable": true },
    { "name": "ENDDATE", "type": "timestamp", "nullable": true },
    { "name": "CATEGORY", "type": "string", "nullable": true },
    { "name": "TYPE", "type": "string", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "POSITION", "type": "string", "nullable": true },
    { "name": "STARTDATETIME", "type": "timestamp", "nullable": true },
    { "name": "ENDDATETIME", "type": "timestamp", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}
""",
"CPIN_CAMS.ROLE":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ID", "type": "long", "nullable": true },
    { "name": "ROLE_CODE", "type": "string", "nullable": true },
    { "name": "APP_ID", "type": "long", "nullable": true },
    { "name": "LOCATION_TYPE_ID", "type": "long", "nullable": true },
    { "name": "DESCRIPTION", "type": "string", "nullable": true },
    { "name": "IS_ACTIVE_FLAG", "type": "string", "nullable": true },
    { "name": "SHORT_ROLE_CODE", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CURAM.SERVICEOFFERING":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "SERVICEOFFERINGID", "type": "long", "nullable": true },
    { "name": "NAME", "type": "string", "nullable": true },
    { "name": "DESCRIPTION", "type": "string", "nullable": true },
    { "name": "STARTDATE", "type": "timestamp", "nullable": true },
    { "name": "ENDDATE", "type": "timestamp", "nullable": true },
    { "name": "UNITOFMEASURE", "type": "string", "nullable": true },
    { "name": "MAXIMUMUNITS", "type": "long", "nullable": true },
    { "name": "UNITFREQUENCY", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "RECORDSTATUS", "type": "string", "nullable": true },
    { "name": "NAMEUPPER", "type": "string", "nullable": true },
    { "name": "PLACEMENTPMTIND", "type": "string", "nullable": true },
    { "name": "SAPLMTINHIBITIND", "type": "string", "nullable": true },
    { "name": "PAYBASEDONATTENDANCEIND", "type": "string", "nullable": true },
    { "name": "TRAININGIND", "type": "string", "nullable": true },
    { "name": "SPECIALCONDITIONTYPE", "type": "string", "nullable": true },
    { "name": "PROVISIONMETHOD", "type": "string", "nullable": true },
    { "name": "DELIVERYFREQUENCY", "type": "string", "nullable": true },
    { "name": "SINGLEORMULTIPLECLIENTS", "type": "string", "nullable": true },
    { "name": "REFERENCE", "type": "string", "nullable": true },
    { "name": "NAMETEXTID", "type": "long", "nullable": true },
    { "name": "DESCRIPTIONTEXTID", "type": "long", "nullable": true },
    { "name": "REFERENCEUPPER", "type": "string", "nullable": true },
    { "name": "DELIVERYTYPE", "type": "string", "nullable": true },
    { "name": "AVAILABILITYCHECKEXCLUDEIND", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true },
    { "name": "REFERREDBY", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CAMS.USER_ACCESS": """
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ID", "type": "long", "nullable": true },
    { "name": "USER_ID", "type": "long", "nullable": true },
    { "name": "CUID_TEXT", "type": "string", "nullable": true },
    { "name": "LOCATION_ID", "type": "long", "nullable": true },
    { "name": "ROLE_ID", "type": "long", "nullable": true },
    { "name": "USER_POSITION_ID", "type": "long", "nullable": true },
    { "name": "REPORT_TO_POSITION_ID", "type": "long", "nullable": true },
    { "name": "IS_TEAM_LEAD", "type": "string", "nullable": true },
    { "name": "IS_ACTIVE_FLAG", "type": "string", "nullable": true },
    { "name": "IS_PRESENT_FLAG", "type": "string", "nullable": true },
    { "name": "USER_POSITION_DATA", "type": "string", "nullable": true },
    { "name": "REPORT_TO_POSITION_DATA", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CAMS.USER_PROFILE":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ID", "type": "long", "nullable": true },
    { "name": "GUID_TEXT", "type": "string", "nullable": true },
    { "name": "USERNAME_TEXT", "type": "string", "nullable": true },
    { "name": "TITLE_ID", "type": "long", "nullable": true },
    { "name": "FIRST_NAME", "type": "string", "nullable": true },
    { "name": "MIDDLE_NAME", "type": "string", "nullable": true },
    { "name": "LAST_NAME", "type": "string", "nullable": true },
    { "name": "DISPLAY_NAME", "type": "string", "nullable": true },
    { "name": "DEFAULT_LANGUAGE_ID", "type": "long", "nullable": true },
    { "name": "WORK_EMAIL_TEXT", "type": "string", "nullable": true },
    { "name": "WORK_PHONE_TEXT", "type": "string", "nullable": true },
    { "name": "WORK_MOBILE_TEXT", "type": "string", "nullable": true },
    { "name": "HOME_EMAIL_TEXT", "type": "string", "nullable": true },
    { "name": "HOME_PHONE_TEXT", "type": "string", "nullable": true },
    { "name": "HOME_MOBILE_TEXT", "type": "string", "nullable": true },
    { "name": "GENDER_ID", "type": "long", "nullable": true },
    { "name": "START_DATE", "type": "timestamp", "nullable": true },
    { "name": "END_DATE", "type": "timestamp", "nullable": true },
    { "name": "CREATED_DATETIME", "type": "timestamp", "nullable": true },
    { "name": "MODIFIED_DATETIME", "type": "timestamp", "nullable": true },
    { "name": "CREATED_BY_ID", "type": "long", "nullable": true },
    { "name": "MODIFIED_BY_ID", "type": "long", "nullable": true },
    { "name": "CUID_SEQUENCE", "type": "long", "nullable": true },
    { "name": "LEGAL_FIRST_NAME", "type": "string", "nullable": true },
    { "name": "LEGAL_LAST_NAME", "type": "string", "nullable": true },
    { "name": "LEGAL_MIDDLE_NAME", "type": "string", "nullable": true },
    { "name": "LEGACY_ID", "type": "string", "nullable": true },
    { "name": "WORK_FAX_TEXT", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CURAM.USERS":"""
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ACCOUNTENABLED", "type": "string", "nullable": true },
    { "name": "APPLICATIONCODE", "type": "string", "nullable": true },
    { "name": "BUSINESSEMAILID", "type": "long", "nullable": true },
    { "name": "BUSINESSPHONEID", "type": "long", "nullable": true },
    { "name": "CREATIONDATE", "type": "timestamp", "nullable": true },
    { "name": "CTIENABLED", "type": "string", "nullable": true },
    { "name": "DEFAULTPRINTERID", "type": "long", "nullable": true },
    { "name": "FAXID", "type": "long", "nullable": true },
    { "name": "FIRSTNAME", "type": "string", "nullable": true },
    { "name": "FULLNAME", "type": "string", "nullable": true },
    { "name": "GRADECODE", "type": "string", "nullable": true },
    { "name": "LASTSUCCESSLOGIN", "type": "timestamp", "nullable": true },
    { "name": "LOCATIONID", "type": "long", "nullable": true },
    { "name": "LOGINDAYFRI", "type": "string", "nullable": true },
    { "name": "LOGINDAYMON", "type": "string", "nullable": true },
    { "name": "LOGINDAYSAT", "type": "string", "nullable": true },
    { "name": "LOGINDAYSUN", "type": "string", "nullable": true },
    { "name": "LOGINDAYTHURS", "type": "string", "nullable": true },
    { "name": "LOGINDAYTUES", "type": "string", "nullable": true },
    { "name": "LOGINDAYWED", "type": "string", "nullable": true },
    { "name": "LOGINFAILURES", "type": "long", "nullable": true },
    { "name": "LOGINRESTRICTIONS", "type": "string", "nullable": true },
    { "name": "LOGINTIMEFROM", "type": "timestamp", "nullable": true },
    { "name": "LOGINTIMETO", "type": "timestamp", "nullable": true },
    { "name": "LOGSSINCEPWDCHANGE", "type": "long", "nullable": true },
    { "name": "MOBILEPHONEID", "type": "long", "nullable": true },
    { "name": "PAGERID", "type": "long", "nullable": true },
    { "name": "PASSWORD", "type": "string", "nullable": true },
    { "name": "PASSWORDCHANGED", "type": "timestamp", "nullable": true },
    { "name": "PASSWORDEXPIRYDATE", "type": "timestamp", "nullable": true },
    { "name": "PERSONALEMAILID", "type": "long", "nullable": true },
    { "name": "PERSONALPHONENUMBERID", "type": "long", "nullable": true },
    { "name": "PWDCHANGEAFTERXLOG", "type": "long", "nullable": true },
    { "name": "PWDCHANGEEVERYXDAY", "type": "long", "nullable": true },
    { "name": "ROLENAME", "type": "string", "nullable": true },
    { "name": "SENSITIVITY", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "SURNAME", "type": "string", "nullable": true },
    { "name": "TITLE", "type": "string", "nullable": true },
    { "name": "USERNAME", "type": "string", "nullable": true },
    { "name": "DEFAULTLOCALE", "type": "string", "nullable": true },
    { "name": "ENDDATE", "type": "timestamp", "nullable": true },
    { "name": "USERPREFSETID", "type": "string", "nullable": true },
    { "name": "UPPERFIRSTNAME", "type": "string", "nullable": true },
    { "name": "UPPERSURNAME", "type": "string", "nullable": true },
    { "name": "UPPERUSERNAME", "type": "string", "nullable": true },
    { "name": "UPPERROLENAME", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}
""",
"CPIN_CAMS.LOCATION": """
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "ID", "type": "long", "nullable": true },
    { "name": "LOCATION_CODE_NUM", "type": "long", "nullable": true },
    { "name": "LOCATION_CODE", "type": "string", "nullable": true },
    { "name": "LOCATION_TYPE_ID", "type": "long", "nullable": true },
    { "name": "PROGRAM_ID", "type": "long", "nullable": true },
    { "name": "DESCRIPTION", "type": "string", "nullable": true },
    { "name": "IS_ACTIVE_FLAG", "type": "string", "nullable": true },
    { "name": "ARCHIVE_TEAM_ID", "type": "long", "nullable": true },
    { "name": "LOCATION_CODE_NUM2", "type": "string", "nullable": true }
  ]
}
""",
"CPIN_CURAM.CONCERNROLEADDRESS": """
{
  "type": "struct",
  "fields": [
    { "metadata": {}, "name": "table", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_type", "nullable": true, "type": "string" },
    { "metadata": {}, "name": "op_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "current_ts", "nullable": true, "type": "timestamp" },
    { "metadata": {}, "name": "pos", "nullable": true, "type": "long" },
    { "name": "CONCERNROLEADDRESSID", "type": "long", "nullable": true },
    { "name": "CONCERNROLEID", "type": "long", "nullable": true },    
    { "name": "ADDRESSID", "type": "long", "nullable": true },        
    { "name": "TYPECODE", "type": "string", "nullable": true },
    { "name": "STARTDATE", "type": "string", "nullable": true },
    { "name": "ENDDATE", "type": "string", "nullable": true },
    { "name": "STATUSCODE", "type": "string", "nullable": true },
    { "name": "COMMENTS", "type": "string", "nullable": true },
    { "name": "VERSIONNO", "type": "long", "nullable": true },  
    { "name": "LASTWRITTEN", "type": "timestamp", "nullable": true }
  ]
}

"""
}

# COMMAND ----------

table_df = spark.sql(f'''
                        select distinct Table_Name, trim(Source_Folder_Name) Source_Folder_Name, Table_Type, PK_Name from {meta_catalog}.idh_config.ref_bronze_tables 
                        where source_system='{source_system}' and is_enabled = 1 
                        and coalesce(pk_name,'') != ''
                        ''')
table_list = table_df.collect()

# COMMAND ----------

# for source_table, keys in query output table_list:
for row in table_list:

    source_table = row['Table_Name'].strip()

    load_type = row['Table_Type'].lower().replace('type_','') if row['Table_Type'] else '1'

    source_folder = row['Source_Folder_Name'].strip()

    keys = row['PK_Name'].split(',') if row['PK_Name'] else []
    
    target_table = source_table.replace('.','_').lower() 

    dataFlowId = f'1000-{source_table}' # Dataflow ID -PK
    
    dataFlowGroup = dataflow_group

    sourceFormat = "cloudFiles" 

    source_schema = source_schema_map.get(source_folder, None)
    
    sourceDetails = {
        "path": f"/Volumes/{source_volume_catalog}{raw_volume_path}{source_folder}/*",
        "source_table": source_table,
        "schema_path": f"/Volumes/{source_volume_catalog}{schema_path}{source_table}",
        "source_schema": source_schema,
        "file_format": "custom-parquet"
    }
    
    highWaterMark = {"contract_id":f"{dataFlowId}","contract_version":"1.000","contract_major_version":"1","watermark_column": "processing_time"} 

    # readerConfigOptions ={
    #         "cloudFiles.format": "parquet",
    #         "cloudFiles.rescuedDataColumn": "_rescued_data",
    #         "cloudFiles.inferColumnTypes": "true",
    #         "cloudFile.readerCaseSensitive": "false",
    #         "cloudFiles.useNotifications": "true", 
    #         "mergeSchema": "false",             
    #         "header": "true",
    #         "inferSchema": "true",
    #         'cloudFiles.schemaEvolutionMode': "none"           
    #     } 
    
    # readerConfigOptions ={
    #         "cloudFiles.format": "parquet",
    #         "cloudFiles.rescuedDataColumn": "_rescued_data",
    #         "cloudFile.readerCaseSensitive": "false",
    #         "cloudFiles.useManagedFileEvents": "true", 
    #         "mergeSchema": "false",             
    #         "header": "true",
    #         'cloudFiles.schemaEvolutionMode': "none"           
    #     } 

    readerConfigOptions ={
            "cloudFiles.format": "parquet",
            "cloudFile.readerCaseSensitive": "false",
            "cloudFiles.inferColumnTypes": "true",
            "cloudFiles.rescuedDataColumn": "_rescued_data",
            "cloudFiles.useManagedFileEvents": "true", 
            "cloudFiles.includeExistingFiles": "true",
            "mergeSchema": "false",           
            "header": "true",  
            "inferSchema": "true",
            'cloudFiles.schemaEvolutionMode': "none"           
        }     

    cloudFileNotificationsConfig = {
            "cloudFiles.subscriptionId": subscription,
            "cloudFiles.tenantId": tenant,
            "cloudFiles.resourceGroup": resource_group,
            "cloudFiles.clientId": "oncareappClientId",
            "cloudFiles.clientSecret": "oncareappClientSecret",
            "secret_scope": "oncare-secrets",
            "cloudFiles.queueName": queue_name,
    }

    targetFormat = 'delta' 

    schema = schema_map.get(source_folder, None)


    targetDetails = {"database": f"{target_catalog}.{target_schema}", "schema":target_schema, "table": target_table}
    tableProperties = {"delta.enableChangeDataFeed": "true", "delta.enableDeletionVectors": "false"}
    partitionColumns = None 
    liquidClusteringColumns = keys if source_table not in ["cpin_curam_users"] else None

    # "apply_as_deletes": "op_type = \'D\'",
    cdcApplyChanges = json.dumps({
        "track_history_except_column_list": None,
        "except_column_list": ["table", "op_type", "op_ts", "current_ts", "pos"],
        "keys": keys,
        "scd_type": f"{load_type}",
        "sequence_by": ["op_ts","pos"]
    }) 
    dataQualityExpectations = None # Example: '{"expect_or_drop": {"no_rescued_data": "_rescued_data IS NULL","valid_customer_id": "customers_id IS NOT NULL"}}'  Documentation: https://docs.databricks.com/en/delta-live-tables/expectations.html
    quarantineTargetDetails = None
    quarantineTableProperties = None
    createDate = datetime.datetime.now()
    updateDate = datetime.datetime.now()
    createdBy =  spark.range(1).select(current_user()).head()[0]
    updatedBy = spark.range(1).select(current_user()).head()[0]
    BRONZE_MD_TABLE = f"{meta_catalog}.{meta_schema}.bronze_dataflowspec_table" # Bronze Metadata Table

    ## Populate Bronze function, merges changes in to the MD table. If there are no changes, it will IGNORE and the version will not be incremented.

    populate_bronze(BRONZE_MD_TABLE,dataFlowId,dataFlowGroup,sourceFormat,sourceDetails,highWaterMark,readerConfigOptions,cloudFileNotificationsConfig,schema,targetFormat,targetDetails,tableProperties,partitionColumns,liquidClusteringColumns,cdcApplyChanges,dataQualityExpectations,quarantineTargetDetails,quarantineTableProperties,createDate,createdBy,updateDate,updatedBy,spark)