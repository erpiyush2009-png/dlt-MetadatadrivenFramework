import dlt
import json
import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr,lit, struct, to_date, when, current_timestamp, from_utc_timestamp, upper
from pyspark.sql.types import StructType, StructField
import inspect

class ChangeFunctionRegistry:
    _registry = {}

    @classmethod
    def register(cls, name):
        def decorator(func):
            cls._registry[name.upper()] = func
            return func
        return decorator

    @classmethod
    def run(cls, spark, target_table, bronze_database, silver_database, silver_view):
        if target_table.upper() not in cls._registry:
            raise ValueError(f"Unsupported target_table: {target_table}")

        try:
            return cls._registry[target_table.upper()](
                spark, bronze_database, silver_database, silver_view
            )
        except Exception as e:
            raise RuntimeError(
                f"Error running change function for {target_table}"
            ) from e

@ChangeFunctionRegistry.register("PROVIDER")
def create_provider_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed(
        "PROVIDER_CONCERN_ROLE_ID", "View_PROVIDER_CONCERN_ROLE_ID"
    )

    columns = [
        col for col in df_view.columns if col not in ["View_PROVIDER_CONCERN_ROLE_ID"]
    ]
    columns.append("PROVIDER_CONCERN_ROLE_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    df_provider = (
        dlt.read(f"{bronze_database}.cpin_curam_provider")
        .select("PROVIDERCONCERNROLEID", "PROVIDERENQUIRYID")
        .distinct()
    )

    df_providerchanges_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_provider")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "PROVIDERCONCERNROLEID",
            "Source_Update_Type",
            "_change_type",
            "_commit_timestamp",
        )
    )

    df_providerchanges = (
        df_providerchanges_all.filter("_change_type in ('update_postimage', 'insert')")
        .select("PROVIDERCONCERNROLEID", "Source_Update_Type", "_commit_timestamp")
    )

    df_concernrole = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_concernrole")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("CONCERNROLEID").alias("PROVIDERCONCERNROLEID"),
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )

    df_mcysproviderext = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_mcysproviderext")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select("PROVIDERCONCERNROLEID", "Source_Update_Type", "_commit_timestamp")
    )

    df_providerenquiry = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_providerenquiry")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select("PROVIDERENQUIRYID", "Source_Update_Type", "_commit_timestamp")
    )

    df_union = (
        df_providerchanges.union(df_concernrole)
        .union(df_mcysproviderext)
        .select(
            col("PROVIDERCONCERNROLEID").alias("PROVIDER_CONCERN_ROLE_ID"),
            "Source_Update_Type",
            col("_commit_timestamp").alias("op_ts"),
        )
    )

    df_update = (
        df_union.join(
            df_view,
            df_view.View_PROVIDER_CONCERN_ROLE_ID == df_union.PROVIDER_CONCERN_ROLE_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("ALLEGATION_ROLE")
def create_allegation_role_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed(
        "CASE_PARTICIPANT_ROLE_ID", "View_CASE_PARTICIPANT_ROLE_ID"
    )
    columns = [
        col for col in df_view.columns if col not in ["View_CASE_PARTICIPANT_ROLE_ID"]
    ]
    columns.append("CASE_PARTICIPANT_ROLE_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    df_allegationrole_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_allegationrole")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "CASEPARTICIPANTROLEID",
            "Source_Update_Type",
            "_change_type",
            "_commit_timestamp",
        )
    )

    df_allegationrole = (
        df_allegationrole_all.filter("_change_type in ('update_postimage', 'insert')")
        .select("CASEPARTICIPANTROLEID", "Source_Update_Type", "_commit_timestamp")
    )

    df_caseparticipantrole = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_caseparticipantrole")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("CASEPARTICIPANTROLEID"), 
                "Source_Update_Type",
                 "_commit_timestamp")
    )
    
    df_concernrole = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_concernrole")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("CONCERNROLEID"),
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )

    df_union = (
        df_allegationrole.union(df_caseparticipantrole)
        .select(
            col("CASEPARTICIPANTROLEID").alias("CASE_PARTICIPANT_ROLE_ID"),
            "Source_Update_Type",
            col("_commit_timestamp").alias("op_ts"),
        )
    )

    df_update = (
        df_union.join(
            df_view,
            df_union.CASE_PARTICIPANT_ROLE_ID == df_view.View_CASE_PARTICIPANT_ROLE_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("ADMINISTRATION_CONCERN_ROLE")
def create_administration_concern_role_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed(
        "ADMINISTRATION_ROLE_ID", "View_ADMINISTRATION_ROLE_ID"
    )
    # build select list (keep all view columns + CDC metadata)
    columns = [c for c in df_view.columns if c not in ["View_ADMINISTRATION_ROLE_ID"]]
    columns.append("ADMINISTRATION_ROLE_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # CDC from the administrationconcernrole table (direct mapping)
    df_administrationconcernrole_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_administrationconcernrole")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "ADMINISTRATIONROLEID",
            "Source_Update_Type",
            "_change_type",
            "_commit_timestamp",
        )
    )

    df_administrationconcernrole = (
        df_administrationconcernrole_all.filter("_change_type in ('update_postimage', 'insert')")
        .select("ADMINISTRATIONROLEID", "Source_Update_Type", "_commit_timestamp")
    )

    df_administrationrole = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_administrationrole")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("ADMINISTRATIONROLEID"),
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )

    df_concernrole = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_concernrole")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("CONCERNROLEID").alias("ADMINISTRATIONROLEID"),
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )


    # union mapped CDC streams (direct ACR changes + role/concernrole mapped changes)
    df_union =( df_administrationconcernrole
               .union(df_administrationrole)
               .union(df_concernrole)
               .select(
               col("ADMINISTRATIONROLEID").alias("ADMINISTRATION_ROLE_ID"),
               "Source_Update_Type",
               col("_commit_timestamp").alias("op_ts"))
               )

    # join back to the view to get full row shape
    df_update = (
        df_union.join(
            df_view,
            df_view.View_ADMINISTRATION_ROLE_ID == df_union.ADMINISTRATION_ROLE_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn("Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York"))
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("ALTERNATE_NAME")
def create_alternate_name_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed(
        "ALTERNATE_NAME_ID", "View_ALTERNATE_NAME_ID"
    )  
    columns = [
        col for col in df_view.columns if col not in ["View_ALTERNATE_NAME_ID"]
    ]
    columns.append("ALTERNATE_NAME_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    df_alternatename_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_alternatename")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "ALTERNATENAMEID",
            "Source_Update_Type",
            "_change_type",
            "_commit_timestamp",
        )
    )

    df_alternatename = (
        df_alternatename_all.filter("_change_type in ('update_postimage', 'insert')")
        .select("ALTERNATENAMEID", "Source_Update_Type", "_commit_timestamp")
    )

    df_mcysalternatenameext = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_mcysalternatenameext")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select("ALTERNATENAMEID", "Source_Update_Type", "_commit_timestamp")
    )

    df_union = (
        df_alternatename.union(df_mcysalternatenameext)
        .select(
            col("ALTERNATENAMEID").alias("ALTERNATE_NAME_ID"),
            "Source_Update_Type",
            col("_commit_timestamp").alias("op_ts"),
        )
    )

    df_update = (
        df_union.join(
            df_view,
            df_view.View_ALTERNATE_NAME_ID == df_union.ALTERNATE_NAME_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("ATTACHMENT")
def create_attachment_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed(
        "ATTACHMENT_ID", "View_ATTACHMENT_ID"
    )
    columns = [
        col for col in df_view.columns if col not in ["View_ATTACHMENT_ID"]
    ]
    columns.append("ATTACHMENT_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    df_attachment_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_attachment")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "ATTACHMENTID",
            "Source_Update_Type",
            "_change_type",
            "_commit_timestamp",
        )
    )

    df_attachment = (
        df_attachment_all.filter("_change_type in ('update_postimage', 'insert')")
        .select(col("ATTACHMENTID").alias("ATTACHMENT_ID"), "Source_Update_Type", col("_commit_timestamp").alias("op_ts"))
    )


    df_update = (
        df_attachment.join(
            df_view,
            df_view.View_ATTACHMENT_ID == df_attachment.ATTACHMENT_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("ADDRESS")
def create_address_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    
    df_view = df_view.withColumnRenamed(
        "Address_Id", "View_Address_Id"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_Address_Id"]]
    columns.append("Address_Id")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_address_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_address")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            col("AddressId").alias("Address_Id"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )
    df_address = (
        df_address_all.filter("_change_type in ('update_postimage', 'insert')")
        .select("Address_Id", "Source_Update_Type", "_commit_timestamp")
    )
    # Change feed for addresselement
    df_addresselement = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_addresselement")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("AddressId").alias("Address_Id"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )

    # Union all CDC sources
    df_union =( df_address.union(df_addresselement)
               .select(
               "Address_Id",
               "Source_Update_Type",
               col("_commit_timestamp").alias("op_ts"),
        )
    )

    # Join back with view to get full row
    df_update = (
        df_union.join(
            df_view,
            df_view.View_Address_Id == df_union.Address_Id,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("CAMS_USER_PROFILE_ROLE")
def create_cams_user_profile_role_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)

    df_view = df_view.withColumnRenamed(
        "USER_ACCESS_ID", "View_USER_ACCESS_ID"
    )
    
    columns = [
        col for col in df_view.columns if col not in ["View_USER_ACCESS_ID"]
    ]
    columns.append("USER_ACCESS_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Static mapping of relationships from user_access
    df_access = (
        dlt.read(f"{bronze_database}.cpin_cams_user_access")
        .select("ID", "USER_ID", "LOCATION_ID", "ROLE_ID")
        .withColumnRenamed("ID", "USER_ACCESS_ID")
        .distinct()
    )

    # --- CDF Readers ---

    # User Access (anchor table)
    df_access_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_cams_user_access")
        .filter("_change_type in ('insert','update_postimage')")
        .select(col("ID").alias("USER_ACCESS_ID"), "Source_Update_Type", "_commit_timestamp")
    )

    # User Profile
    df_profile_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_cams_user_profile")
        .filter("_change_type in ('insert','update_postimage')")
        .select(col("ID").alias("USER_ID"), "Source_Update_Type", "_commit_timestamp")
    )
    df_profile_change = (
        df_access.join(df_profile_cdf, df_access.USER_ID == df_profile_cdf.USER_ID, "inner")
        .select(df_access["USER_ACCESS_ID"], df_profile_cdf["Source_Update_Type"], df_profile_cdf["_commit_timestamp"])
    )

    # Location
    df_location_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_cams_location")
        .filter("_change_type in ('insert','update_postimage')")
        .select(col("ID").alias("LOCATION_ID"), "Source_Update_Type", "_commit_timestamp")
    )
    df_location_change = (
        df_access.join(df_location_cdf, df_access.LOCATION_ID == df_location_cdf.LOCATION_ID, "inner")
        .select(df_access["USER_ACCESS_ID"], df_location_cdf["Source_Update_Type"], df_location_cdf["_commit_timestamp"])
    )

    # Role
    df_role_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_cams_role")
        .filter("_change_type in ('insert','update_postimage')")
        .select(col("ID").alias("ROLE_ID"), "APP_ID", "Source_Update_Type", "_commit_timestamp")
    )
    df_role_change = (
        df_access.join(df_role_cdf, df_access.ROLE_ID == df_role_cdf.ROLE_ID, "inner")
        .select(df_access["USER_ACCESS_ID"], df_role_cdf["Source_Update_Type"], df_role_cdf["_commit_timestamp"])
    )

    # Application (linked through role → APP_ID)
    df_app_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_cams_application")
        .filter("_change_type in ('insert','update_postimage')")
        .select(col("ID").alias("APP_ID"), "Source_Update_Type", "_commit_timestamp")
    )
    df_app_change = (
        df_role_cdf.join(df_app_cdf, df_role_cdf.APP_ID == df_app_cdf.APP_ID, "inner")
        .join(df_access, df_access.ROLE_ID == df_role_cdf.ROLE_ID, "inner")
        .select(df_access["USER_ACCESS_ID"], df_app_cdf["Source_Update_Type"], df_app_cdf["_commit_timestamp"])
    )

    # --- Union all changes ---
    df_union = (
        df_access_cdf
        .union(df_profile_change)
        .union(df_location_change)
        .union(df_role_change)
        .union(df_app_change)
    ).select(
            "USER_ACCESS_ID",
            "Source_Update_Type",
            col("_commit_timestamp").alias("op_ts"))

    df_update = (
        df_union.join(df_view, df_union.USER_ACCESS_ID == df_view.View_USER_ACCESS_ID, "left")
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("CASE_USER_ROLE")
def create_case_user_role_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)

    df_view = df_view.withColumnRenamed("USER_NAME", "View_USER_NAME")

    columns = [c for c in df_view.columns if c not in ["View_USER_NAME"]]
    columns.append("USER_NAME")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # --- CDF Readers ---

    # CaseUserRole (anchor)
    df_role_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_caseuserrole")
        .filter("_change_type in ('insert','update_postimage')")
        .select(col("ORGOBJECTLINKID"), "Source_Update_Type", "_commit_timestamp")
    )

    # OrgObjectLink
    df_org_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_orgobjectlink")
        .filter("_change_type in ('insert','update_postimage')")
        .select("ORGOBJECTLINKID", "USERNAME", "Source_Update_Type", "_commit_timestamp")
    )

    df_org_change = (
        df_role_cdf.join(df_org_cdf, "ORGOBJECTLINKID", "inner")
        .select(df_org_cdf["USERNAME"].alias("USER_NAME"), df_org_cdf["Source_Update_Type"], df_org_cdf["_commit_timestamp"])
    )

    # Users
    df_users_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_users")
        .filter("_change_type in ('insert','update_postimage')")
        .select("USERNAME", "Source_Update_Type", "_commit_timestamp")
    )

    # --- Union all changes ---
    df_union = (
        df_org_change
        .union(df_users_cdf.select(col("USERNAME").alias("USER_NAME"), "Source_Update_Type", "_commit_timestamp"))
    ).select(
        col("USER_NAME"),
        "Source_Update_Type",
        col("_commit_timestamp").alias("op_ts")
    )

    # Join with base view
    df_update = (
        df_union.join(df_view, df_union.USER_NAME == df_view.View_USER_NAME, "left")
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn("Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York"))
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("CONCERN_ROLE")
def create_concern_role_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    
    df_view = df_view.withColumnRenamed(
        "PRIM_EMAIL_ADDRESS_ID", "View_PRIM_EMAIL_ADDRESS_ID"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_PRIM_EMAIL_ADDRESS_ID"]]
    columns.append("PRIM_EMAIL_ADDRESS_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_concern_role_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_concernrole")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            col("PRIMARYEMAILADDRESSID"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )
    df_concern_role = (
        df_concern_role_all.filter("_change_type in ('update_postimage', 'insert')")
        .select("PRIMARYEMAILADDRESSID", "Source_Update_Type", "_commit_timestamp")
    )
    # Change feed for addresselement
    df_emailaddress = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_emailaddress")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("EMAILADDRESSID").alias("PRIMARYEMAILADDRESSID"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )

    # Union all CDC sources
    df_union =( df_concern_role.union(df_emailaddress)
               .select(
               col("PRIMARYEMAILADDRESSID").alias("PRIM_EMAIL_ADDRESS_ID"),
               "Source_Update_Type",
               col("_commit_timestamp").alias("op_ts"),
        )
    )

    # Join back with view to get full row
    df_update = (
        df_union.join(
            df_view,
            df_view.View_PRIM_EMAIL_ADDRESS_ID == df_union.PRIM_EMAIL_ADDRESS_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("CONCERN_ROLE_EMAIL")
def create_concern_role_email_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    
    
    df_view = df_view.withColumnRenamed(
        "EMAIL_ADDRESS_ID", "View_EMAIL_ADDRESS_ID"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_EMAIL_ADDRESS_ID"]]
    columns.append("EMAIL_ADDRESS_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_concernroleemailaddress_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_concernroleemailaddress")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            col("EMAILADDRESSID"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )
    df_concernroleemailaddress = (
        df_concernroleemailaddress_all.filter("_change_type in ('update_postimage', 'insert')")
        .select("EMAILADDRESSID", "Source_Update_Type", "_commit_timestamp")
    )
    # Change feed for addresselement
    df_emailaddress = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_emailaddress")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("EMAILADDRESSID"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )

    # Union all CDC sources
    df_union =( df_concernroleemailaddress.union(df_emailaddress)
               .select(
               col("EMAILADDRESSID").alias("EMAIL_ADDRESS_ID"),
               "Source_Update_Type",
               col("_commit_timestamp").alias("op_ts"),
        )
    )

    # Join back with view to get full row
    df_update = (
        df_union.join(
            df_view,
            df_view.View_EMAIL_ADDRESS_ID == df_union.EMAIL_ADDRESS_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("CONCERN_ROLE_PHONE")
def create_concern_role_phone_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)

    df_view = df_view.withColumnRenamed(
        "PHONE_NUMBER_ID", "View_PHONE_NUMBER_ID"
    )
    # build select list (keep all view columns + CDC metadata)
    columns = [c for c in df_view.columns if c not in ["View_PHONE_NUMBER_ID"]]
    columns.append("PHONE_NUMBER_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # CDC from the administrationconcernrole table (direct mapping)
    df_concernrolephonenumbe_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_concernrolephonenumber")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            col("PHONENUMBERID").alias("PHONE_NUMBER_ID"),
            "Source_Update_Type",
            col("_commit_timestamp").alias("op_ts"),
        )
    )

    df_concernrolephonenumbe = (
        df_concernrolephonenumbe_all.filter("_change_type in ('update_postimage', 'insert')")
        .select("PHONE_NUMBER_ID", "Source_Update_Type", "op_ts")
    )
    df_phonenumber = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_phonenumber")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("PHONENUMBERID").alias("PHONE_NUMBER_ID"),
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )

    # union mapped CDC streams (direct ACR changes + role/concernrole mapped changes)
    df_union = df_concernrolephonenumbe.union(df_phonenumber)

    # join back to the view to get full row shape
    df_update = (
        df_union.join(
            df_view,
            df_view.View_PHONE_NUMBER_ID == df_union.PHONE_NUMBER_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn("Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York"))
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("CONTACT_LOG_CONCERNING")
def create_contact_log_concerning_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)

    df_view = df_view.withColumnRenamed("CONTACTLOGCONCERNID", "View_CONTACTLOGCONCERNID")

    columns = [c for c in df_view.columns if c not in ["View_CONTACTLOGCONCERNID"]]
    columns.append("CONTACTLOGCONCERNID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # --- CDF Readers ---

    # ContactLogConcern (anchor)
    df_clc_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_contactlogconcern")
        .filter("_change_type in ('insert','update_postimage')")
        .select(col("CONTACTLOGCONCERNID"), "CONCERNROLEID", "RECORDSTATUS", "Source_Update_Type", "_commit_timestamp")
    )

    # ConcernRole
    df_cr_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_concernrole")
        .filter("_change_type in ('insert','update_postimage')")
        .select("CONCERNROLEID", "CONCERNROLETYPE", "Source_Update_Type", "_commit_timestamp")
    )

    df_cr_change = (
        df_clc_cdf.join(df_cr_cdf, "CONCERNROLEID", "inner")
        .select(df_clc_cdf["CONTACTLOGCONCERNID"], df_cr_cdf["Source_Update_Type"], df_cr_cdf["_commit_timestamp"])
    )

    # --- Union changes ---
    df_union = (
        df_clc_cdf
        .select("CONTACTLOGCONCERNID", "Source_Update_Type", "_commit_timestamp")
        .union(df_cr_change)
    ).select(
        "CONTACTLOGCONCERNID",
        "Source_Update_Type",
        col("_commit_timestamp").alias("op_ts")
    )

    # Join back to base view
    df_update = (
        df_union.join(df_view, df_union.CONTACTLOGCONCERNID == df_view.View_CONTACTLOGCONCERNID, "left")
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn("Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York"))
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("ENQUIRY")
def create_enquiry_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    
    
    df_view = df_view.withColumnRenamed(
        "ENQUIRY_ID", "View_ENQUIRY_ID"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_ENQUIRY_ID"]]
    columns.append("ENQUIRY_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_providerenquiry_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_providerenquiry")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            col("PROVIDERENQUIRYID"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )
    df_providerenquiry = (
        df_providerenquiry_all.filter("_change_type in ('update_postimage', 'insert')")
        .select("PROVIDERENQUIRYID", "Source_Update_Type", "_commit_timestamp")
    )
    # Change feed for addresselement
    df_mcysproviderenquiryext = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_mcysproviderenquiryext")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("PROVIDERENQUIRYID"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )

    # Union all CDC sources
    df_union =( df_providerenquiry.union(df_mcysproviderenquiryext)
               .select(
               col("PROVIDERENQUIRYID").alias("ENQUIRY_ID"),
               "Source_Update_Type",
               col("_commit_timestamp").alias("op_ts"),
        )
    )

    # Join back with view to get full row
    df_update = (
        df_union.join(
            df_view,
            df_view.View_ENQUIRY_ID == df_union.ENQUIRY_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("IDBD")
def create_idbd_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    
    
    df_view = df_view.withColumnRenamed(
        "CONCERNROLEID", "View_CONCERNROLEID"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_CONCERNROLEID"]]
    columns.append("CONCERNROLEID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_mcysidbd_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_mcysidbd")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            col("CONCERNROLEID"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )
    df_mcysidbd = (
        df_mcysidbd_all.filter("_change_type in ('update_postimage', 'insert')")
        .select(col("CONCERNROLEID").alias("CONCERNROLEID"), "Source_Update_Type", col("_commit_timestamp").alias("op_ts"))
    )


    # Join back with view to get full row
    df_update = (
        df_mcysidbd.join(
            df_view,
            df_view.View_CONCERNROLEID == df_mcysidbd.CONCERNROLEID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("ORG_OBJECT_LINK")
def create_org_object_link_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    
    
    df_view = df_view.withColumnRenamed(
        "ORG_OBJECT_LINK_ID", "View_ORG_OBJECT_LINK_ID"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_ORG_OBJECT_LINK_ID"]]
    columns.append("ORG_OBJECT_LINK_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_orgobjectlink_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_orgobjectlink")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            col("ORGOBJECTLINKID"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )
    df_orgobjectlink = (
        df_orgobjectlink_all.filter("_change_type in ('update_postimage', 'insert')")
        .select(col("ORGOBJECTLINKID").alias("ORG_OBJECT_LINK_ID"), "Source_Update_Type", col("_commit_timestamp").alias("op_ts"))
    )


    # Join back with view to get full row
    df_update = (
        df_orgobjectlink.join(
            df_view,
            df_view.View_ORG_OBJECT_LINK_ID == df_orgobjectlink.ORG_OBJECT_LINK_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("PERSON")
def create_person_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)

    df_view = df_view.withColumnRenamed(
        "CONCERNROLEID", "View_CONCERNROLEID"
    )
    
    columns = [
        col for col in df_view.columns if col not in ["View_CONCERNROLEID"]
    ]
    columns.append("CONCERNROLEID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    df_person_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_person")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "CONCERNROLEID",
            "Source_Update_Type",
            "_change_type",
            "_commit_timestamp",
        )
    )

    df_person = (
        df_person_all.filter("_change_type in ('update_postimage', 'insert')")
        .select("CONCERNROLEID", "Source_Update_Type", "_commit_timestamp")
    )

    df_mcyspersonext = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_mcyspersonext")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select("CONCERNROLEID", "Source_Update_Type", "_commit_timestamp")
    )

    df_PROSPECTPERSON = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_PROSPECTPERSON")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("CONCERNROLEID"),
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )



    df_MCYSProspectPersonExt = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_MCYSProspectPersonExt")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select("CONCERNROLEID", "Source_Update_Type", "_commit_timestamp")
    )

    df_union_subquery = (
        df_person.union(df_mcyspersonext)
        .union(df_PROSPECTPERSON).union(df_MCYSProspectPersonExt)
        .select(
            col("CONCERNROLEID"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )

    df_concernrole = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_concernrole")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select(
            col("CONCERNROLEID"),
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )
    df_union = (
        df_union_subquery.union(df_concernrole)
        .select(
            col("CONCERNROLEID"),
            "Source_Update_Type",
            col("_commit_timestamp").alias("op_ts"),
        )
    )
    df_update = (
        df_union.join(
            df_view,
            df_view.View_CONCERNROLEID == df_union.CONCERNROLEID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("PROVIDER_OFFERING")
def create_provider_offering_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    
    df_view = df_view.withColumnRenamed(
        "PROVIDER_OFFERING_ID", "View_PROVIDER_OFFERING_ID"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_PROVIDER_OFFERING_ID"]]
    columns.append("PROVIDER_OFFERING_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_provideroffering_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_provideroffering")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            col("providerofferingID"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )
    df_provideroffering = (
        df_provideroffering_all.filter("_change_type in ('update_postimage', 'insert')")
        .select(col("providerofferingID").alias("PROVIDER_OFFERING_ID"), "Source_Update_Type", col("_commit_timestamp").alias("op_ts"))
    )


    # Join back with view to get full row
    df_update = (
        df_provideroffering.join(
            df_view,
            df_view.View_PROVIDER_OFFERING_ID == df_provideroffering.PROVIDER_OFFERING_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("SERVICE_OFFERING")
def create_service_offering_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)

    df_view = df_view.withColumnRenamed(
        "SERVICE_OFFERING_ID", "View_SERVICE_OFFERING_ID"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_SERVICE_OFFERING_ID"]]
    columns.append("SERVICE_OFFERING_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_serviceoffering_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_serviceoffering")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            col("serviceofferingID"),
            "Source_Update_Type",
            col("_commit_timestamp"),
        )
    )
    df_serviceoffering = (
        df_serviceoffering_all.filter("_change_type in ('update_postimage', 'insert')")
        .select(col("serviceofferingID").alias("SERVICE_OFFERING_ID"), "Source_Update_Type", col("_commit_timestamp").alias("op_ts"))
    )


    # Join back with view to get full row
    df_update = (
        df_serviceoffering.join(
            df_view,
            df_view.View_SERVICE_OFFERING_ID == df_serviceoffering.SERVICE_OFFERING_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("USERS")
def create_users_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)

    df_view = df_view.withColumnRenamed(
        "USER_NAME", "View_USER_NAME"
    )
    
    columns = [
        col for col in df_view.columns if col not in ["View_USER_NAME"]
    ]
    columns.append("USER_NAME")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    df_users = (
        dlt.read(f"{bronze_database}.cpin_curam_users")
        .select("USERNAME", "BUSINESSPHONEID", "MOBILEPHONEID", "BUSINESSEMAILID", "LOCATIONID","UPPERUSERNAME")
        .distinct()
    )

    df_users_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_users")
        .filter("_change_type in ('insert','update_postimage')")
        .select("USERNAME", "Source_Update_Type", "_commit_timestamp")
        .withColumnRenamed("USERNAME", "USER_NAME")
    )

    # Phone numbers
    df_phone_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_phonenumber")
        .filter("_change_type in ('insert','update_postimage')")
        .select("PHONENUMBERID", "Source_Update_Type", "_commit_timestamp")
    )
    df_phone_change = (
        df_users.join(df_phone_cdf, df_users.BUSINESSPHONEID == df_phone_cdf.PHONENUMBERID, "inner")
        .select(df_users["USERNAME"].alias("USER_NAME"),
                df_phone_cdf["Source_Update_Type"],
                df_phone_cdf["_commit_timestamp"])
    )

    # Email
    df_email_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_emailaddress")
        .filter("_change_type in ('insert','update_postimage')")
        .select("EMAILADDRESSID", "Source_Update_Type", "_commit_timestamp")
    )
    df_email_change = (
        df_users.join(df_email_cdf, df_users.BUSINESSEMAILID == df_email_cdf.EMAILADDRESSID, "inner")
        .select(df_users["USERNAME"].alias("USER_NAME"),
                df_email_cdf["Source_Update_Type"],
                df_email_cdf["_commit_timestamp"])
    )

    # Location
    df_location_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_location")
        .filter("_change_type in ('insert','update_postimage')")
        .select("LOCATIONID", "Source_Update_Type", "_commit_timestamp")
    )
    df_location_change = (
        df_users.join(df_location_cdf, df_users.LOCATIONID == df_location_cdf.LOCATIONID, "inner")
        .select(df_users["USERNAME"].alias("USER_NAME"),
                df_location_cdf["Source_Update_Type"],
                df_location_cdf["_commit_timestamp"])
    )

    # Org object link (joined on username directly)
    df_ool_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_orgobjectlink")
        .filter("_change_type in ('insert','update_postimage')")
        .select("USERNAME", "Source_Update_Type", "_commit_timestamp")
        .withColumnRenamed("USERNAME", "USER_NAME")
    )

    df_ool_change = (
    df_users.join(
        df_ool_cdf,
        df_users["UPPERUSERNAME"] == upper(df_ool_cdf["USER_NAME"]),
        "inner",
    )
    .select(
        df_users["USERNAME"].alias("USER_NAME"),
        df_ool_cdf["Source_Update_Type"],
        df_ool_cdf["_commit_timestamp"]
    )
)
    # Union all changes
    df_union = (
        df_users_cdf
        .union(df_phone_change)
        .union(df_email_change)
        .union(df_location_change)
        .union(df_ool_change)
    ).select(
            "USER_NAME",
            "Source_Update_Type",
            col("_commit_timestamp").alias("op_ts"))



    df_update = (
        df_union.join(df_view, df_union.USER_NAME == df_view.View_USER_NAME, "left")
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("ORG_STRUCTURE_FLAT")
def create_org_structure_flat_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)

    df_orgunitparentlink = (
        dlt.read(f"{bronze_database}.cpin_curam_orgunitparentlink")
        .select("ORGUNITPARENTLINKID", "ORGANISATIONUNITID", "ORGANISATIONSTRUCTUREID")
        .distinct()
    )

    df_view = df_view.withColumnRenamed(
        "ORG_UNIT_PARENT_LINK_ID", "View_ORG_UNIT_PARENT_LINK_ID"
    )
    columns = [
        col for col in df_view.columns if col not in ["View_ORG_UNIT_PARENT_LINK_ID"]
    ]
    columns.append("ORG_UNIT_PARENT_LINK_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    df_orgunitparentlinkchanges_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_orgunitparentlink")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "ORGUNITPARENTLINKID", 
            "Source_Update_Type", 
            "_change_type", 
            "_commit_timestamp",
        )
    )

    df_orgunitparentlinkchanges = df_orgunitparentlinkchanges_all.filter(
        "_change_type in ('update_postimage', 'insert')"
    ).select("ORGUNITPARENTLINKID", "Source_Update_Type", "_commit_timestamp")

    df_organisationunitchanges = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_organisationunit")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select("ORGANISATIONUNITID", "Source_Update_Type", "_commit_timestamp")
    )

    df_organisationunitchange = (
        df_organisationunitchanges.alias("ou")
        .join(
            df_orgunitparentlink.alias("oupl"),
            df_orgunitparentlink.ORGANISATIONUNITID
            == df_organisationunitchanges.ORGANISATIONUNITID,
            how="inner",
        )
        .select("ORGUNITPARENTLINKID", "Source_Update_Type", "_commit_timestamp")
    )

    df_orgunitpositionlinkchanges = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_orgunitpositionlink")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select("ORGANISATIONUNITID", "Source_Update_Type", "_commit_timestamp")
    )

    df_orgunitpositionlinkchange = (
        df_orgunitpositionlinkchanges.alias("opl")
        .join(
            df_orgunitparentlink.alias("oupl"),
            df_orgunitparentlink.ORGANISATIONUNITID
            == df_orgunitpositionlinkchanges.ORGANISATIONUNITID,
            how="inner",
        )
        .select("ORGUNITPARENTLINKID", "Source_Update_Type", "_commit_timestamp")
    )

    df_organisationstructurechanges = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_organisationstructure")
        .filter("_change_type in ('update_postimage', 'insert')")
        .select("ORGANISATIONSTRUCTUREID", "Source_Update_Type", "_commit_timestamp")
    )

    df_organisationstructurechange = (
        df_organisationstructurechanges.alias("os")
        .join(
            df_orgunitparentlink.alias("oupl"),
            df_orgunitparentlink.ORGANISATIONSTRUCTUREID
            == df_organisationstructurechanges.ORGANISATIONSTRUCTUREID,
            how="inner",
        )
        .select("ORGUNITPARENTLINKID", "Source_Update_Type", "_commit_timestamp")
    )

    df_union = (
        df_orgunitparentlinkchanges.union(df_organisationunitchange)
        .union(df_orgunitpositionlinkchange)
        .union(df_organisationstructurechange)
        .select(
            col("ORGUNITPARENTLINKID").alias("ORG_UNIT_PARENT_LINK_ID"),
            "Source_Update_Type", 
            col("_commit_timestamp").alias("op_ts"),
        )
    )

    df_update = (
        df_union.join(
            df_view,
            df_view.View_ORG_UNIT_PARENT_LINK_ID == df_union.ORG_UNIT_PARENT_LINK_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT",
            from_utc_timestamp(current_timestamp(), "America/New_York"),
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("PROVIDER_PARTY")
def create_provider_party_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed("PROVIDER_PARTY_KEY", "View_PROVIDER_PARTY_KEY")

    columns = [c for c in df_view.columns if c not in ["View_PROVIDER_PARTY_KEY"]]
    columns.append("PROVIDER_PARTY_KEY")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # --- CDF Readers ---

    # ProviderParty (anchor)
    df_pp_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_providerparty")
        .filter("_change_type in ('insert','update_postimage')")
        .select(
            col("PROVIDERPARTYID").cast("bigint").alias("PROVIDER_PARTY_KEY"),
            "PARTYCONCERNROLEID",
            "CATEGORY",
            "POSITION",
            "RECORDSTATUS",
            "TYPE",
            "Source_Update_Type",
            "_commit_timestamp"
        )
    )

    # McysProviderPartyExt
    df_mpp_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_mcysproviderpartyext")
        .filter("_change_type in ('insert','update_postimage')")
        .select(
            col("PROVIDERPARTYID").cast("bigint").alias("PROVIDER_PARTY_KEY"),
            "DATEOFBIRTH",
            "Source_Update_Type",
            "_commit_timestamp"
        )
    )

    # ConcernRole
    df_cr_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_concernrole")
        .filter("_change_type in ('insert','update_postimage')")
        .select(
            col("CONCERNROLEID").alias("REFERENCE_NUMBER"),
            "CONCERNROLENAME",
            "Source_Update_Type",
            "_commit_timestamp"
        )
    )

    # --- Join changes ---

    # From ProviderParty → ConcernRole
    df_cr_change = (
        df_pp_cdf.join(df_cr_cdf, df_pp_cdf.PARTYCONCERNROLEID == df_cr_cdf.REFERENCE_NUMBER, "inner")
        .select(df_pp_cdf["PROVIDER_PARTY_KEY"], df_cr_cdf["Source_Update_Type"], df_cr_cdf["_commit_timestamp"])
    )

    # From ProviderParty → McysProviderPartyExt
    df_mpp_change = (
    df_pp_cdf.join(df_mpp_cdf, df_pp_cdf.PROVIDER_PARTY_KEY == df_mpp_cdf.PROVIDER_PARTY_KEY, "inner")
    .select(df_pp_cdf["PROVIDER_PARTY_KEY"], df_mpp_cdf["Source_Update_Type"], df_mpp_cdf["_commit_timestamp"])
    )


    # --- Union all changes ---
    df_union = (
        df_pp_cdf
        .select("PROVIDER_PARTY_KEY", "Source_Update_Type", "_commit_timestamp")
        .union(df_cr_change)
        .union(df_mpp_change)
    ).select(
        "PROVIDER_PARTY_KEY",
        "Source_Update_Type",
        col("_commit_timestamp").alias("op_ts")
    )

    # --- Join back to base view ---
    df_update = (
        df_union.join(df_view, df_union.PROVIDER_PARTY_KEY == df_view.View_PROVIDER_PARTY_KEY, "left")
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn("Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York"))
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("LOCATION")
def create_location_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed(
        "ID", "View_ID"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_ID"]]
    columns.append("ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_location_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_cams_location")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "ID",
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )
    df_location = (
        df_location_all.filter("_change_type in ('update_postimage', 'insert')")
        .select(col("ID"), "Source_Update_Type", col("_commit_timestamp").alias("op_ts"))
    )


    # Join back with view to get full row
    df_update = (
        df_location.join(
            df_view,
            df_view.View_ID == df_location.ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("CODE_TABLE_ITEM")
def create_code_table_item_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed("ITEM_CODE", "View_ITEM_CODE")

    columns = [col for col in df_view.columns if col not in ["View_ITEM_CODE"]]
    columns.append("ITEM_CODE")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    df_cti_en_cdf = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_codetableitem")
        .filter("_change_type in ('insert','update_postimage')")
        .select("TABLENAME", col("CODE").alias("ITEM_CODE"), "Source_Update_Type", col("_commit_timestamp").alias("op_ts"))
    )

    # Join back to base view
    df_update = (
        df_cti_en_cdf.join(
            df_view,
            (df_cti_en_cdf.TABLENAME == df_view.TABLE_NAME) &
            (df_cti_en_cdf.ITEM_CODE == df_view.View_ITEM_CODE), 
            how="left"
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn("Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York"))
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("POSITION_HOLDER_LINK")
def create_position_holder_link_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed(
        "USER_NAME", "View_USER_NAME"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_USER_NAME"]]
    columns.append("USER_NAME")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_users_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_users")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "USERNAME",
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )
    df_users = (
        df_users_all.filter("_change_type in ('update_postimage', 'insert')")
        .select(col("USERNAME").alias("USER_NAME"), "Source_Update_Type", col("_commit_timestamp").alias("op_ts"))
    )


    # Join back with view to get full row
    df_update = (
        df_users.join(
            df_view,
            df_view.View_USER_NAME == df_users.USER_NAME,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
        .drop("USER_NAME")
    )

    return df_update

@ChangeFunctionRegistry.register("CONCERN_ROLE_ADDRESS")
def create_concern_role_address_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed(
        "CONCERN_ROLE_ADDRESS_ID", "View_CONCERN_ROLE_ADDRESS_ID"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_CONCERN_ROLE_ADDRESS_ID"]]
    columns.append("CONCERN_ROLE_ADDRESS_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_concernroleaddress_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_concernroleaddress")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "CONCERNROLEADDRESSID",
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )
    df_concernroleaddress = (
        df_concernroleaddress_all.filter("_change_type in ('update_postimage', 'insert')")
        .select(col("CONCERNROLEADDRESSID").alias("CONCERN_ROLE_ADDRESS_ID"), "Source_Update_Type", col("_commit_timestamp").alias("op_ts"))
    )


    # Join back with view to get full row
    df_update = (
        df_concernroleaddress.join(
            df_view,
            df_view.View_CONCERN_ROLE_ADDRESS_ID == df_concernroleaddress.CONCERN_ROLE_ADDRESS_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update

@ChangeFunctionRegistry.register("ORG_UNIT")
def create_org_unit_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed(
        "ORGANISATION_UNIT_ID", "View_ORGANISATION_UNIT_ID"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_ORGANISATION_UNIT_ID"]]
    columns.append("ORGANISATION_UNIT_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_orgunit_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_organisationunit")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "ORGANISATIONUNITID",
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )
    df_orgunit = (
        df_orgunit_all.filter("_change_type in ('update_postimage', 'insert')")
        .select(col("ORGANISATIONUNITID").alias("ORGANISATION_UNIT_ID"), "Source_Update_Type", col("_commit_timestamp").alias("op_ts"))
    )


    # Join back with view to get full row
    df_update = (
        df_orgunit.join(
            df_view,
            df_view.View_ORGANISATION_UNIT_ID == df_orgunit.ORGANISATION_UNIT_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update



@ChangeFunctionRegistry.register("CONCERNROLE_ATTACHMENT_LINK")
def create_concern_role_address_change_view(spark, bronze_database, silver_database, silver_view):
    df_view = dlt.read(silver_view)
    df_view = df_view.withColumnRenamed(
        "ATTACHMENT_LINK_ID", "View_ATTACHMENT_LINK_ID"
    )

    # Keep all columns + change metadata
    columns = [col for col in df_view.columns if col not in ["View_ATTACHMENT_LINK_ID"]]
    columns.append("ATTACHMENT_LINK_ID")
    columns.append("Source_Update_Type")
    columns.append("op_ts")

    # Change feed for address
    df_cdc_all = (
        spark.readStream.option("readChangeFeed", "true")
        .table(f"{bronze_database}.cpin_curam_concernroleattachmentlink")
        .filter("_change_type in ('update_postimage', 'insert', 'delete')")
        .select(
            "ATTACHMENTLINKID",
            "Source_Update_Type",
            "_commit_timestamp",
        )
    )
    df_cdc = (
        df_cdc_all.filter("_change_type in ('update_postimage', 'insert')")
        .select(col("ATTACHMENTLINKID").alias("ATTACHMENT_LINK_ID"), "Source_Update_Type", col("_commit_timestamp").alias("op_ts"))
    )


    # Join back with view to get full row
    df_update = (
        df_cdc.join(
            df_view,
            df_view.View_ATTACHMENT_LINK_ID == df_cdc.ATTACHMENT_LINK_ID,
            how="inner",
        )
        .select(*columns)
        .withColumn("Effective_Start_DT", col("op_ts"))
        .withColumn(
            "Last_Update_DT", from_utc_timestamp(current_timestamp(), "America/New_York")
        )
        .drop("op_ts")
    )

    return df_update
