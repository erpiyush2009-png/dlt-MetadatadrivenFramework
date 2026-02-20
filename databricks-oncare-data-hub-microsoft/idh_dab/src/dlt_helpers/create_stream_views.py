# Databricks notebook source
# MAGIC %md
# MAGIC This notebook prepare matadata tables 

# COMMAND ----------

dbutils.widgets.text('env',defaultValue='ist')

dbutils.widgets.text('Data_Catalog',defaultValue='oncare_integrated_data_hub_ist')

dbutils.widgets.text('Table_Schema',defaultValue='bronze_cpin')

dbutils.widgets.text('Table_Layer',defaultValue='bronze')


# COMMAND ----------

srcDB =dbutils.widgets.get("Data_Catalog")

srcSchema = dbutils.widgets.get("Table_Schema")

env =dbutils.widgets.get("env")

layer = dbutils.widgets.get("Table_Layer").lower()

# COMMAND ----------

def create_target_table_view(db, schema, table: str) -> str:
    query = f"Create or Replace view {db}.{schema}.v_{table} as select * from {db}.{schema}.{table} where __End_At is null "
    return query

# COMMAND ----------

print(f"create all views in bronze schema {srcSchema} ....")

query = f"select table_name as tableName from system.information_schema.tables where table_catalog = '{srcDB}' and table_schema = '{srcSchema}' and table_name ilike 'cpin_%'";

# query =  f"SHOW Tables in {srcDB}.{orgSchema}"
tables_df = spark.sql(query)
tables = tables_df.collect()
mappings = [(f"{row['tableName']}") for row in tables]
# Generate copy statements for each table

create_statements = [create_target_table_view(srcDB, srcSchema, dest) for (dest) in mappings]
    
# Execute each copy statement
for statement in create_statements:
    try:
        print(f"---------- [Start] - clone -------")
        print(statement)
        spark.sql(statement)
        print(f"---------- [End] - clone -------")
    except Exception as e:
        print(f"!!!!! [Error] - view not created: {statement} !!!!")
        print(e)
        break

# COMMAND ----------

if layer == 'bronze':
	query = f"""
	Create or replace view {srcDB}.{srcSchema}.v_cpin_curam_codetableitem_flat as
	Select * From (
		Select  TABLENAME,
				CODE,
				DESCRIPTION,
				LOCALEIDENTIFIER
		From {srcDB}.{srcSchema}.v_cpin_curam_codetableitem
	)
	Pivot (
		Max(Description) DESCRIPTION
		for LOCALEIDENTIFIER in ('en' EN_DESC, 'fr' FR_DESC)
	)
	"""
	spark.sql(query)