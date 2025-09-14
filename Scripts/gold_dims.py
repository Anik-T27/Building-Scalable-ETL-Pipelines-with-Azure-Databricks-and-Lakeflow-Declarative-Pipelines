# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM workspace.silver.silver_flights

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Parameters

# COMMAND ----------

key_cols="['passenger_id']"
key_cols_list=eval(key_cols)

catalog="workspace"

cdc_col="modified_date"

backdated_refresh=""

source_object="silver_passengers"
source_schema="silver"

target_schema="gold"
target_object="DimPassengers"

surrogate_key="DimPassengersKey"

# COMMAND ----------

# MAGIC %md
# MAGIC ### INCREMENTAL DATA INGESTION

# COMMAND ----------

# Creating last_load_date

if len(backdated_refresh)==0:
    if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
        last_load_date=spark.sql(f"SELECT max({cdc_col}) FROM workspace.{target_schema}.{target_object}").collect()[0][0]

    else:
        last_load_date="1900-01-01 00:00:00"
else:
    last_load_date=backdated_refresh



# COMMAND ----------

last_load_date

# COMMAND ----------

df_source= spark.sql(f" SELECT * FROM {source_schema}.{source_object} WHERE {cdc_col}>='{last_load_date}'")

# COMMAND ----------

# MAGIC %md
# MAGIC #OLD VS NEW RECORD

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Target Dataframe

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    key_cols_list_incremental= ",".join(key_cols_list)
    df_target=spark.sql(f"SELECT {key_cols_list_incremental} ,{surrogate_key},create_date,update_date FROM {catalog}.{target_schema}.{target_object}")
    
else:
    key_cols_string_initial= [f" '' AS {i}" for i in key_cols_list]
    key_cols_string_initial=",".join(key_cols_string_initial)
    df_target= spark.sql(f"SELECT {key_cols_string_initial},CAST('0' AS INT) AS {surrogate_key}, CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS create_date, CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS update_date where 1=0")

# COMMAND ----------

df_target.display()

# COMMAND ----------

df_source.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Joining Source and target DF

# COMMAND ----------

join_condition='AND'.join([f"src.{i}=trg.{i}" for i in key_cols_list])

# COMMAND ----------


df_source.createOrReplaceTempView("src")
df_target.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
                        SELECT src.*,
                        trg.{surrogate_key},
                        trg.create_date,
                        trg.update_date
                        FROM src
                        LEFT JOIN trg
                        ON {join_condition}
                        """)



# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering Old and New Record
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import*

# COMMAND ----------

df_old=df_join.filter(col(f'{surrogate_key}').isNotNull())
df_new=df_join.filter(col(f'{surrogate_key}').isNull())
df_old.display()
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enriching Old and New DF

# COMMAND ----------

df_old_enriched=df_old.withColumn('update_date',current_timestamp())


# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    max_surrogate_key=spark.sql(f"SELECT MAX({surrogate_key}) FROM {catalog}.{target_schema}.{target_object}").collect()[0][0]
    df_new_enriched= df_new.withColumn(f"{surrogate_key}",lit(max_surrogate_key)+ lit(1)+ monotonically_increasing_id())\
        .withColumn('create_date',current_timestamp())\
        .withColumn('update_date',current_timestamp())
else:
    max_surrogate_key=0
    df_new_enriched= df_new.withColumn(f"{surrogate_key}",lit(max_surrogate_key)+ lit(1)+ monotonically_increasing_id())\
        .withColumn('create_date',current_timestamp())\
            .withColumn('update_date',current_timestamp())
            

# COMMAND ----------

df_union=df_old_enriched.unionByName(df_new_enriched)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Performing UPSERT- To handle SCD

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj=DeltaTable.forName(spark, f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_union.alias("src"),f"trg.{surrogate_key}=src.{surrogate_key}")\
        .whenMatchedUpdateAll(condition=f"src.{cdc_col}>=trg.{cdc_col}")\
        .whenNotMatchedInsertAll().execute()
                    
else:
    df_union.write.format("delta")\
        .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")
    

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.gold.dimpassengers
# MAGIC WHERE passenger_id="P0049"

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATING FACT TABLE

# COMMAND ----------

catalog='workspace'
source_schema='silver'
source_object='silver_bookings'
cdc_col='modified_date'
backdated_refresh=""
fact_table=f"{catalog}.{source_schema}.{source_object}"
target_schema='gold'
target_object="fact_bookings"
fact_key_cols=["DimPassengersKey","DimFlightsKey","DimAirportsKey","booking_date"]

# COMMAND ----------

dimensions=[
    {
        "table":f"{catalog}.{target_schema}.DimPassengers",
        "alias": "DimPassengers",
        "join_keys": [("passenger_id","passenger_id")]
    },
    {
        "table":f"{catalog}.{target_schema}.DimFlights",
        "alias": "DimFlights",
        "join_keys": [("flight_id","flight_id")]
    },
    {
        "table":f"{catalog}.{target_schema}.DimAirports",
        "alias": "DimAirports",
        "join_keys": [("airport_id","airport_id")]

    }
]

# COMMAND ----------

fact_columns=["amount","booking_date","modified_date"]

# COMMAND ----------

if len(backdated_refresh)==0:
    if spark.catalog.tableExists(f"workspace.{target_schema}.{target_object}"):
        last_load_date=spark.sql(f"SELECT max({cdc_col}) FROM workspace.{target_schema}.{target_object}").collect()[0][0]

    else:
        last_load_date="1900-01-01 00:00:00"
else:
    last_load_date=backdated_refresh

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Dynamic Fact Query

# COMMAND ----------

def generate_fact_query_incremental(fact_table,dimensions,fact_columns,cdc_col,processing_date):
    fact_alias="f"

    # Base Columns to Select
    select_cols=[f"{fact_alias}.{col}" for col in fact_columns]

    # Building Joins Dynamically
    join_clauses=[]
    for dim in dimensions:
        table_full=dim["table"]
        alias=dim["alias"]
        table_name=table_full.split(".")[-1]
        surrogate_key=f"{alias}.{table_name}key"
        select_cols.append(surrogate_key)

    # Build ON clauses
        on_conditions=[
        f"{fact_alias}.{fk}={alias}.{dk}" for fk,dk in dim["join_keys"]
        ]
        join_clause=f"LEFT JOIN {table_full} AS {alias} ON "+" AND "\
        .join(on_conditions)

        join_clauses.append(join_clause)

    # Final Select and Join Clauses
    select_clause=",\n     ".join(select_cols)
    joins="\n".join(join_clauses)

    # WHERE clause for incremental filtering
    where_clause=f"{fact_alias}.{cdc_col}>=DATE('{last_load_date}')"

    # Final query
    query=f"""
    SELECT
        {select_clause}
    FROM {fact_table} AS {fact_alias}
    {joins}
    WHERE
        {where_clause}
    """.strip()
    return query



    

# COMMAND ----------

query= generate_fact_query_incremental(fact_table,dimensions,fact_columns,cdc_col,last_load_date)

# COMMAND ----------

print(query)

# COMMAND ----------

df_fact= spark.sql(query)

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UPSERT- TO handle Slowly Changing Dimensions(SCD)

# COMMAND ----------

fact_key_cols

fact_key_cols_str=" AND ".join([f"src.{col}=trg.{col}" for col in fact_key_cols])

fact_key_cols_str


# COMMAND ----------

if spark.catalog.tableExists(f"{catalog}.{target_schema}.{target_object}"):
    dlt_obj=DeltaTable.forName(spark,f"{catalog}.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_fact.alias("src"), fact_key_cols_str)\
        .whenMatchedUpdateAll(condition =f"src.{cdc_col}>=trg.{cdc_col}")\
            .whenNotMatchedInsertAll()\
                .execute()
else:
    df_fact.write.format("delta")\
        .mode("append")\
            .saveAsTable(f"{catalog}.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.gold.fact_bookings