# Databricks notebook source
# MAGIC %md
# MAGIC Input:
# MAGIC 1. evaluations.report_collectionmetrics
# MAGIC
# MAGIC Output:
# MAGIC 1. evaluations.AiPA_collectionwise_data

# COMMAND ----------

# DBTITLE 1,Library
import elsevier_eagle
from elsevier_eagle import *

# COMMAND ----------

# DBTITLE 1,Read collection name
# query = "SELECT * FROM collections ALLOW FILTERING;"
# df_collections_collections = cassandra_orgdb(query, 'prod')
# df_collections_collections = df_collections_collections.filter(df_collections_collections["owner"]=='AiPA')
# df_collections_collections = df_collections_collections.filter(df_collections_collections["name"].like('%_fromDataBricks%'))
# df_collections_collections = df_collections_collections.select("owner", "name", "count", "createddate", "lastupdateddate", "lock_holder", "lock_owner_history", "matcheriterationns")
# display(df_collections_collections)

df_collections = spark.read.table("evaluations.report_collectionmetrics")
df_collections = df_collections.filter(df_collections["collection_name"].like('%_fromDataBricks%'))
df_collections = df_collections.filter(df_collections["duration_minutes_total"] > 0)
df_collections_list = df_collections.select("collection_name").distinct().collect()
df_collections_list = [row["collection_name"] for row in df_collections_list]
print(df_collections_list)

# COMMAND ----------

# DBTITLE 1,Data from Cassandra, based on filter collection name
spark.sql(f"DROP TABLE IF EXISTS evaluations.AiPA_collectionwise_data") # truncate table if exists


for collection_name in df_collections_list:

    query = f"SELECT * FROM collection_entries WHERE collection = '{collection_name}' ALLOW FILTERING;"
    df_collection_entries = cassandra_orgdb(query, 'prod')
    # df_collection_entries = df_collection_entries.select("collection", "id", "affiliationid", "affil", "affilorgs", "matchedorgdborg", "forcedlinkedaffil", "matchednearestmainorgid", "matchedorgprofileids", "nearestmain", "opm_orgprofileid", "parsedopm_list", "parsedopmm_list", "incompatibleopmm_list")
    df_collection_entries = df_collection_entries.withColumn("Incompatibleopmm", expr("transform(incompatibleopmm_list, x -> x.orgprofileid)"))
    df_collection_entries = df_collection_entries.withColumn("Incompatibleopmm", when(col("Incompatibleopmm").isNull(), array()).otherwise(col("Incompatibleopmm")))
    df_collection_entries = df_collection_entries.withColumn("Parsedopm", expr("transform(parsedopm_list, x -> x.orgprofileid)"))
    df_collection_entries = df_collection_entries.withColumn("Parsedopm", when(col("Parsedopm").isNull(), array()).otherwise(col("Parsedopm")))
    df_collection_entries = df_collection_entries.withColumn("Parsedopmm", expr("transform(parsedopmm_list, x -> x.orgprofileid)"))
    df_collection_entries = df_collection_entries.withColumn("Parsedopmm", when(col("Parsedopmm").isNull(), array()).otherwise(col("Parsedopmm")))

    df_collection_entries = df_collection_entries.withColumn("nearestmain", when(col("nearestmain").isNull(), 0).otherwise(col("nearestmain")))
    df_collection_entries = df_collection_entries.withColumn("opm_orgprofileid", when(col("opm_orgprofileid").isNull(), 0).otherwise(col("opm_orgprofileid")))
    df_collection_entries = df_collection_entries.withColumn("matchedorgprofileids", when(col("matchedorgprofileids").isNull(), array()).otherwise(col("matchedorgprofileids")))

    df_collection_entries = df_collection_entries.withColumn("ExistingIDs", array_distinct(array(col("nearestmain"), col("opm_orgprofileid"))))
    df_collection_entries = df_collection_entries.withColumn("ExistingIDs", array_union(col("ExistingIDs"), col("Parsedopm")))
    df_collection_entries = df_collection_entries.withColumn("ExistingIDs", when(col("ExistingIDs").isNull(), array()).otherwise(col("ExistingIDs")))
    df_collection_entries = df_collection_entries.withColumn("NewIDs", array_except(col("matchedorgprofileids"), col("Incompatibleopmm")))
    df_collection_entries = df_collection_entries.withColumn("NewIDs", array_union(col("NewIDs"), col("Parsedopmm")))
    df_collection_entries = df_collection_entries.withColumn("NewIDs", when(col("NewIDs").isNull(), array()).otherwise(col("NewIDs")))
    df_collection_entries = df_collection_entries.withColumn("ID", regexp_extract(col("collection"), r'_(\d{8})_', 1).cast(LongType()))
    df_collection_entries = df_collection_entries.withColumn("ChangesResult", when((array_contains(col("NewIDs"), col("ID"))), lit("no")).otherwise(lit("yes")))

    df_collection_entries = df_collection_entries.select("collection", "ID", "affiliationid", "ExistingIDs", "NewIDs", "Parsedopmm", "Incompatibleopmm", "ChangesResult")
    print(f"{collection_name} : {df_collection_entries.count()}")

    schema_yes = StructType([
        StructField("collection", StringType(), True),
        StructField("yes_affiliationid", ArrayType(LongType()), True),
        StructField("yes_count", LongType(), True)
    ])

    schema_no = StructType([
        StructField("collection", StringType(), True),
        StructField("no_affiliationid", ArrayType(LongType()), True),
        StructField("no_count", LongType(), True)
    ])


    if df_collection_entries.rdd.isEmpty():
        pass
    else:
        df_collection_entries_yes = df_collection_entries.filter(df_collection_entries["ChangesResult"] == "yes")        
        df_collection_entries_no = df_collection_entries.filter(df_collection_entries["ChangesResult"] == "no")
    
    if df_collection_entries_yes.rdd.isEmpty():
        df_collection_entries_yes_value = [Row(collection_name, [], 0)]
        df_collection_entries_yes_value = spark.createDataFrame(df_collection_entries_yes_value, schema=schema_yes)
    else:
        df_collection_entries_yes_value = df_collection_entries_yes.groupby("collection").agg(collect_set("affiliationid").alias("yes_affiliationid"), countDistinct("affiliationid").alias("yes_count"))

    if df_collection_entries_no.rdd.isEmpty():
        df_collection_entries_no_value = [Row(collection_name, [], 0)]
        df_collection_entries_no_value = spark.createDataFrame(df_collection_entries_no_value, schema=schema_no)
    else:
        df_collection_entries_no_value = df_collection_entries_no.groupby("collection").agg(collect_set("affiliationid").alias("no_affiliationid"), countDistinct("affiliationid").alias("no_count"))

    #join
    df_collection_entries_master = df_collection_entries_yes_value.join(df_collection_entries_no_value, on="collection", how="left")
    df_collection_entries_master = df_collection_entries_master.withColumn("affiliation_count", col("yes_count") + col("no_count"))
    df_collection_entries_master = df_collection_entries_master.select("collection", col("yes_affiliationid").alias("HasPrecisionIssue_affiliationid"), col("yes_count").alias("HasPrecisionIssue_count"), col("no_affiliationid").alias("NoPrecisionIssue_affiliationid"), col("no_count").alias("NoPrecisionIssue_count"), "affiliation_count")
    df_collection_entries_master.write.mode("append").saveAsTable("evaluations.AiPA_collectionwise_data")

# COMMAND ----------

df = spark.read.table("evaluations.AiPA_collectionwise_data")
display(df)

# COMMAND ----------

