# Databricks notebook source
# MAGIC %md
# MAGIC Input:
# MAGIC   - evaluations.ai_list_orgs_with_allorgs_batch === **This is a main list of orgs for using in AI job**
# MAGIC   - evaluations.ai_list_orgs_with_results_async_mode_2 === **This is list of ai data output**
# MAGIC   
# MAGIC Output:
# MAGIC   - evaluations.report_orgspicking
# MAGIC   - evaluations.report_aioutputdata
# MAGIC   - evaluations.report_collectionmetrics
# MAGIC   - evaluations.AiPA_collectionwise_data
# MAGIC
# MAGIC Support:
# MAGIC   - https://elsevier-dev.cloud.databricks.com/editor/notebooks/4383827528071839?o=8907390598234411#command/8585318122823221
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Library
import sys
sys.path.append('/Workspace/OrganizationDomain/elsevier_eagle')
from elsevier_eagle import *

# COMMAND ----------

# DBTITLE 1,Selection of Organizations for AI Evaluation
#data orgs pick for ai.
df_orgs_picking = spark.read.table("evaluations.ai_list_orgs_with_allorgs_batch")
df_orgs_picking = df_orgs_picking.withColumn("collection_name", concat(lit("AiPA+"), col("collection_name"), lit("+ANI")))
df_orgs_picking.write.format("delta").mode("OVERWRITE").option("overwriteSchema", "true").saveAsTable("evaluations.report_orgspicking")

# COMMAND ----------

# DBTITLE 1,Create ai output results data with Organization (PID)
#data after ai result
df_aioutput = spark.read.table("evaluations.ai_list_orgs_with_results_async_mode_2")

#data after ai result in the term of xml
df_aioutput = df_aioutput.withColumn(
        "Result",
        when(
            (col("Raw") == "true") & (col("Aft") == "true") & (col("vendor") == "true"),
            "RN"
        ).when(
            (col("Raw") == "false") & (col("Aft") == "true") & (col("vendor") == "true"),
            "RN"
        ).when(
            (col("Raw") == "false") & (col("Aft") == "false") & (col("vendor") == "true"),
            "RY"
        ).when(
            (col("Raw") == "false") & (col("Aft") == "true") & (col("vendor") == "false"),
            "RY"
        ).when(
            (col("Raw") == "true") & (col("Aft") == "false") & (col("vendor") == "false"),
            "RY"
        ).when(
            (col("Raw") == "false") & (col("Aft") == "false") & (col("vendor") == "false"),
            "RY"
        ).when(
            (col("Raw") == "true") & (col("Aft") == "true") & (col("vendor") == "false"),
            "2outof3"
        ).when(
            (col("Raw") == "true") & (col("Aft") == "false") & (col("vendor") == "true"),
            "2outof3"
        )
    )

df_aioutput_xml = df_aioutput.groupBy("PID", "processed_datetime").pivot("Result").agg(count("AffID")).fillna(0)
df_aioutput_xml = df_aioutput_xml.withColumn("RY", col("2outof3") + col("RY"))
df_aioutput_xml = df_aioutput_xml.withColumn("SampleSize", col("RY") + col("RN"))

#data after ai result in the term of affiliation_ids
df_aioutput_afids = df_aioutput.withColumn("AffID", explode_outer(split(col("AffID"), "\|")))
df_aioutput_afids = df_aioutput_afids.groupBy("PID", "processed_datetime").pivot("Result").agg(count("AffID")).fillna(0)
df_aioutput_afids = df_aioutput_afids.withColumn("RY", col("2outof3") + col("RY"))
df_aioutput_afids = df_aioutput_afids.withColumn("SampleSize", col("RY") + col("RN"))
df_aioutput_afids = df_aioutput_afids.select("PID", "processed_datetime", col("2outof3").alias("2outof3_affil"), col("RN").alias("RN_affil"), col("RY").alias("RY_affil"), col("SampleSize").alias("SampleSize_affil"))

#Self Join
df_aioutput_xml_afids = df_aioutput_xml.join(df_aioutput_afids, on=['PID','processed_datetime'], how="left")


#add affiliation count
df_orgid_list = [row["PID"] for row in df_aioutput_xml_afids.select("PID").distinct().collect()]
df_solr_afcount = afcount_from_solr_for_orgids('prod', 'ani', df_orgid_list).distinct()

df_aioutput_xml_afids_afcount = df_aioutput_xml_afids.join(df_solr_afcount, df_aioutput_xml_afids.PID == df_solr_afcount.id, "left").drop("id")
df_aioutput_xml_afids_afcount = df_aioutput_xml_afids_afcount.fillna(0, subset=['count'])

#Save data into tables
df_aioutput_xml_afids_afcount.write.format("delta").mode("OVERWRITE").option("overwriteSchema", "true").saveAsTable("evaluations.report_aioutputdata")

# COMMAND ----------

# DBTITLE 1,Create collectionmetrics table after cleanup
df_cassandra_orgdb = cassandra_orgdb('SELECT * FROM collectionmetrics ALLOW FILTERING;', env="prod")
# df_cassandra_orgdb = spark.createDataFrame(df_cassandra_orgdb, schema = get_schema_prod_collectionmetrics())

#clusterid_count and affiliation_count_sum
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("count_of_clusterid", expr("aggregate(filter(clusterids, x -> x.cluster_id > 69999999), 0, (acc, x) -> acc + 1)"))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("count_of_orgid", expr("aggregate(filter(clusterids, x -> x.cluster_id < 69999999 AND x.cluster_id > 0), 0, (acc, x) -> acc + 1)"))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("affiliation_sum_of_clusterid", expr("aggregate(filter(clusterids, x -> x.cluster_id > 69999999), 0, (acc, x) -> acc + x.affiliation_count)"))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("affiliation_sum_of_orgid", expr("aggregate(filter(clusterids, x -> x.cluster_id < 69999999 AND x.cluster_id > 0), 0, (acc, x) -> acc + x.affiliation_count)"))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("affiliation_sum_of_0", expr("aggregate(filter(clusterids, x -> x.cluster_id = 0), 0, (acc, x) -> acc + x.affiliation_count)"))
df_cassandra_orgdb = df_cassandra_orgdb.fillna(0, subset=['affiliation_count', 'count_of_clusterid', 'count_of_orgid', 'affiliation_sum_of_clusterid', 'affiliation_sum_of_orgid', 'affiliation_sum_of_0'])
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("start_time", to_timestamp(col("start_time"))).withColumn("start_time", from_utc_timestamp(col("start_time"), "Asia/Kolkata"))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("end_time", to_timestamp(col("end_time"))).withColumn("end_time", from_utc_timestamp(col("end_time"), "Asia/Kolkata"))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("duration_minutes_total", ((col("end_time").cast("long") - col("start_time").cast("long")) / 60).cast("int"))
df_cassandra_orgdb = df_cassandra_orgdb.groupby("collection_name", "username", "role", "project", "orgid", "curation_type", "count_of_clusterid", "affiliation_count", "count_of_orgid", "affiliation_sum_of_clusterid", "affiliation_sum_of_orgid", "affiliation_sum_of_0").agg(sum("duration_minutes_total").alias("duration_minutes_total"), collect_set("start_time").alias("start_time"), collect_set("end_time").alias("end_time"), min("start_time").alias("min_start_time"), max("end_time").alias("max_end_time"))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("duration_formatted", format_string("%02d:%02d min", expr("floor(duration_minutes_total / 60)"), expr("duration_minutes_total % 60")))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("duration_formatted", when(col("duration_formatted") == "null:null min", "").otherwise(col("duration_formatted")))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("Date", regexp_replace(col("collection_name"), "AiPA\+", ""))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("Date", left(col("Date"), lit(10)))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("Date", from_unixtime(col("Date")))
df_cassandra_orgdb = df_cassandra_orgdb.withColumn("PID", when(col("orgid").isNull(), regexp_extract(col("collection_name"), r"6\d{7}", 0)).otherwise(col("orgid")))

df_usersdetails = spark.read.csv(path=f'/mnt/els/evaluation/amarnath/list_usersdetails', sep='\t', quote='', header=True, inferSchema=True).distinct()
df_cassandra_orgdb_team = df_cassandra_orgdb.join(df_usersdetails, df_cassandra_orgdb.username == df_usersdetails.OrgTool_username, "left").drop("OrgTool_username")
df_cassandra_orgdb_team.write.format("delta").mode("OVERWRITE").option("overwriteSchema", "true").saveAsTable("evaluations.report_collectionmetrics")

# COMMAND ----------

# DBTITLE 1,Data from Cassandra, based on filter collection name
df_collections = spark.read.table("evaluations.report_collectionmetrics")
df_collections = df_collections.filter(df_collections["collection_name"].like('%_fromDataBricks%'))
df_collections = df_collections.filter(df_collections["duration_minutes_total"] > 0)
df_collections = df_collections.select("collection_name").distinct() 

df_collectionwise = spark.read.table("evaluations.AiPA_collectionwise_data")
df_collectionwise = df_collectionwise.select("collection").distinct() 

df_collectionwise_balance = df_collections.join(df_collectionwise, df_collections.collection_name == df_collectionwise.collection, "leftanti")
df_collections_list = df_collectionwise_balance.select("collection_name").distinct().collect()
df_collections_list = [row["collection_name"] for row in df_collections_list]

for collection_name in df_collections_list:

    query = f"SELECT * FROM collection_entries WHERE collection = '{collection_name}' ALLOW FILTERING;"
    df_collection_entries = cassandra_orgdb(query, 'prod')
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
    df_collection_entries = df_collection_entries.withColumn("ID", regexp_extract(col("collection"), r"6\d{7}", 0).cast(LongType()))
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