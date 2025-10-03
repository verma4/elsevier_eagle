# Databricks notebook source
# MAGIC %md
# MAGIC Input:
# MAGIC   - evaluations.ai_list_orgs_with_allorgs_batch === **This is a main list of orgs for using in AI job**
# MAGIC   - evaluations.ai_list_orgs_with_results_async_mode_2 === **This is list of ai data output**
# MAGIC   
# MAGIC Output:
# MAGIC   - evaluations.report_orgs_picking_async
# MAGIC   - evaluations.report_collectionmetrics
# MAGIC
# MAGIC
# MAGIC Support:
# MAGIC   - https://elsevier-dev.cloud.databricks.com/editor/notebooks/4383827528071839?o=8907390598234411#command/8585318122823221
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Library
import elsevier_eagle
from elsevier_eagle import *

# COMMAND ----------

# DBTITLE 1,Selection of Organizations for AI Evaluation
#data orgs pick for ai.
df_orgs_picking = read_file_from_catalog('evaluations', 'ai_list_orgs_with_allorgs_batch')
df_orgs_picking = df_orgs_picking.withColumn("collection_name", concat(lit("AiPA+"), col("collection_name"), lit("+ANI")))

# COMMAND ----------

# DBTITLE 1,Final AI Results per Organization (PID)
#data after ai result.
df_async = read_file_from_catalog('evaluations', 'ai_list_orgs_with_result_async_mode_final_2')
df_async = df_async.withColumn("processed_datetime", to_timestamp("processed_datetime", "yyyy-MM-dd HH:mm:ss"))
window_spec = Window.partitionBy("PID").orderBy(df_async["processed_datetime"].desc())
df_with_rank = df_async.withColumn("row_num", row_number().over(window_spec))
df_async = df_with_rank.filter("row_num = 1").drop("row_num")

# COMMAND ----------

# DBTITLE 1,Joining Organizations for AI Evaluation with Final AI Results
#join both tables
df_orgs_picking_async = df_orgs_picking.join(df_async, df_orgs_picking.org_id == df_async.PID, "left")
df_orgs_picking_async = df_orgs_picking_async.select("org_id", "org_name", "street", "city", "state", "postal_code", "country", "former_name", "departments", "ai_picked", "collection_review_pending", "collection_name", "ai_ok", "OA", "RY", "RN", "MP", "processed_datetime")

#add affiliation count
df_orgs_picking_list = [row["org_id"] for row in df_orgs_picking_async.select("org_id").distinct().collect()]
df_affound = afcount_from_solr_for_orgids('prod', 'ani', df_orgs_picking_list).distinct()

df_orgs_picking_async_afcount = df_orgs_picking_async.join(df_affound, df_orgs_picking_async.org_id == df_affound.id, "left").drop("id")
df_orgs_picking_async_afcount = df_orgs_picking_async_afcount.fillna(0, subset=['count'])
df_orgs_picking_async_afcount.write.format("delta").mode("OVERWRITE").option("overwriteSchema", "true").saveAsTable("evaluations.report_orgs_picking_async")

# COMMAND ----------

# DBTITLE 1,Get the collectionmetrics Data and Cleanup
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

df_usersdetails = read_file_s3_amarnath('list_usersdetails')
df_usersdetails = df_usersdetails["list_usersdetails"]
df_cassandra_orgdb_team = df_cassandra_orgdb.join(df_usersdetails, df_cassandra_orgdb.username == df_usersdetails.OrgTool_username, "left").drop("OrgTool_username")
df_cassandra_orgdb_team.write.format("delta").mode("OVERWRITE").option("overwriteSchema", "true").saveAsTable("evaluations.report_collectionmetrics")