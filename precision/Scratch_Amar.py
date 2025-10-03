# Databricks notebook source
import elsevier_eagle
from elsevier_eagle import cassandra_orgdb

# COMMAND ----------

#elsevier:, -acmaffiliation, -affiliation, -anidata, -corrapitaskaudit, -deltabatchesinfo, -deptprofile, -master, -mhubaffiliation, -orgprofile, -patentaffiliation, -rdmaffiliation, -releasesupportfiles
#orgdb:, -acmrelease, -archived_orgs, -archived_workspaces, -collection_entries, -configurationlimits, -counters, -deltabatchesinfo, -insthierarchydetails, -insthierarchydetailsrelease, -job_collection_entries, -jobs, -jobs_excecution_time, -locked_orgs, -lockedorginfo, -org_note, -orgaudit, -orgauditbydate, -orgs, -orgtool_scheduler, -orgtoolproperties, -orgxmlaudit, -release, -useraudit, -userauditbydate, -users, -variantsource, -varianttypes, -working_copy, -workspace_comments, -workspace_history, -workspaces


# COMMAND ----------

import pandas as pd

# COMMAND ----------

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy
import uuid

host_map = ['orgtool-uat-cassandra-2.np-mhub2.elsst.com']  # 👈 wrapped in list

cluster = Cluster(
    host_map,
    port=9042,
    protocol_version=4,
    execution_profiles={
        EXEC_PROFILE_DEFAULT: ExecutionProfile(
            load_balancing_policy=RoundRobinPolicy()
        )
    },
)

try:
    session = cluster.connect()
    session.set_keyspace('orgdb')
    rows = session.execute("SELECT * FROM lockedorginfo where orgid = 60230122")
    data = []
    for row in rows:
        row_dict = {}
        for column, value in row._asdict().items():
            if isinstance(value, uuid.UUID):
                row_dict[column] = str(value)
            elif isinstance(value, set):
                row_dict[column] = list(sorted(value))
            else:
                row_dict[column] = str(value)
        data.append(row_dict)
    pd_data = pd.DataFrame(data)
    if not pd_data.empty:
        display(spark.createDataFrame(pd_data))
finally:
    cluster.shutdown()

# COMMAND ----------

query = "select * from workspaces"
cassandra_orgdb(query ,'prod')

# COMMAND ----------

df_main = spark.read.table("evaluations.ai_list_orgs_with_allorgs_batch")
df_main = df_main.withColumnRenamed("collection_name", "main_collection_name")
df_main = df_main.withColumnRenamed("ai_picked", "main_ai_picked")
df_calc = spark.read.table("evaluations.report_orgs_picking_async")


df_data = df_main.join(df_calc, on="org_id", how="left")
df_data = df_data.filter(df_data["main_ai_picked"]=="yes")
df_data = df_data.filter(df_data["main_collection_name"].isNull())
display(df_data)

# COMMAND ----------

df = spark.read.table("evaluations.Precision_Check_Reports1")
display(df)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

list_notes = spark.read.json("s3://dp-evaluation/amarnath/orgnote.json")
list_notes = list_notes.withColumn("orgNotes", explode_outer("orgNotes"))
list_notes = list_notes.withColumn("addeddate", col("orgNotes.addeddate"))
list_notes = list_notes.withColumn("comment", col("orgNotes.comment"))
list_notes = list_notes.withColumn("location", col("orgNotes.location"))
list_notes = list_notes.withColumn("reason", col("orgNotes.reason"))
list_notes = list_notes.withColumn("ticketnumber", col("orgNotes.ticketnumber"))
list_notes = list_notes.withColumn("user", col("orgNotes.user"))
list_notes = list_notes.withColumn("reason", explode_outer("reason"))
list_notes = list_notes.select(col("orgId").alias("org_id"), "addeddate", "comment", "location", "reason", "ticketnumber", "user")
list_notes = list_notes.filter(list_notes["addeddate"].isNotNull())
list_notes = list_notes.withColumn("year", expr('RIGHT(addeddate, 4)'))
list_notes = list_notes.withColumn("month", expr('LEFT(addeddate, 3)'))
list_notes = list_notes.withColumn("day", substring_index("addeddate", ',', 1))
list_notes = list_notes.withColumn("day", substring_index("day", ' ', -1))
list_notes = list_notes.withColumn("month", when((col("month")=="Jan"),"01").when((col("month")=="Feb"),"02").when((col("month")=="Mar"),"03").when((col("month")=="Apr"),"04").when((col("month")=="May"),"05").when((col("month")=="Jun"),"06").when((col("month")=="Jul"),"07").when((col("month")=="Aug"),"08").when((col("month")=="Sep"),"09").when((col("month")=="Oct"),"10").when((col("month")=="Nov"),"11").when((col("month")=="Dec"),"12"))
list_notes = list_notes.withColumn("addeddate", concat(col("year"),lit("-"),col("month"),lit("-"),"day").cast(DateType())).drop("year").drop("month").drop("day")
list_notes = list_notes.orderBy(col("addeddate").desc()).distinct()
print(f"count of rows: {list_notes.count()}")

display(list_notes)

# COMMAND ----------

df = spark.read.table("orgdb_support.list_address")
df = df.filter(col("org_id")=="60096672")
display(df)