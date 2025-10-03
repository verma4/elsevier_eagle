# Databricks notebook source
import elsevier_eagle
from elsevier_eagle import *

# COMMAND ----------

# - evaluations.ai_list_orgs_with_allorgs_batch
# - evaluations.ai_list_orgs_with_result_async_mode_final_2
#     - evaluations.AiPA_RN_output_datewise
# - evaluations.affiliation_list_async_mode_2
#     - evaluations.report_orgs_picking_async
#     - evaluations.report_collectionmetrics
#         - evaluations.AiPA_RNRY_Numbers
#         - evaluations.AiPA_RN_Jsonl_Track
#         - evaluations.AiPA_collectionwise_data


# COMMAND ----------

list_of_ids = ['60029706','60025577','60007215','60071364','60006469','60029681','60012704','60084126','60032592','60100519','60080207','60069943','60021005','60031257','60048717','60006988','60105371','60069716','60104002','60000906','60012881','60024872','60018029','60012256','60059951','60052808','60022637','60001873','60014217','60026245']

for orgid in list_of_ids:
    print(f"orgid : {orgid}")

    df = spark.read.table("evaluations.AiPA_RNRY_Numbers")
    df = df.sort("PID","processed_datetime")

    list_of_affiliationid_ry = df.filter(df["PID"]==orgid).select("AffID_RY").collect()[0]['AffID_RY']
    # print(list_of_affiliationid_ry)

    df_check = spark.read.table("evaluations.ai_list_orgs_with_results_async_mode_2")
    df_check = df_check.withColumn("AffID", explode(split("AffID", "\|")))
    #filter afids
    df_check = df_check.filter(df_check["AffID"].isin(list_of_affiliationid_ry))


    #solr data
    df_solr = data_from_solr_for_afid('prod', 'ani', list_of_affiliationid_ry)


    #merge data
    df_solr_check = df_solr.join(df_check, df_check.AffID == df_solr.affiliationid, "left")
    df_solr_check = df_solr_check.select("AffID", "Raw", "Aft", "Vendor", "Reasoning", "PID", "Batch_Id", "processed_datetime", "xml", "afttaggedjson", "vendortaggedjson")
    print(f"total affiliations for review : {df_solr_check.select('AffID').distinct().count()}")

    df_solr_check = df_solr_check.withColumn(
        "Result",
        when(
            (col("Raw") == "true") & (col("Aft") == "true") & (col("vendor") == "true"),
            "RN - NoProblem"
        ).when(
            (col("Raw") == "false") & (col("Aft") == "true") & (col("vendor") == "true"),
            "RN - NoProblem"
        ).when(
            (col("Raw") == "false") & (col("Aft") == "false") & (col("vendor") == "true"),
            "RY - Problem"
        ).when(
            (col("Raw") == "false") & (col("Aft") == "true") & (col("vendor") == "false"),
            "RY - Problem"
        ).when(
            (col("Raw") == "true") & (col("Aft") == "false") & (col("vendor") == "false"),
            "RY - Problem"
        ).when(
            (col("Raw") == "false") & (col("Aft") == "false") & (col("vendor") == "false"),
            "RY - Problem"
        ).when(
            (col("Raw") == "true") & (col("Aft") == "true") & (col("vendor") == "false"),
            "2outof3"
        ).when(
            (col("Raw") == "true") & (col("Aft") == "false") & (col("vendor") == "true"),
            "2outof3"
        )
    )


    df_collectionwise = spark.read.table("evaluations.AiPA_collectionwise_data")
    df_collectionwise = df_collectionwise.filter(df_collectionwise["collection"].like(f'%{orgid}%'))
    df_collectionwise = df_collectionwise.select(explode("HasPrecisionIssue_affiliationid").alias("AfterCollectionReview")).distinct()

    df_solr_check_collectionwise = df_solr_check.join(df_collectionwise, df_solr_check.AffID == df_collectionwise.AfterCollectionReview, "left")
    df_solr_check_collectionwise = df_solr_check_collectionwise.withColumn("AfterCollectionReview", when(col("AfterCollectionReview").isNull(), "No Precision").otherwise("Has Precision"))
    df_solr_check_collectionwise.write.mode("append").saveAsTable("evaluations.hasissueajit24")

# COMMAND ----------

data = spark.read.table("evaluations.hasissueajit24")
display(data)