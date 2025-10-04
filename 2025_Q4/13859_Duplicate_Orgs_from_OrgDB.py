# https://elsevier.atlassian.net/browse/ORGD-13859
# title: Find the duplicate organization from orgdb.xml

import os
import re
import numpy as np
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from databricks.connect import DatabricksSession

# Databricks Connect Session
cluster_id = os.getenv("DATABRICKS_CLUSTERID")
spark = DatabricksSession.builder.remote(cluster_id = cluster_id).getOrCreate()

# Normalizer UDF
def normalize_udf():
    def normalize_txt(txt):
        if txt is None:
            return None
        txt = txt.lower()
        txt = re.sub(r"&", " ", txt)
        txt = re.sub(r"\.", " ", txt)
        txt = re.sub(r",", " ", txt)
        txt = re.sub(r"[^a-z0-9\s]", " ", txt)
        txt = re.sub(r"\s+", " ", txt).strip()
        return txt
    return udf(normalize_txt, StringType())


# Load Data
orgdb_df = spark.read.table("orgdb_support.list_orgdb")
orgdb_df = orgdb_df.withColumn("orglevel", when(col("orglevel").isNull(), "").otherwise(col("orglevel")))
orgdb_df = orgdb_df.filter(col("orglevel") != "Skeletal")
# orgdb_df = orgdb_df.filter(col("twolettercountry") == "in")
orgdb_df = orgdb_df.select(col("org_id").alias("id"), "city", "country", "orgvisibility")

orgname_df = spark.read.table("orgdb_support.list_orgname")
orgname_df = orgname_df.filter(col("type") != "Acronym")
orgname_df = orgname_df.join(orgdb_df, orgname_df.org_id == orgdb_df.id, how="left")
orgname_df = orgname_df.filter(col("id").isNotNull())
orgname_df = orgname_df.withColumn("city", when(col("city").isNull(), lit("")).otherwise(col("city")))

orgname1_df = orgname_df.select(col("org_id").alias("set1_id"), concat(col("value"), lit(", "), col("city")).alias("set1_orgname_with_city"), col("city").alias("set1_city"), col("country").alias("set1_country"), col("orgvisibility").alias("set1_orgvisibility"))
orgname2_df = orgname_df.select(col("org_id").alias("set2_id"), concat(col("value"), lit(", "), col("city")).alias("set2_orgname_with_city"), col("city").alias("set2_city"), col("country").alias("set2_country"), col("orgvisibility").alias("set2_orgvisibility"))

# Normalize
norm_udf = normalize_udf()
orgname1_df = orgname1_df.withColumn("set1_normalize", norm_udf("set1_orgname_with_city"))
orgname2_df = orgname2_df.withColumn("set2_normalize", norm_udf("set2_orgname_with_city"))
print(f"First: {orgname1_df.count()}, Second: {orgname2_df.count()}")

# Blocking (avoid full cross join explosion)
orgname1_df = orgname1_df.withColumn("block", substring("set1_normalize", 1, 3))
orgname2_df = orgname2_df.withColumn("block", substring("set2_normalize", 1, 3))
crosejoin_df = orgname1_df.join(orgname2_df, "block")
print(f"Blocked candidate count: {crosejoin_df.count()}")

def tokenize(colname):
    stopwords = [
        "co", "inc", "a", "an", "and", "are", "as", "at", "but", "by", "for", "from", "in", "into", "is",
        "of", "on", "or", "over", "that", "the", "these", "this", "those", "to", "was", "were", "with", "about", "after"
    ]
    stopwords_expr = ",".join([f"'{w}'" for w in stopwords])
    return expr(f"filter(split(regexp_replace({colname}, '[^a-z0-9]+', ' '), ' '), x -> x != '' and x not in ({stopwords_expr}))")
crosejoin_df = crosejoin_df.withColumn("set1_tokens", tokenize("set1_normalize"))
crosejoin_df = crosejoin_df.withColumn("set2_tokens", tokenize("set2_normalize"))

def cosine_tokens_udf():
    def cosine_similarity(tokens1, tokens2):
        if tokens1 is None or tokens2 is None:
            return 0.0
        set1, set2 = set(tokens1), set(tokens2)
        if not set1 or not set2:
            return 0.0
        inter = len(set1.intersection(set2))
        denom = (len(set1) * len(set2)) ** 0.5
        return float(inter / denom) if denom != 0 else 0.0
    return udf(cosine_similarity, DoubleType())

cos_udf = cosine_tokens_udf()
final_result = crosejoin_df.withColumn("similarity", cos_udf("set1_tokens", "set2_tokens"))
final_result = final_result.withColumn("id_match", col("set1_id") == col("set2_id"))

#Filter Data
best_matches = final_result.filter(col("id_match") == False)
best_matches = best_matches.filter(col("similarity") >= 0.95)
best_matches = best_matches.select("set1_id", "set1_country", "set1_city", "set1_orgvisibility", "set2_id", "set2_country", "set2_city", "set2_orgvisibility", "set1_orgname_with_city", "set2_orgname_with_city", "similarity")

print(f"Best match count: {best_matches.count()}")
output_files = best_matches.toPandas()
output_files.to_csv("C:/Evaluation/Python/elsevier_eagle/output/duplicate_orgs.tsv", sep="\t", index=True)