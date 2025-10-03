import os
from databricks.connect import DatabricksSession

cluster_id = os.getenv("DATABRICKS_CLUSTERID")

# Replace with your actual cluster ID (from Databricks UI)
spark = DatabricksSession.builder.remote(cluster_id = cluster_id).getOrCreate()
print("Spark version:", spark.version)
df = spark.range(5)
df.show()
