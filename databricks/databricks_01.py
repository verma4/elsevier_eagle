import os
from databricks.sdk import WorkspaceClient

# Replace with your details
HOST = os.getenv("DATABRICKS_HOST")
TOKEN = os.getenv("DATABRICKS_TOKEN")
ClusterID = os.getenv("DATABRICKS_CLUSTERID")

# Create client
wc = WorkspaceClient(host=HOST, token=TOKEN)

# List clusters
clusters = list(wc.clusters.list())
print("Total Clusters:", len(clusters))

# Print output of cluster_id: 0822-174200-pocy9ct5
cluster = wc.clusters.get(cluster_id = ClusterID)
print("Name:", cluster.cluster_name)
print("State:", cluster.state)
