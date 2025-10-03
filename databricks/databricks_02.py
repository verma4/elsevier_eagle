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
print("Total clusters:", len(clusters))

for c in clusters:
        if ClusterID in c.cluster_id:
                print("Cluster ID           :", c.cluster_id)
                print("Cluster Name         :", c.cluster_name)
                print("instance_profile_arn :", c.aws_attributes.instance_profile_arn)
                print("cluster_cores        :", c.cluster_cores)
                print("Creator              :", c.creator_user_name)
                print("Min_Workers          :", c.autoscale.min_workers if c.autoscale else None)
                print("Max_Workers          :", c.autoscale.max_workers if c.autoscale else None)
                print("Environment          :", c.custom_tags["Environment"])
                print("Contact              :", c.custom_tags["Contact"])
                print("Description          :", c.custom_tags["Description"])
                print("Orchestration        :", c.custom_tags["Orchestration"])
                print("Product              :", c.custom_tags["Product"])
                print("CostCode             :", c.custom_tags["CostCode"])
                print("Vendor               :", c.default_tags["Vendor"])
                print("Driver_Private_IP    :", c.driver.private_ip)
                print("Executors_Private_IP :", c.executors[0].private_ip if c.executors else None)
                print("Spark_context_id     :", c.spark_context_id)
                print("Spark_env_vars       :", c.spark_env_vars)
                print("Spark Ver.           :", c.spark_version)
                print("Start_Time           :", c.start_time)
                print("State                :", c.state.name)
                print("Restarted_Time       :", c.last_restarted_time)
                print("Driver_Type          :", c.driver_node_type_id)
                print("Terminated_Time      :", c.terminated_time)
                print("Termination_Code     :", c.termination_reason.code.value if c.termination_reason and c.termination_reason.code else None)
                print("Termination_Type     :", c.termination_reason.type.value if c.termination_reason and c.termination_reason.type else None)
