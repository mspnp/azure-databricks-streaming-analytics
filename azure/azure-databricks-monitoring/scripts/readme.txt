Install the databricks CLI and configure it to connect to the cluster of choice

Edit the metrics properties to add the Log Analytics WorkspaceId and secret

Save!

Run commands below from this folder after building, replacing <cluster-name> with the name of your cluster

databricks fs cp --overwrite ../target/azure-databricks-monitoring-0.9.jar dbfs:/azure-databricks-job/azure-databricks-monitoring-0.9.jar
databricks fs cp --overwrite metrics.properties dbfs:/azure-databricks-job/metrics.properties
databricks fs cp --overwrite spark-metrics.sh dbfs:/databricks/init/<cluster-name>/spark-metrics.sh

Restart the cluster.
