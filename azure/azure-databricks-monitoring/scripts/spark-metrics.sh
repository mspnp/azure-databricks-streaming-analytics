#!/bin/bash

METRICS_DIR=/dbfs/azure-databricks-job
SPARK_CONF_DIR=/databricks/spark/conf
METRICS_PROPS=${SPARK_CONF_DIR}/metrics.properties
TEMP_METRICS_PROPS=${SPARK_CONF_DIR}/tmp.metrics.properties
echo "Copying metrics jar"
cp -f "$METRICS_DIR/azure-databricks-monitoring-0.9.jar" /mnt/driver-daemon/jars || { echo "Error copying file"; exit 1;}
echo "Copied metrics jar successfully"
cat "$METRICS_DIR/metrics.properties" <(echo) "$METRICS_PROPS" > "$TEMP_METRICS_PROPS"
mv "$TEMP_METRICS_PROPS" "$METRICS_PROPS" || { echo "Error rewriting metric.properties"; exit 2; }
echo "Merged metrics.properties successfully"
