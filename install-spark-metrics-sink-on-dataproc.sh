#!/bin/bash

set -euxo pipefail

readonly VM_CONNECTORS_DATAPROC_DIR=/usr/local/share/google/dataproc/lib
readonly SPARK_METRICS_SINK_JAR_GCS_PATH=$(/usr/share/google/get_metadata_value attributes/spark-metrics-sink-jar-gcs-path || true)
readonly SPARK_METRICS_SINK_JAR_NAME=$(/usr/share/google/get_metadata_value attributes/spark-metrics-sink-jar-name || true)

is_worker() {
  local role
  role="$(/usr/share/google/get_metadata_value attributes/dataproc-role)"
  if [[ $role != Master ]]; then
    return 0
  fi
  return 1
}

install_sink() {
  echo "[SPARK_METRICS_SINK] Installing Spark metrics sink jar..."
  echo "[SPARK_METRICS_SINK] VM_CONNECTORS_DATAPROC_DIR/=${VM_CONNECTORS_DATAPROC_DIR}/"
  echo "[SPARK_METRICS_SINK] SPARK_METRICS_SINK_JAR_GCS_PATH=${SPARK_METRICS_SINK_JAR_GCS_PATH}"
  echo "[SPARK_METRICS_SINK] SPARK_METRICS_SINK_JAR_NAME=${SPARK_METRICS_SINK_JAR_NAME}"
  gsutil cp "${SPARK_METRICS_SINK_JAR_GCS_PATH}" "${VM_CONNECTORS_DATAPROC_DIR}/"
  local -r jar_name=${SPARK_METRICS_SINK_JAR_GCS_PATH##*/}
  echo "[SPARK_METRICS_SINK] jar_name=${jar_name}"
  ln -s -f ${VM_CONNECTORS_DATAPROC_DIR}/${jar_name} ${VM_CONNECTORS_DATAPROC_DIR}/${SPARK_METRICS_SINK_JAR_NAME}
  local -r directory_content="$(ls -la ${VM_CONNECTORS_DATAPROC_DIR}/)"
  echo "[SPARK_METRICS_SINK] Content of directory ${VM_CONNECTORS_DATAPROC_DIR}: ${directory_content}"
  echo "[SPARK_METRICS_SINK] Done"
}

install_sink "$SPARK_METRICS_SINK_JAR_GCS_PATH" "$SPARK_METRICS_SINK_JAR_NAME"
