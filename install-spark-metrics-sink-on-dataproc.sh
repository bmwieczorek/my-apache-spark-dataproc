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
  echo "SPARK_METRICS_SINK SPARK_METRICS_SINK_JAR_GCS_PATH=${SPARK_METRICS_SINK_JAR_GCS_PATH}, SPARK_METRICS_SINK_JAR_NAME=${SPARK_METRICS_SINK_JAR_NAME}"
  gsutil cp "${SPARK_METRICS_SINK_JAR_GCS_PATH}" "${VM_CONNECTORS_DATAPROC_DIR}/"
  local -r jar_name=${SPARK_METRICS_SINK_JAR_GCS_PATH##*/}
  ln -s -f ${VM_CONNECTORS_DATAPROC_DIR}/${jar_name} ${VM_CONNECTORS_DATAPROC_DIR}/${SPARK_METRICS_SINK_JAR_NAME}
  echo "SPARK_METRICS_SINK jar_name=${jar_name}, content of directory ${VM_CONNECTORS_DATAPROC_DIR}: $(ls -la ${VM_CONNECTORS_DATAPROC_DIR}/ | sed -z 's/\n/,/g;s/,$/\n/')"
}

install_sink "$SPARK_METRICS_SINK_JAR_GCS_PATH" "$SPARK_METRICS_SINK_JAR_NAME"
