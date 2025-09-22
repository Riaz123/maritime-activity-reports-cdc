#!/bin/bash
# Dataproc Serverless submission for maritime-activity-reports-cdc
# Usage examples:
#   ./scripts/gcp/dataproc_submit.sh demo --date 2025-01-15
#   ./scripts/gcp/dataproc_submit.sh orchestrator setup-cdf
#   ./scripts/gcp/dataproc_submit.sh cli activity-reports --args "--report_type hrz_v2 --input_date 2025-01-15"

set -euo pipefail

# ---- Config ----
PROJECT_ID="${PROJECT_ID:-prj-prod-ds-lon-svc-01}"
REGION="${REGION:-europe-west2}"
ENVIRONMENT="${ENVIRONMENT:-dev}"  # dev|stg|prod
# If you publish a container for this repo, set to your Artifact Registry image:
CONTAINER_IMAGE="${CONTAINER_IMAGE:-${REGION}-docker.pkg.dev/${PROJECT_ID}/maritime/maritime-activity-reports-cdc:latest}"

# Spark properties (Delta + perf)
SPARK_PROPERTIES=(
  "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"
  "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
  "spark.jars.packages=io.delta:delta-spark_2.13:3.2.1"
  "spark.sql.adaptive.enabled=true"
  "spark.sql.adaptive.coalescePartitions.enabled=true"
  "spark.sql.adaptive.skewJoin.enabled=true"
  "spark.serializer=org.apache.spark.serializer.KryoSerializer"
  "spark.sql.execution.arrow.pyspark.enabled=true"
  "spark.sql.execution.arrow.maxRecordsPerBatch=10000"
)

# Resources (tune per workload)
EXECUTOR_INSTANCES="${EXECUTOR_INSTANCES:-2}"
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-8g}"
DRIVER_CORES="${DRIVER_CORES:-4}"
DRIVER_MEMORY="${DRIVER_MEMORY:-8g}"

SPARK_PROPERTIES+=(
  "spark.executor.instances=${EXECUTOR_INSTANCES}"
  "spark.executor.cores=${EXECUTOR_CORES}"
  "spark.executor.memory=${EXECUTOR_MEMORY}"
  "spark.driver.cores=${DRIVER_CORES}"
  "spark.driver.memory=${DRIVER_MEMORY}"
  "spark.executor.memoryOverhead=4g"
  "spark.driver.memoryOverhead=4g"
)

COMMAND="${1:-}"
shift || true

die() { echo "Error: $*" >&2; exit 1; }

[[ -n "${COMMAND}" ]] || die "Missing command. Use one of: demo|orchestrator|cli"

BATCH_ID="maritime-${COMMAND}-$(date +%Y%m%d-%H%M%S)-$RANDOM"
echo "Submitting Dataproc Serverless batch: ${BATCH_ID}"

# Build args and entrypoint per command
MAIN_URI=""
ARGS=()

case "${COMMAND}" in
  demo)
    # Runs the demo script included in this repo. Assumes your container bakes the repo at /home/spark
    # If not using a container, upload this script to GCS and change file:/// to gs:// URI.
    MAIN_URI="file:///home/spark/demo_openais_activity_report.py"
    # Optional arg pass-through, e.g. --date 2025-01-15
    ARGS=("$@")
    ;;
  orchestrator)
    # Calls the CDC/CDF Orchestrator to setup tables or run a flow.
    # Provide a small runner file in GCS that imports and runs the orchestrator,
    # or bake the package into the container and point to the module runner.
    # Option A: baked-in module (requires your image to include the package):
    MAIN_URI="file:///home/spark/-m"
    ARGS=("maritime_activity_reports.orchestrator.cdc_cdf_orchestrator_runner" "$@")
    # Option B (alternative): a runner in GCS (uncomment and replace bucket/path):
    # MAIN_URI="gs://YOUR_BUCKET/jobs/cdc_cdf_orchestrator_runner.py"
    # ARGS=("$@")
    ;;
  cli)
    # Runs the Typer CLI entrypoints if present in src/maritime_activity_reports/cli.py
    # Example:
    #   ./dataproc_submit.sh cli activity-reports --args "--report_type hrz_v2 --input_date 2025-01-15"
    # Expecting: first argument is the subcommand, rest with --args "..."
    SUBCMD="${1:-}"
    shift || true
    EXTRA="${*:-}"
    [[ -n "${SUBCMD}" ]] || die "cli requires a subcommand (e.g. activity-reports)"
    MAIN_URI="file:///home/spark/-m"
    ARGS=("maritime_activity_reports.__main__" "${SUBCMD}")
    if [[ -n "${EXTRA}" ]]; then
      # split EXTRA into args (preserves quoted strings)
      # shellcheck disable=SC2206
      EXTRA_ARR=(${EXTRA})
      ARGS+=("${EXTRA_ARR[@]}")
    fi
    ;;
  *)
    die "Unknown command: ${COMMAND}"
    ;;
esac

# Submit
gcloud dataproc batches submit pyspark \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --batch="${BATCH_ID}" \
  --container-image="${CONTAINER_IMAGE}" \
  --version="2.2" \
  $(printf -- "--properties=%s " "${SPARK_PROPERTIES[@]}") \
  -- "${MAIN_URI}" "${ARGS[@]}"

echo "Submitted. Check status:"
echo "  gcloud dataproc batches describe ${BATCH_ID} --region=${REGION}"
