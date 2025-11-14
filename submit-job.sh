#!/usr/bin/env bash
set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Usage: $0 <name> [--namespace ns] [--image img] [--url URL] [--large-job true|false] [--num-runs N] [--completions N] [--parallelism N] [--aws-access-key X] [--aws-secret-key X] [--aws-session-token X] [--aws-endpoint URL] [--aws-region R]" >&2
  exit 1
fi

NAME="$1"
shift

NAMESPACE="default"
IMAGE="ghcr.io/versioneer-tech/benchmarking:0.1"
URL="https://s3.waw4-1.cloudferro.com/EarthCODE/OSCAssets/seasfire/seasfire_v0.4.zarr"
LARGE_JOB="false"
NUM_RUNS="1"
COMPLETIONS="1"
PARALLELISM="1"

AWS_ACCESS_KEY=""
AWS_SECRET_KEY=""
AWS_SESSION_TOKEN=""
AWS_ENDPOINT=""
AWS_REGION=""

while [ $# -gt 0 ]; do
  case "$1" in
    --namespace)         NAMESPACE="$2"; shift 2 ;;
    --image)             IMAGE="$2"; shift 2 ;;
    --url)               URL="$2"; shift 2 ;;
    --large-job)         LARGE_JOB="$2"; shift 2 ;;
    --num-runs)          NUM_RUNS="$2"; shift 2 ;;
    --completions)       COMPLETIONS="$2"; shift 2 ;;
    --parallelism)       PARALLELISM="$2"; shift 2 ;;
    --aws-access-key)    AWS_ACCESS_KEY="$2"; shift 2 ;;
    --aws-secret-key)    AWS_SECRET_KEY="$2"; shift 2 ;;
    --aws-session-token) AWS_SESSION_TOKEN="$2"; shift 2 ;;
    --aws-endpoint)      AWS_ENDPOINT="$2"; shift 2 ;;
    --aws-region)        AWS_REGION="$2"; shift 2 ;;
    *) echo "Unknown argument: $1" >&2; exit 1 ;;
  esac
done

RUN_ID="$(date +%Y%m%d%H%M%S)"
JOB_NAME="${NAME}-${RUN_ID}"

{
cat <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: ${JOB_NAME}
  namespace: ${NAMESPACE}
spec:
  completions: ${COMPLETIONS}
  parallelism: ${PARALLELISM}
  template:
    metadata:
      labels:
        app: ${NAME}
        benchmarkRunId: "${RUN_ID}"
    spec:
      restartPolicy: Never
      containers:
      - name: benchmarking
        image: ${IMAGE}
        env:
        - name: URL
          value: "${URL}"
        - name: LARGE_JOB
          value: "${LARGE_JOB}"
        - name: NUM_RUNS
          value: "${NUM_RUNS}"
        - name: RUN_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.labels['benchmarkRunId']
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
EOF

if [ -n "$AWS_ACCESS_KEY" ]; then
cat <<EOF
        - name: AWS_ACCESS_KEY_ID
          value: "${AWS_ACCESS_KEY}"
EOF
fi

if [ -n "$AWS_SECRET_KEY" ]; then
cat <<EOF
        - name: AWS_SECRET_ACCESS_KEY
          value: "${AWS_SECRET_KEY}"
EOF
fi

if [ -n "$AWS_SESSION_TOKEN" ]; then
cat <<EOF
        - name: AWS_SESSION_TOKEN
          value: "${AWS_SESSION_TOKEN}"
EOF
fi

if [ -n "$AWS_ENDPOINT" ]; then
cat <<EOF
        - name: AWS_ENDPOINT
          value: "${AWS_ENDPOINT}"
EOF
fi

if [ -n "$AWS_REGION" ]; then
cat <<EOF
        - name: AWS_REGION
          value: "${AWS_REGION}"
EOF
fi

} | kubectl apply --validate=false -n "${NAMESPACE}" -f -
