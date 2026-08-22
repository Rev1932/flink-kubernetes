#!/usr/bin/env bash
# Sobe o MinIO (backend S3 de checkpoints/savepoints/HA) e cria o bucket.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

require_bin kubectl

log "Instalando MinIO"
ensure_ns "${NS_STORAGE}"
kubectl apply -f ./minio.yaml

log "Aguardando MinIO"
kubectl -n "${NS_STORAGE}" wait --for=condition=Available deployment/minio --timeout=300s
kubectl -n "${NS_STORAGE}" wait --for=condition=Complete job/minio-create-bucket --timeout=300s

log "MinIO pronto -- console em http://localhost:9001 (${MINIO_ROOT_USER}/${MINIO_ROOT_PASSWORD})"
