#!/usr/bin/env bash
# Sobe o MongoDB do ambiente local (sink de teste).
# NAO usar em producao -- la o MongoDB e externo, via Secret.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

require_bin kubectl

log "Instalando MongoDB (somente desenvolvimento)"
ensure_ns "${NS_STORAGE}"
kubectl apply -f ./mongodb-dev.yaml

log "Aguardando MongoDB"
kubectl -n "${NS_STORAGE}" rollout status statefulset/mongodb --timeout=300s

log "MongoDB pronto em mongodb.${NS_STORAGE}.svc.cluster.local:27017"
