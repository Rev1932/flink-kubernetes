#!/usr/bin/env bash
# Instala o Strimzi Cluster Operator e sobe o Kafka do ambiente local.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

require_bin kubectl
require_bin helm

log "Instalando Strimzi ${STRIMZI_VERSION}"
ensure_ns "${NS_KAFKA}"
ensure_helm_repo strimzi "https://strimzi.io/charts/"

# watchAnyNamespace=false: o operator so observa o namespace `kafka`. Os
# KafkaTopic dos tenants tambem sao criados nesse namespace (com o label
# strimzi.io/cluster), e nao no namespace do tenant.
helm upgrade --install strimzi-cluster-operator strimzi/strimzi-kafka-operator \
  --version "${STRIMZI_VERSION}" \
  --namespace "${NS_KAFKA}" \
  --set watchNamespaces="{${NS_KAFKA}}" \
  --wait --timeout 10m

log "Aplicando o cluster Kafka"
kubectl apply -f ./kafka-cr.yaml

log "Aguardando o Kafka ficar pronto (pode levar alguns minutos)"
kubectl -n "${NS_KAFKA}" wait kafka/flink-kafka --for=condition=Ready --timeout=15m

log "Kafka pronto. Bootstrap interno:"
echo "  flink-kafka-kafka-bootstrap.${NS_KAFKA}.svc.cluster.local:9092"
