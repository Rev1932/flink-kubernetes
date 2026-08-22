#!/usr/bin/env bash
# Instala cert-manager (pre-requisito do webhook) e o Flink Kubernetes Operator.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

require_bin kubectl
require_bin helm

log "Instalando cert-manager ${CERT_MANAGER_VERSION}"
# O operator usa um webhook de validacao/mutacao que precisa de certificados.
kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"

log "Aguardando cert-manager ficar pronto"
kubectl -n cert-manager wait --for=condition=Available deployment --all --timeout=300s

log "Instalando Flink Kubernetes Operator ${FLINK_OPERATOR_VERSION}"
ensure_ns "${NS_FLINK_OPERATOR}"
ensure_helm_repo flink-operator \
  "https://downloads.apache.org/flink/flink-kubernetes-operator-${FLINK_OPERATOR_VERSION}/"

# Metricas do operator expostas em :9999 para o Prometheus. Sao elas que dao o
# "estado atual das transmissoes" a nivel de FlinkDeployment (lifecycle state),
# complementando as metricas que vem de dentro de cada job.
helm upgrade --install flink-kubernetes-operator \
  flink-operator/flink-kubernetes-operator \
  --version "${FLINK_OPERATOR_VERSION}" \
  --namespace "${NS_FLINK_OPERATOR}" \
  --set webhook.create=true \
  --set 'metrics.port=9999' \
  --set-string 'defaultConfiguration.flink-conf\.yaml=kubernetes\.operator\.metrics\.reporter\.prom\.factory\.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
kubernetes.operator.metrics.reporter.prom.port: 9999
kubernetes.operator.reconcile.interval: 15 s
kubernetes.operator.observer.progress-check.interval: 10 s' \
  --wait --timeout 10m

log "Flink Operator instalado"
kubectl -n "${NS_FLINK_OPERATOR}" get pods
