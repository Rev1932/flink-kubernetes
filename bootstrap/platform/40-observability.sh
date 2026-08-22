#!/usr/bin/env bash
# Instala kube-prometheus-stack (Prometheus + Grafana + Alertmanager).
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
source ./lib.sh

require_bin kubectl
require_bin helm

log "Instalando kube-prometheus-stack ${KUBE_PROM_STACK_VERSION}"
ensure_ns "${NS_MONITORING}"
ensure_helm_repo prometheus-community "https://prometheus-community.github.io/helm-charts"

# Os seletores vazios sao o ponto importante: por padrao o Prometheus Operator
# so descobre PodMonitor/PrometheusRule que carreguem o label da release. Como os
# PodMonitor sao gerados pelo chart de cada tenant (releases diferentes), abrimos
# a descoberta para todos os namespaces.
helm upgrade --install kube-prometheus-stack \
  prometheus-community/kube-prometheus-stack \
  --version "${KUBE_PROM_STACK_VERSION}" \
  --namespace "${NS_MONITORING}" \
  --set prometheus.prometheusSpec.podMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.ruleSelectorNilUsesHelmValues=false \
  --set prometheus.prometheusSpec.retention=6h \
  --set prometheus.prometheusSpec.scrapeInterval=15s \
  --set grafana.service.type=NodePort \
  --set grafana.service.nodePort=30300 \
  --set grafana.adminPassword=admin \
  --set grafana.sidecar.dashboards.enabled=true \
  --set grafana.sidecar.dashboards.searchNamespace=ALL \
  --set alertmanager.enabled=true \
  --wait --timeout 15m

log "Carregando dashboards da plataforma"
# O sidecar do Grafana descobre ConfigMaps com o label grafana_dashboard=1 em
# qualquer namespace e os carrega automaticamente.
kubectl -n "${NS_MONITORING}" create configmap flink-dashboards \
  --from-file=../../observability/dashboards/ \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "${NS_MONITORING}" label configmap flink-dashboards \
  grafana_dashboard=1 --overwrite

log "Aplicando ServiceMonitor do Flink Operator"
kubectl apply -f ../../observability/operator-servicemonitor.yaml

log "Observabilidade pronta -- Grafana em http://localhost:3000 (admin/admin)"
