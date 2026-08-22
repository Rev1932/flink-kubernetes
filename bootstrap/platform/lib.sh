#!/usr/bin/env bash
# Helpers compartilhados pelos scripts de bootstrap da plataforma.
set -euo pipefail

# ---------------------------------------------------------------------------
# Versoes fixadas da plataforma.
#
# ATENCAO ao alterar FLINK_VERSION: o conector MongoDB oficial
# (org.apache.flink:flink-sql-connector-mongodb) so tem release publicado para
# Flink 1.19 e 1.20 -- nao existe artefato para 2.x. Flink 1.20 LTS e portanto o
# teto de versao da plataforma enquanto o sink for MongoDB via SQL.
# Ver README.md, secao "Teto de versao".
# ---------------------------------------------------------------------------
export FLINK_VERSION="${FLINK_VERSION:-1.20.2}"
export FLINK_MAJOR_MINOR="1.20"
export FLINK_OPERATOR_VERSION="${FLINK_OPERATOR_VERSION:-1.15.0}"
export CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"
export STRIMZI_VERSION="${STRIMZI_VERSION:-1.2.0}"
export KAFKA_VERSION="${KAFKA_VERSION:-4.3.1}"
export KAFKA_METADATA_VERSION="${KAFKA_METADATA_VERSION:-4.3-IV0}"
export KUBE_PROM_STACK_VERSION="${KUBE_PROM_STACK_VERSION:-88.5.3}"

# Namespaces da plataforma.
export NS_FLINK_OPERATOR="flink-operator"
export NS_KAFKA="kafka"
export NS_STORAGE="storage"
export NS_MONITORING="monitoring"

# Credenciais do ambiente LOCAL apenas. Em producao estes valores vem de um
# Secret externo gerenciado fora deste repositorio (ver README).
export MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
export MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
export MINIO_BUCKET="flink"
export MONGO_ROOT_USER="${MONGO_ROOT_USER:-root}"
export MONGO_ROOT_PASSWORD="${MONGO_ROOT_PASSWORD:-rootpass}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[!]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[x]\033[0m %s\n' "$*" >&2; exit 1; }

# Cria um namespace de forma idempotente.
ensure_ns() {
  kubectl get namespace "$1" >/dev/null 2>&1 || kubectl create namespace "$1"
}

# helm repo add idempotente (o helm falha se o repo ja existe com outra URL).
ensure_helm_repo() {
  local name=$1 url=$2
  if helm repo list -o json 2>/dev/null | grep -q "\"$name\""; then
    helm repo add "$name" "$url" --force-update >/dev/null
  else
    helm repo add "$name" "$url" >/dev/null
  fi
}

require_bin() {
  command -v "$1" >/dev/null 2>&1 || die "binario obrigatorio nao encontrado: $1"
}
