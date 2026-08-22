SHELL := /bin/bash
.DEFAULT_GOAL := help

CLUSTER      ?= flink-platform
RUNNER_IMAGE ?= flink-sql-runner:1.20.2
CHART        := charts/flink-pipeline
# Convencao: tenants/<nome>.yaml  ->  release Helm <nome>, namespace <nome>.
# O identificador logico do tenant (ex.: `N`, usado em N_topico_1) vem de
# tenant.id dentro do proprio arquivo -- ver charts/flink-pipeline/README.md.
TENANTS      := $(patsubst tenants/%.yaml,%,$(wildcard tenants/*.yaml))

.PHONY: help
help: ## Lista os alvos disponiveis
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "\033[36m%-16s\033[0m %s\n",$$1,$$2}'

# ---------------------------------------------------------------------------
# Ciclo de vida do ambiente
# ---------------------------------------------------------------------------

.PHONY: cluster
cluster: ## Cria o cluster kind
	@kind get clusters 2>/dev/null | grep -qx '$(CLUSTER)' \
		&& echo "cluster '$(CLUSTER)' ja existe" \
		|| kind create cluster --config bootstrap/kind-cluster.yaml

.PHONY: platform
platform: ## Instala operator, Kafka, MinIO, observabilidade e MongoDB dev
	@for s in bootstrap/platform/[0-9]*.sh; do \
		echo "--- $$s"; bash "$$s" || exit 1; \
	done

.PHONY: up
up: cluster platform build ## Sobe o ambiente completo do zero
	@echo ""
	@echo "Ambiente pronto. Proximo passo: make deploy-all"

.PHONY: down
down: ## Destroi o cluster kind
	kind delete cluster --name $(CLUSTER)

# ---------------------------------------------------------------------------
# Imagem do runner
# ---------------------------------------------------------------------------

.PHONY: build
build: ## Constroi a imagem do SQL runner e carrega no kind
	docker build -t $(RUNNER_IMAGE) runner/
	kind load docker-image $(RUNNER_IMAGE) --name $(CLUSTER)

# ---------------------------------------------------------------------------
# Deploy de tenants
# ---------------------------------------------------------------------------

.PHONY: template
template: ## Renderiza os manifestos de um tenant sem aplicar (TENANT=tenant-n)
	@test -n "$(TENANT)" || { echo "uso: make template TENANT=tenant-n"; exit 1; }
	@helm template $(TENANT) $(CHART) -f tenants/$(TENANT).yaml --namespace $(TENANT)

.PHONY: deploy
deploy: ## Instala/atualiza um tenant (TENANT=tenant-n)
	@test -n "$(TENANT)" || { echo "uso: make deploy TENANT=tenant-n"; exit 1; }
	helm upgrade --install $(TENANT) $(CHART) \
		-f tenants/$(TENANT).yaml \
		--namespace $(TENANT) --create-namespace \
		--wait --timeout 10m

.PHONY: deploy-all
deploy-all: ## Instala/atualiza todos os tenants em tenants/
	@for t in $(TENANTS); do \
		echo "=== $$t"; $(MAKE) --no-print-directory deploy TENANT=$$t || exit 1; \
	done

.PHONY: undeploy
undeploy: ## Remove um tenant (TENANT=tenant-n)
	@test -n "$(TENANT)" || { echo "uso: make undeploy TENANT=tenant-n"; exit 1; }
	helm uninstall $(TENANT) --namespace $(TENANT)

# ---------------------------------------------------------------------------
# Operacao e verificacao
# ---------------------------------------------------------------------------

.PHONY: status
status: ## Estado atual de todas as transmissoes
	@kubectl get flinkdeployment -A -o custom-columns=\
NAMESPACE:.metadata.namespace,NAME:.metadata.name,LIFECYCLE:.status.lifecycleState,JOB:.status.jobStatus.state

.PHONY: lint
lint: ## Valida o chart e a normalizacao de nomes (nao precisa de cluster)
	helm lint $(CHART) -f tenants/tenant-n.yaml
	bash tests/test-naming.sh

.PHONY: test-e2e
test-e2e: ## Teste end-to-end: produz no Kafka e verifica o MongoDB
	bash tests/e2e.sh

.PHONY: grafana
grafana: ## Endereco do Grafana
	@echo "http://localhost:3000  (admin/admin)"

.PHONY: flink-ui
flink-ui: ## Port-forward do Flink UI (TENANT=tenant-n JOB=n-streaming-1)
	@test -n "$(TENANT)" -a -n "$(JOB)" || { echo "uso: make flink-ui TENANT=tenant-n JOB=n-streaming-1"; exit 1; }
	kubectl -n $(TENANT) port-forward svc/$(JOB)-rest 8081:8081
