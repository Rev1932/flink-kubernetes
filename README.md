# Plataforma de streaming multi-tenant sobre Apache Flink

Infraestrutura Kubernetes para hospedar N×K jobs de streaming: **N tenants × K
pipelines padronizados**, com pipelines versionados em Git, recursos governados
por manifesto e o estado das transmissões visível em tempo real.

Adicionar um tenant ou um pipeline é editar YAML. Nunca compilar Java, nunca
duplicar manifesto.

---

## O contrato de nomes

É o eixo de toda a arquitetura. Dois espaços de nomes convivem e **não podem ser
confundidos**:

| Conceito | Padrão | Tenant `N` | Tenant `K` |
|---|---|---|---|
| Tópico de origem (Kafka) | `<TENANT>_topico_<i>` | `N_topico_1` | `K_topico_1` |
| Job / consumer group | `<TENANT>_<pipeline>` | `N_streaming_1` | `K_streaming_1` |
| Coleção destino (MongoDB) | `<TENANT>_<pipeline>` | `N_streaming_1` | `K_streaming_1` |
| **Recurso Kubernetes** | DNS-1123 | `n-streaming-1` | `k-streaming-1` |

O Kubernetes exige nomes minúsculos sem underscore; Kafka e MongoDB precisam do
literal. O chart mantém os dois separados via `tenant.id` (lógico) e
`tenant.slug` (recurso) — ver `charts/flink-pipeline/templates/_helpers.tpl`.

> **Sempre cite o id entre aspas:** `tenant.id: "N"`. Sem aspas, o YAML lê `N`
> como o booleano `false` (regra YAML 1.1, que trata `N`/`Y`/`no`/`on` como
> booleanos) e o tenant N geraria tópicos `false_topico_1`. O chart detecta e
> recusa isso com mensagem explícita, mas a origem do erro é a falta de aspas.

---

## Arquitetura

```
                    ┌────────────── namespace: tenant-n ──────────────┐
Kafka (Strimzi)     │  FlinkDeployment n-streaming-1  [JM + n×TM]     │
  N_topico_1  ──────┼─▶  ConfigMap n-streaming-1-sql                  ├──▶ MongoDB
  N_topico_2  ──────┼─▶  FlinkDeployment n-streaming-2  [JM + n×TM]   │    N_streaming_1
                    │   ResourceQuota · LimitRange · SA+RBAC          │    N_streaming_2
                    └─────────────────────────────────────────────────┘
                    ┌────────────── namespace: tenant-k ──────────────┐
  K_topico_1  ──────┼─▶  FlinkDeployment k-streaming-1  [JM + n×TM]   ├──▶ K_streaming_1
                    └─────────────────────────────────────────────────┘

  MinIO / S3  ← checkpoints, savepoints, metadados de HA
  Prometheus  ← PodMonitor :9249 (JM/TM) + métricas do operator → Grafana
```

Uma **única imagem** roda todos os jobs. O que difere entre eles é o `.sql`
montado por ConfigMap e as variáveis de ambiente injetadas. Um `FlinkDeployment`
por (tenant, pipeline) em *application mode*: o raio de impacto de uma falha é
exatamente um job.

---

## Componentes

| Diretório | Papel |
|---|---|
| `bootstrap/` | Cluster kind + instalação da plataforma (operator, Kafka, MinIO, Prometheus, MongoDB dev) |
| `runner/` | `SqlRunner` genérico + Dockerfile multi-stage (build sem JDK local) |
| `charts/flink-pipeline/` | Chart Helm: **uma release = um tenant** |
| `charts/flink-pipeline/pipelines/` | **Catálogo de pipelines** — o SQL versionado |
| `tenants/` | Um arquivo por tenant: quais pipelines e com quais recursos |
| `observability/` | Dashboard Grafana e ServiceMonitor do operator |
| `tests/` | Testes de normalização de nomes e teste end-to-end |

O catálogo fica **dentro** do chart porque o Helm só lê arquivos do próprio
diretório do chart (`.Files.Get`).

---

## Uso

```bash
make up            # kind + plataforma + imagem do runner
make deploy-all    # aplica todos os tenants em tenants/
make status        # estado atual de todas as transmissões
make test-e2e      # Kafka -> Flink -> MongoDB, ponta a ponta
```

Outros alvos: `make lint` (valida sem cluster), `make template TENANT=tenant-n`
(renderiza sem aplicar), `make flink-ui TENANT=tenant-n JOB=n-streaming-1`,
`make down`.

Grafana em `http://localhost:3000` (admin/admin), console do MinIO em
`http://localhost:9001`.

---

## Runbook

### Adicionar um tenant

```bash
cp tenants/tenant-n.yaml tenants/tenant-j.yaml
# edite: tenant.id: "J" e os pipelines contratados
make deploy TENANT=tenant-j
```

Cria namespace, RBAC, cota, tópicos `J_topico_*`, jobs `J_streaming_*` e os
alertas correspondentes.

### Adicionar um pipeline ao catálogo

1. `charts/flink-pipeline/pipelines/streaming_3/pipeline.sql` — use apenas
   placeholders (`${TENANT}`, `${JOB_NAME}`, `${KAFKA_BOOTSTRAP}`,
   `${MONGO_URI}`, `${MONGO_DB}`), nunca nomes literais de tenant.
2. `pipeline.yaml` ao lado — `sources`, `stateful`, `requiredVars`, `defaults`.
3. Habilite nos tenants que o contratarem.
4. `make lint` verifica que todo placeholder tem env correspondente.

Nada de rebuild de imagem: o SQL viaja por ConfigMap.

### Alterar um pipeline em produção

Edite o `.sql` e rode `make deploy TENANT=...`. Com `upgradeMode: savepoint`
(padrão para pipelines com estado), o operator tira um savepoint, sobe a nova
versão e retoma do estado.

Alterações que mudam a topologia do job (novos operadores, mudança de chave de
agregação) podem impedir a restauração do savepoint. Nesse caso, ou o estado é
descartado conscientemente (`upgradeMode: stateless`), ou o pipeline novo entra
como `streaming_N+1` e o antigo é desligado depois — a segunda opção é a
preferível quando o estado importa.

### Múltiplos sinks num pipeline

Use `EXECUTE STATEMENT SET ... END;` em volta dos `INSERT`. O parser do runner
trata o bloco como um único statement, então continua sendo **um** job com um
checkpoint coerente — em vez de um job por `INSERT`.

---

## Decisões e limites

### Teto de versão: Flink 1.20 LTS

O conector MongoDB oficial (`org.apache.flink:flink-sql-connector-mongodb`) tem
como release mais recente a tag `v2.0.0`, publicada no Maven Central **apenas**
para Flink 1.19 e 1.20. Não existe artefato para 2.x. O conector Kafka, em
contraste, já está em `5.0.0-2.2`.

Como o sink é MongoDB via SQL, **Flink 1.20 LTS é a única versão coerente**:

| Componente | Versão | Observação |
|---|---|---|
| Apache Flink | `1.20.2` | LTS, na matriz validada do operator |
| Flink Kubernetes Operator | `1.15.0` | valida 1.20.x, 2.0–2.2 |
| `flink-sql-connector-kafka` | `3.4.0-1.20` | |
| `flink-sql-connector-mongodb` | `2.0.0-1.20` | **o teto** |
| Strimzi / Kafka | `1.2.0` / `4.3.1` | API `v1`, KRaft |

Migrar para Flink 2.x depende de um release do conector MongoDB para 2.x. O
trabalho existe no branch `main` do repositório upstream, sem release publicado.
Mitigação já embutida: o sink está isolado num único bloco `CREATE TABLE` por
pipeline — trocá-lo não afeta o restante da arquitetura.

### Custo do application mode em escala

Isolamento por job custa um JobManager por job. Na faixa dimensionada
(10–30 tenants × 5–10 pipelines):

> **30 tenants × 10 pipelines = 300 JobManagers.** A 0,5 vCPU / 1,6 GiB cada,
> são ~150 vCPU e ~**480 GiB** **antes de qualquer TaskManager**.

O valor de 1,6 GiB não é folga: com 1 GiB o JobManager é **OOMKilled** (exit 137)
durante o planejamento do SQL — verificado neste ambiente. O planner do Flink
1.20 pressiona o metaspace, e `jobmanager.memory.process.size` soma heap +
metaspace + overhead da JVM. Como há um JobManager por job, esse número domina
o custo da plataforma.

Duas mitigações já estão no desenho:

1. JobManager dimensionado enxuto — em application mode ele coordena um só job.
2. `defaults.executionMode` existe no chart como ponto de extensão. Hoje só
   `application` está implementado; migrar pipelines leves para um session
   cluster por tenant (`FlinkSessionJob`) será mudança de values e de um
   template, não reescrita.

**Recomendação:** medir o custo real com um tenant completo antes do onboarding
dos 30.

### Partições ociosas travam janelas temporais

O watermark de uma fonte Kafka é o **mínimo** entre suas partições. Se o tráfego
não é uniforme, uma partição sem eventos mantém o watermark parado no passado e
**nenhuma janela fecha** — o job aparece como `RUNNING`, consome mensagens
normalmente e nunca escreve nada no sink. É o modo de falha mais confuso desta
arquitetura, porque não gera erro nenhum.

Este comportamento foi observado na validação: com 3 partições e agregação em
janela de 1 minuto, as coleções ficavam vazias enquanto o pipeline sem janela do
mesmo tenant gravava normalmente.

A plataforma define `table.exec.source.idle-timeout` (padrão `15s`, em
`defaults.sourceIdleTimeout`): uma partição sem eventos por esse período é
marcada como ociosa e deixa de participar do cálculo do watermark. Ajuste para
cima em pipelines cuja latência tolerada seja maior.

### Entrega no sink é at-least-once

O conector MongoDB não suporta exactly-once. Por isso todo `CREATE TABLE` de
sink declara `PRIMARY KEY ... NOT ENFORCED`: é o que faz o conector operar em
modo upsert, derivando o `_id` da chave. Sem isso, um restart a partir de
checkpoint reescreveria janelas já gravadas como documentos novos. O teste
end-to-end verifica essa propriedade reenviando o mesmo lote.

### Segredos

As credenciais nunca entram no corpo do `FlinkDeployment` — um CR é legível por
qualquer um com `get` no namespace. A URI do MongoDB chega por `secretKeyRef`, e
as do S3 como `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` via cadeia padrão da
AWS.

Em produção, aponte `mongo.existingSecret` para um Secret gerenciado fora deste
repositório (External Secrets, Sealed Secrets, Vault) e deixe
`storage.s3.accessKeyName` vazio para usar IRSA/Workload Identity — assim nenhum
Secret de credencial é criado. Os valores em `tenants/*.yaml` existem para o
ambiente local e **não devem ser usados fora dele**.

---

## Testes

```bash
make lint       # normalização de nomes, isolamento, contrato catálogo<->chart
make test-e2e   # exige ambiente de pé
```

`tests/test-naming.sh` roda sem cluster e cobre os invariantes que falham em
silêncio: nome lógico trocado por slug (job assina tópico inexistente, fica
`RUNNING` e não consome nada), pipeline desabilitado que sobe assim mesmo,
placeholder de SQL sem variável correspondente.

Os testes unitários do runner (`runner/src/test/`) rodam no `docker build`.

---

## Próximos passos (fora do escopo atual)

- GitOps com ArgoCD `ApplicationSet` — caminho natural acima de ~30 tenants.
- `NetworkPolicy` entre namespaces de tenants.
- Autoscaler do operator: já há a flag `defaults.autoscaler.enabled`, desligada
  por padrão; requer validação de carga antes de habilitar.
- Migração para Flink 2.x, bloqueada pelo conector MongoDB.
