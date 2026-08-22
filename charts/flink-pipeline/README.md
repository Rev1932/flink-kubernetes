# Chart `flink-pipeline`

Uma release deste chart = **um tenant**. Cada pipeline habilitado vira um
`FlinkDeployment` em application mode, com ConfigMap de SQL, tópicos Kafka,
cota, RBAC, PodMonitor e alertas próprios.

```bash
helm upgrade --install tenant-n . -f ../../tenants/tenant-n.yaml \
  --namespace tenant-n --create-namespace
```

## Precedência de configuração

Três camadas, da mais genérica para a mais específica:

| Ordem | Origem | Exemplo |
|---|---|---|
| 1 | `values.yaml` → `defaults` | padrões da plataforma (parallelism 2, JM 0.5 CPU) |
| 2 | `pipelines/<nome>/pipeline.yaml` → `defaults` | padrões do pipeline (`upgradeMode`, memória) |
| 3 | `tenants/<tenant>.yaml` → `pipelines.<nome>` | override do tenant (parallelism 4) |

Um tenant declara apenas o que difere. Mudar o padrão de um pipeline no catálogo
vale para todos os tenants que não o sobrescreveram.

## Valores principais

| Chave | Descrição |
|---|---|
| `tenant.id` | Identificador **lógico**, entre aspas (`"N"`). Prefixo de tópicos e coleções. |
| `pipelines.<nome>.enabled` | Habilita o pipeline para este tenant. |
| `kafka.createTopics` | `true` no local (cria `KafkaTopic`); `false` onde os tópicos já existem. |
| `mongo.existingSecret` | **Produção**: Secret externo com a URI. Preferir sempre a esta forma. |
| `mongo.uri` | Desenvolvimento: gera o Secret a partir de values. |
| `storage.s3.existingSecret` | Credenciais do S3. Vazio + `accessKeyName` vazio = IRSA. |
| `quota.*` | Teto de CPU/memória/pods do namespace do tenant. |
| `defaults.executionMode` | Hoje só `application`. Ponto de extensão para session clusters. |

## Nomes: o cuidado central

`templates/_helpers.tpl` mantém separados:

- `tenant.id` → **lógico**, preservado (`N` → `N_topico_1`, `N_streaming_1`)
- `tenant.slug` → **recurso**, DNS-1123 (`n` → `n-streaming-1`)

Trocar um pelo outro é a falha mais provável ao editar este chart, e a mais
perigosa: um nome lógico incorreto faz o job assinar um tópico inexistente e
ficar `RUNNING` sem consumir nada. `tests/test-naming.sh` cobre esses casos.

`tenant.id` **deve** ser citado. Sem aspas, o YAML lê `N` como o booleano
`false` — o chart detecta e recusa.

## Adicionar um pipeline ao catálogo

Crie `pipelines/<nome>/` com `pipeline.sql` (só placeholders, nunca nomes
literais de tenant) e `pipeline.yaml` (`sources`, `stateful`, `requiredVars`,
`defaults`). Depois habilite nos tenants. Rode `make lint` na raiz para validar
que todo placeholder tem variável de ambiente correspondente.
