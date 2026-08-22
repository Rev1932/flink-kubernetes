{{/*
===========================================================================
Normalizacao de nomes -- o ponto mais delicado deste chart.

Convivem aqui dois espacos de nomes que NAO podem ser confundidos:

  1. Nome LOGICO   -- `N`, `N_topico_1`, `N_streaming_1`
     Vai para topicos Kafka, colecoes MongoDB e pipeline.name.
     Preserva maiusculas e underscore exatamente como o contrato exige.

  2. Nome de RECURSO -- `n`, `n-streaming-1`
     Vai para metadata.name de objetos Kubernetes.
     Precisa ser DNS-1123: minusculo, [a-z0-9-], comecando por letra.

Usar o slug onde se espera o nome logico faz o job assinar um topico que nao
existe (falha silenciosa: job RUNNING, zero registros). Usar o nome logico onde
se espera o slug faz a API do Kubernetes rejeitar o objeto (falha barulhenta).
O primeiro caso e o perigoso -- dai os helpers separados e o teste em
tests/test-naming.sh.
===========================================================================
*/}}

{{/*
Identificador logico do tenant, ex.: "N".

A checagem de booleano nao e paranoia: em YAML 1.1 os escalares nao citados
`N`, `Y`, `n`, `y`, `no`, `yes`, `on`, `off` sao BOOLEANOS. Escrever
`tenant.id: N` produz `false`, e nao a letra "N" -- exatamente os
identificadores deste projeto. Sem esta guarda o tenant N geraria topicos
chamados `false_topico_1`. Sempre citar: `tenant.id: "N"`.
*/}}
{{- define "tenant.id" -}}
{{- $raw := .Values.tenant.id -}}
{{- if kindIs "bool" $raw -}}
{{- fail "tenant.id foi lido como booleano. Em YAML, N/Y/no/yes/on/off sem aspas viram true/false. Escreva entre aspas: tenant.id: \"N\"" -}}
{{- end -}}
{{- $id := ternary "" ($raw | toString) (kindIs "invalid" $raw) -}}
{{- if eq $id "" -}}
{{- fail "tenant.id e obrigatorio (ex.: tenant.id: \"N\" para gerar N_topico_1 / N_streaming_1)" -}}
{{- end -}}
{{- $id -}}
{{- end -}}

{{/* Slug DNS-1123 do tenant, ex.: "n". */}}
{{- define "tenant.slug" -}}
{{- include "platform.slugify" (include "tenant.id" .) -}}
{{- end -}}

{{/*
Converte um identificador logico em um slug DNS-1123.
Falha explicitamente se o resultado ainda for invalido, em vez de deixar o
Kubernetes rejeitar o manifesto com uma mensagem sem contexto.
*/}}
{{- define "platform.slugify" -}}
{{- $slug := . | toString | lower | replace "_" "-" | replace "." "-" | trim -}}
{{- if not (regexMatch "^[a-z]([-a-z0-9]*[a-z0-9])?$" $slug) -}}
{{- fail (printf "nome '%s' nao produz um slug DNS-1123 valido (obtido: '%s'). Use apenas letras, digitos e underscore, comecando por letra." . $slug) -}}
{{- end -}}
{{- $slug -}}
{{- end -}}

{{/*
Nome logico do job: <TENANT>_<pipeline>, ex.: "N_streaming_1".
E tambem o nome da colecao MongoDB de destino e do consumer group Kafka.
Recebe: dict com root=$ e pipeline=<nome do pipeline>
*/}}
{{- define "job.logicalName" -}}
{{- printf "%s_%s" (include "tenant.id" .root) .pipeline -}}
{{- end -}}

{{/*
Nome do recurso Kubernetes: <slug-tenant>-<slug-pipeline>, ex.: "n-streaming-1".
Recebe: dict com root=$ e pipeline=<nome do pipeline>
*/}}
{{- define "job.resourceName" -}}
{{- printf "%s-%s" (include "tenant.slug" .root) (include "platform.slugify" .pipeline) -}}
{{- end -}}

{{/*
===========================================================================
Resolucao da configuracao de um pipeline.

Tres camadas, da mais generica para a mais especifica:
  1. .Values.defaults              -- padroes da plataforma
  2. pipelines/<nome>/pipeline.yaml -- padroes do catalogo (defaults:)
  3. .Values.pipelines.<nome>       -- override do tenant

Assim um tenant so declara o que difere; alterar o padrao de um pipeline no
catalogo vale para todos os tenants que nao o sobrescreveram.
Recebe: dict com root=$ e pipeline=<nome>
Retorna: o dict resolvido (via toYaml, para ser consumido com fromYaml).
===========================================================================
*/}}
{{- define "pipeline.config" -}}
{{- $catalog := include "pipeline.catalog" (dict "root" .root "pipeline" .pipeline) | fromYaml -}}
{{- $merged := deepCopy .root.Values.defaults -}}
{{- $merged = mergeOverwrite $merged (default (dict) $catalog.defaults) -}}
{{- $merged = mergeOverwrite $merged (default (dict) (get .root.Values.pipelines .pipeline)) -}}
{{- toYaml $merged -}}
{{- end -}}

{{/*
Le pipelines/<nome>/pipeline.yaml do catalogo.
Falha se o pipeline referenciado pelo tenant nao existir -- sem isso, um erro de
digitacao em tenants/*.yaml geraria um job sem SQL.
*/}}
{{- define "pipeline.catalog" -}}
{{- $path := printf "pipelines/%s/pipeline.yaml" .pipeline -}}
{{- $raw := .root.Files.Get $path -}}
{{- if not $raw -}}
{{- fail (printf "pipeline '%s' nao existe no catalogo (esperado: charts/flink-pipeline/%s)" .pipeline $path) -}}
{{- end -}}
{{- $raw -}}
{{- end -}}

{{/*
Le pipelines/<nome>/pipeline.sql do catalogo.
*/}}
{{- define "pipeline.sql" -}}
{{- $path := printf "pipelines/%s/pipeline.sql" .pipeline -}}
{{- $sql := .root.Files.Get $path -}}
{{- if not $sql -}}
{{- fail (printf "SQL do pipeline '%s' nao encontrado (esperado: charts/flink-pipeline/%s)" .pipeline $path) -}}
{{- end -}}
{{- $sql -}}
{{- end -}}

{{/*
Lista ordenada dos pipelines habilitados para este tenant.

Nao usar `default true $cfg.enabled` aqui: no Helm, `default` devolve o padrao
sempre que o valor e "vazio", e `false` conta como vazio. `enabled: false`
seria lido como `true` e o pipeline desabilitado subiria assim mesmo. Por isso
a presenca da chave e testada explicitamente.
*/}}
{{- define "tenant.enabledPipelines" -}}
{{- $names := list -}}
{{- range $name, $cfg := .Values.pipelines -}}
{{- $enabled := true -}}
{{- if hasKey $cfg "enabled" -}}
{{- $enabled = $cfg.enabled -}}
{{- end -}}
{{- if $enabled -}}
{{- $names = append $names $name -}}
{{- end -}}
{{- end -}}
{{- toYaml (sortAlpha $names) -}}
{{- end -}}

{{/*
Labels comuns a todos os objetos do tenant.

Os valores vao SEMPRE entre aspas. Um slug de uma letra como `n` ou `y` e um
booleano em YAML 1.1: sem as aspas, `tenant: n` chega a API do Kubernetes como
`tenant: false` e o objeto e rejeitado com "cannot unmarshal bool into
ObjectMeta.labels". O mesmo motivo pelo qual tenant.id precisa ser citado.
*/}}
{{- define "platform.labels" -}}
app.kubernetes.io/name: flink-pipeline
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
app.kubernetes.io/part-of: streaming-platform
tenant: {{ include "tenant.slug" . | quote }}
{{- end -}}

{{/*
Labels de um job. `tenant` e `pipeline` sao os slugs (labels do Kubernetes nao
aceitam maiuscula em valores... na verdade aceitam, mas manter o slug garante
consistencia com o seletor do PodMonitor). Os nomes logicos ficam em anotacoes.
Recebe: dict com root=$ e pipeline=<nome>
*/}}
{{- define "job.labels" -}}
{{ include "platform.labels" .root }}
pipeline: {{ include "platform.slugify" .pipeline | quote }}
{{- end -}}
