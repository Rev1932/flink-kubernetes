#!/usr/bin/env bash
# Testa a normalizacao de nomes do chart. Nao precisa de cluster.
#
# Estes invariantes sao a parte da plataforma que falha em silencio quando
# quebra: um nome logico trocado por um slug faz o job assinar um topico
# inexistente e ficar RUNNING sem consumir nada. Por isso sao testados, e nao
# apenas revisados.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

CHART=charts/flink-pipeline
PASS=0
FAIL=0
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

ok()   { printf '  \033[32mok\033[0m   %s\n' "$1"; PASS=$((PASS+1)); }
bad()  { printf '  \033[31mFALHA\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }

render() { # render <values-file> <namespace> <out>
  helm template "$2" "$CHART" -f "$1" --namespace "$2" > "$3" 2>"$3.err"
}

echo "== normalizacao de nomes =="

# ---------------------------------------------------------------------------
render tenants/tenant-n.yaml tenant-n "$TMP/n.yaml"
if [[ $? -ne 0 ]]; then
  bad "tenant-n deveria renderizar"; cat "$TMP/n.yaml.err"; exit 1
fi

# Regressao: o slug do tenant N e a letra `n`, que o YAML 1.1 le como booleano.
# `tenant: n` sem aspas vira `tenant: false` e a API recusa o objeto -- mas
# `helm template` passa, entao so um teste de tipos pega isso antes do install.
if out=$(python3 tests/check_yaml_types.py "$TMP/n.yaml" 2>&1); then
  ok "labels/annotations sao todos string (nenhum virou booleano)"
else
  bad "$out"
fi

# Nome de recurso Kubernetes: DNS-1123 estrito.
invalid=$(grep -E "^  name: " "$TMP/n.yaml" | awk '{print $2}' \
          | grep -vE "^[a-z]([-a-z0-9]*[a-z0-9])?$" || true)
if [[ -z "$invalid" ]]; then
  ok "todo metadata.name e DNS-1123"
else
  bad "nomes de recurso invalidos: $invalid"
fi

# Nome logico preservado onde importa: topico Kafka.
if grep -q 'topicName: "N_topico_1"' "$TMP/n.yaml"; then
  ok "topico Kafka preserva o nome logico N_topico_1"
else
  bad "topicName deveria ser N_topico_1 (encontrado: $(grep -o 'topicName:.*' "$TMP/n.yaml" | head -1))"
fi

# O recurso KafkaTopic correspondente usa o slug.
if grep -q "name: n-topico-1" "$TMP/n.yaml"; then
  ok "recurso do topico usa o slug n-topico-1"
else
  bad "recurso KafkaTopic deveria chamar-se n-topico-1"
fi

# A colecao MongoDB e o consumer group vem do nome logico, via env JOB_NAME.
if grep -q 'value: "N_streaming_1"' "$TMP/n.yaml"; then
  ok "JOB_NAME logico N_streaming_1 chega ao pod"
else
  bad "env JOB_NAME deveria ser N_streaming_1"
fi

# E o FlinkDeployment usa o slug.
if grep -q "name: n-streaming-1" "$TMP/n.yaml"; then
  ok "FlinkDeployment usa o slug n-streaming-1"
else
  bad "FlinkDeployment deveria chamar-se n-streaming-1"
fi

# ---------------------------------------------------------------------------
echo "== isolamento entre tenants =="

render tenants/tenant-k.yaml tenant-k "$TMP/k.yaml"

# Nada do tenant N pode vazar para os manifestos do tenant K.
#
# A verificacao e estrutural (check_isolation.py) e nao um grep: o .sql do
# catalogo e compartilhado e cita os dois tenants nos comentarios de exemplo,
# o que faria um grep textual acusar vazamento inexistente.
if out=$(python3 tests/check_isolation.py "$TMP/k.yaml" K N 2>&1); then
  ok "manifestos de K nao referenciam o tenant N"
else
  bad "isolamento K<-N: $out"
fi
if out=$(python3 tests/check_isolation.py "$TMP/n.yaml" N K 2>&1); then
  ok "manifestos de N nao referenciam o tenant K"
else
  bad "isolamento N<-K: $out"
fi

# streaming_2 esta explicitamente desabilitado para K.
# Regressao: `default true $cfg.enabled` lia `false` como `true`, porque no Helm
# `false` conta como valor vazio.
if ! grep -qE "streaming.2|topico.2" "$TMP/k.yaml"; then
  ok "pipeline com enabled:false nao gera recurso algum"
else
  bad "streaming_2 esta desabilitado para K mas gerou recursos"
fi

# ---------------------------------------------------------------------------
echo "== contrato catalogo <-> chart =="

# Toda variavel declarada em requiredVars precisa existir como env no pod. Sem
# isso o SqlRunner so descobriria a falta em runtime, apos o deploy.
for pipe in streaming_1 streaming_2; do
  missing=""
  for var in $(grep -A20 '^requiredVars:' "$CHART/pipelines/$pipe/pipeline.yaml" \
               | grep -E '^\s+- ' | sed 's/[ -]//g'); do
    grep -q "name: $var" "$TMP/n.yaml" || missing="$missing $var"
  done
  if [[ -z "$missing" ]]; then
    ok "$pipe: todas as requiredVars sao injetadas no pod"
  else
    bad "$pipe: variaveis declaradas mas nao injetadas:$missing"
  fi
done

# Todo placeholder ${VAR} usado no SQL precisa ter env correspondente ou default.
for pipe in streaming_1 streaming_2; do
  missing=""
  for var in $(grep -oE '\$\{[A-Z_][A-Z0-9_]*(:-[^}]*)?\}' "$CHART/pipelines/$pipe/pipeline.sql" \
               | grep -v ':-' | tr -d '${}' | sort -u); do
    grep -q "name: $var" "$TMP/n.yaml" || missing="$missing $var"
  done
  if [[ -z "$missing" ]]; then
    ok "$pipe: todo placeholder obrigatorio do SQL tem env correspondente"
  else
    bad "$pipe: placeholders sem env (job falharia ao subir):$missing"
  fi
done

# ---------------------------------------------------------------------------
echo "== erros de configuracao devem falhar cedo =="

# Regressao: `tenant.id: N` sem aspas vira booleano `false` em YAML 1.1.
cat > "$TMP/bool.yaml" <<'EOF'
tenant:
  id: N
mongo:
  uri: mongodb://x
pipelines:
  streaming_1: { enabled: true }
EOF
if helm template t "$CHART" -f "$TMP/bool.yaml" --namespace t >/dev/null 2>"$TMP/bool.err"; then
  bad "tenant.id nao citado deveria falhar (YAML lê N como false)"
elif grep -q "booleano" "$TMP/bool.err"; then
  ok "tenant.id nao citado falha com mensagem explicativa"
else
  bad "falhou, mas sem explicar a causa: $(head -1 "$TMP/bool.err")"
fi

# Um id que nao produz slug valido deve ser recusado na renderizacao.
cat > "$TMP/badid.yaml" <<'EOF'
tenant:
  id: "9-tenant!"
mongo:
  uri: mongodb://x
pipelines:
  streaming_1: { enabled: true }
EOF
if helm template t "$CHART" -f "$TMP/badid.yaml" --namespace t >/dev/null 2>"$TMP/badid.err"; then
  bad "tenant.id invalido deveria falhar"
else
  ok "tenant.id que nao vira slug DNS-1123 e recusado"
fi

# Pipeline inexistente no catalogo: erro de digitacao em tenants/*.yaml.
cat > "$TMP/nopipe.yaml" <<'EOF'
tenant:
  id: "Z"
mongo:
  uri: mongodb://x
pipelines:
  streaming_999: { enabled: true }
EOF
if helm template t "$CHART" -f "$TMP/nopipe.yaml" --namespace t >/dev/null 2>"$TMP/nopipe.err"; then
  bad "pipeline fora do catalogo deveria falhar"
else
  ok "pipeline inexistente no catalogo e recusado"
fi

# Tenant com underscore/maiuscula no id: o caso que motivou os helpers.
cat > "$TMP/underscore.yaml" <<'EOF'
tenant:
  id: "Tenant_ACME"
mongo:
  uri: mongodb://x
pipelines:
  streaming_1: { enabled: true }
EOF
if helm template t "$CHART" -f "$TMP/underscore.yaml" --namespace t > "$TMP/us.yaml" 2>&1; then
  if grep -q "name: tenant-acme-streaming-1" "$TMP/us.yaml" \
     && grep -q 'topicName: "Tenant_ACME_topico_1"' "$TMP/us.yaml"; then
    ok "id com underscore/maiuscula: recurso vira slug, topico preserva o literal"
  else
    bad "id com underscore normalizado incorretamente"
  fi
else
  bad "id com underscore deveria renderizar: $(head -2 "$TMP/us.yaml")"
fi

# ---------------------------------------------------------------------------
echo
printf 'resultado: \033[32m%d ok\033[0m, \033[31m%d falha(s)\033[0m\n' "$PASS" "$FAIL"
[[ $FAIL -eq 0 ]]
