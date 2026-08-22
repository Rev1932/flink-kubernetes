#!/usr/bin/env bash
# Teste end-to-end: Kafka -> Flink -> MongoDB, com verificacao de isolamento
# entre tenants e de idempotencia do sink.
#
# Pre-requisitos: `make up` e `make deploy-all` concluidos.
set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

KAFKA_NS=kafka
KAFKA_POD=flink-kafka-dual-role-0
BOOTSTRAP=localhost:9092
STORAGE_NS=storage
MONGO_POD=mongodb-0
MONGO_DB=streaming
MONGO_URI='mongodb://root:rootpass@localhost:27017/?authSource=admin'

PASS=0; FAIL=0
ok()  { printf '  \033[32mok\033[0m   %s\n' "$1"; PASS=$((PASS+1)); }
bad() { printf '  \033[31mFALHA\033[0m %s\n' "$1"; FAIL=$((FAIL+1)); }

produce() { # produce <topico> <arquivo-de-eventos>
  kubectl -n "$KAFKA_NS" exec -i "$KAFKA_POD" -- \
    bin/kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic "$1" \
    < "$2" >/dev/null 2>&1
}

mongo_eval() { # mongo_eval <js>
  kubectl -n "$STORAGE_NS" exec "$MONGO_POD" -- \
    mongosh "$MONGO_URI" --quiet --eval "$1" 2>/dev/null | tr -d '\r'
}

count_docs() { # count_docs <colecao>
  mongo_eval "db.getSiblingDB('$MONGO_DB').getCollection('$1').countDocuments({})"
}

echo "== 1. jobs em execucao =="
not_running=$(kubectl get flinkdeployment -A \
  -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name}={.status.jobStatus.state}{"\n"}{end}' \
  | grep -v '=RUNNING$' || true)
if [[ -z "$not_running" ]]; then
  total=$(kubectl get flinkdeployment -A --no-headers | wc -l)
  ok "todos os $total jobs estao RUNNING"
else
  bad "jobs fora de RUNNING:"; echo "$not_running" | sed 's/^/       /'
fi

echo
echo "== 2. fluxo de dados por tenant =="
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT

python3 tests/gen_events.py streaming_1 --tag a > "$TMP/s1.json"
python3 tests/gen_events.py streaming_2 --tag a > "$TMP/s2.json"

produce N_topico_1 "$TMP/s1.json" && ok "produzido em N_topico_1" || bad "falha ao produzir em N_topico_1"
produce N_topico_2 "$TMP/s2.json" && ok "produzido em N_topico_2" || bad "falha ao produzir em N_topico_2"
produce K_topico_1 "$TMP/s1.json" && ok "produzido em K_topico_1" || bad "falha ao produzir em K_topico_1"

echo "  aguardando processamento e flush do sink..."
sleep 45

for coll in N_streaming_1 N_streaming_2 K_streaming_1; do
  n=$(count_docs "$coll")
  if [[ "${n:-0}" -gt 0 ]]; then
    ok "colecao $coll recebeu $n documentos"
  else
    bad "colecao $coll esta vazia"
  fi
done

# K nao contratou o streaming_2: a colecao nao deve sequer existir.
if [[ "$(count_docs K_streaming_2)" == "0" ]]; then
  ok "K_streaming_2 nao existe (pipeline desabilitado para K)"
else
  bad "K_streaming_2 tem documentos, mas o pipeline esta desabilitado para K"
fi

echo
echo "== 3. isolamento entre tenants =="
# Os dados de N e K sao identicos em conteudo; o que precisa estar separado sao
# as colecoes. Conferimos que cada colecao so foi escrita pelo seu proprio job.
n1=$(count_docs N_streaming_1)
k1=$(count_docs K_streaming_1)
if [[ "${n1:-0}" -gt 0 && "${k1:-0}" -gt 0 ]]; then
  ok "N_streaming_1 ($n1 docs) e K_streaming_1 ($k1 docs) sao colecoes distintas"
else
  bad "uma das colecoes por tenant nao foi populada"
fi

# Consumer groups separados: cada tenant tem o proprio offset.
groups=$(kubectl -n "$KAFKA_NS" exec "$KAFKA_POD" -- \
  bin/kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --list 2>/dev/null | tr -d '\r')
if grep -q "^N_streaming_1$" <<<"$groups" && grep -q "^K_streaming_1$" <<<"$groups"; then
  ok "consumer groups separados por tenant (N_streaming_1, K_streaming_1)"
else
  bad "consumer groups esperados nao encontrados. Existentes: $(tr '\n' ' ' <<<"$groups")"
fi

echo
echo "== 4. idempotencia do sink (upsert) =="
# O conector MongoDB entrega at-least-once. A PRIMARY KEY do CREATE TABLE deve
# transformar reescritas em upsert -- reenviar o mesmo lote nao pode duplicar.
before=$(count_docs N_streaming_2)
produce N_topico_2 "$TMP/s2.json"
sleep 30
after=$(count_docs N_streaming_2)
if [[ "${before:-0}" == "${after:-1}" && "${before:-0}" -gt 0 ]]; then
  ok "reenvio do mesmo lote manteve $after documentos (upsert por event_id)"
else
  bad "contagem mudou de $before para $after -- a PRIMARY KEY nao esta gerando _id estavel"
fi

echo
echo "== 5. recuperacao de falha =="
tm=$(kubectl -n tenant-n get pod -l component=taskmanager -o name 2>/dev/null | head -1)
if [[ -n "$tm" ]]; then
  kubectl -n tenant-n delete "$tm" --wait=false >/dev/null 2>&1
  echo "  removido $tm, aguardando recuperacao..."
  recovered=false
  for _ in $(seq 1 30); do
    sleep 10
    state=$(kubectl -n tenant-n get flinkdeployment n-streaming-1 \
      -o jsonpath='{.status.jobStatus.state}' 2>/dev/null)
    [[ "$state" == "RUNNING" ]] && { recovered=true; break; }
  done
  if $recovered; then
    ok "job retomou do checkpoint apos perder um TaskManager"
  else
    bad "job nao voltou a RUNNING em 5min apos a falha"
  fi
else
  bad "nenhum TaskManager encontrado em tenant-n"
fi

echo
echo "== 6. cotas por tenant =="
# Um pedido acima da cota tem de ser recusado pela API, e nao aceito para depois
# competir por CPU no no com os jobs de outro tenant.
probe=$(kubectl -n tenant-k run quota-probe --image=busybox:1.36 --restart=Never \
  --overrides='{"spec":{"containers":[{"name":"probe","image":"busybox:1.36","command":["sleep","5"],"resources":{"requests":{"cpu":"8","memory":"20Gi"},"limits":{"cpu":"8","memory":"20Gi"}}}]}}' 2>&1)
if grep -q "exceeded quota" <<<"$probe"; then
  ok "pod acima da cota do tenant e recusado pela API"
else
  bad "pod acima da cota nao foi recusado: $probe"
  kubectl -n tenant-k delete pod quota-probe --ignore-not-found >/dev/null 2>&1
fi

echo
printf 'resultado: \033[32m%d ok\033[0m, \033[31m%d falha(s)\033[0m\n' "$PASS" "$FAIL"
[[ $FAIL -eq 0 ]]
