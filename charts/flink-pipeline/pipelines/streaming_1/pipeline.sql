-- =========================================================================
-- streaming_1 -- agregacao em janela de 1 minuto por usuario.
--
-- Este arquivo e IDENTICO para todos os tenants. Os placeholders ${...} sao
-- resolvidos em runtime pelo SqlRunner a partir das variaveis de ambiente que
-- o FlinkDeployment injeta, de modo que:
--
--   tenant N -> le N_topico_1, escreve na colecao N_streaming_1
--   tenant K -> le K_topico_1, escreve na colecao K_streaming_1
--
-- Alterar este arquivo e versionar o pipeline para todos os tenants de uma vez.
-- =========================================================================

SET 'pipeline.name' = '${JOB_NAME}';

-- Origem -------------------------------------------------------------------
CREATE TABLE source_events (
    event_id    STRING,
    user_id     STRING,
    amount      DECIMAL(10, 2),
    event_time  TIMESTAMP(3),
    -- Tolera ate 5s de desordem antes de fechar a janela. Eventos mais atrasados
    -- que isso sao descartados pela janela ja fechada.
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = '${TENANT}_topico_1',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    -- Consumer group por job: dois tenants nunca competem pelas mesmas particoes.
    'properties.group.id' = '${JOB_NAME}',
    -- group-offsets retoma de onde parou; o checkpoint do Flink continua sendo a
    -- fonte de verdade dos offsets quando ha estado.
    'scan.startup.mode' = '${STARTUP_MODE:-group-offsets}',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    -- Uma mensagem malformada nao deve derrubar o pipeline inteiro do tenant.
    'json.ignore-parse-errors' = 'true'
);

-- Destino ------------------------------------------------------------------
CREATE TABLE sink_aggregates (
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    user_id       STRING,
    total_amount  DECIMAL(20, 2),
    event_count   BIGINT,
    -- A PRIMARY KEY e o que faz o conector operar em modo upsert, derivando o
    -- _id do MongoDB a partir destas colunas. Sem ela o conector so faz INSERT
    -- e, como a entrega e at-least-once, um restart duplicaria as janelas ja
    -- gravadas.
    PRIMARY KEY (window_start, user_id) NOT ENFORCED
) WITH (
    'connector' = 'mongodb',
    'uri' = '${MONGO_URI}',
    'database' = '${MONGO_DB}',
    'collection' = '${JOB_NAME}',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '1s',
    'sink.delivery-guarantee' = 'at-least-once',
    'sink.max-retries' = '3'
);

-- Transformacao ------------------------------------------------------------
INSERT INTO sink_aggregates
SELECT
    window_start,
    window_end,
    user_id,
    SUM(amount)  AS total_amount,
    COUNT(*)     AS event_count
FROM TABLE(
    TUMBLE(TABLE source_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
WHERE user_id IS NOT NULL
GROUP BY window_start, window_end, user_id;
