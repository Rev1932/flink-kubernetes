-- =========================================================================
-- streaming_2 -- transformacao evento a evento, sem estado.
--
-- Mesmo contrato de nomes do streaming_1:
--   tenant N -> le N_topico_2, escreve na colecao N_streaming_2
--   tenant K -> le K_topico_2, escreve na colecao K_streaming_2
-- =========================================================================

SET 'pipeline.name' = '${JOB_NAME}';

CREATE TABLE source_transactions (
    event_id    STRING,
    user_id     STRING,
    amount      DECIMAL(10, 2),
    currency    STRING,
    event_time  TIMESTAMP(3),
    -- Metadados do Kafka ajudam a rastrear a origem de um documento no MongoDB
    -- ate a mensagem exata que o gerou.
    `partition`  INT      METADATA FROM 'partition'   VIRTUAL,
    `offset`     BIGINT   METADATA FROM 'offset'      VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = '${TENANT}_topico_2',
    'properties.bootstrap.servers' = '${KAFKA_BOOTSTRAP}',
    'properties.group.id' = '${JOB_NAME}',
    'scan.startup.mode' = '${STARTUP_MODE:-group-offsets}',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE sink_transactions (
    event_id     STRING,
    user_id      STRING,
    amount       DECIMAL(10, 2),
    currency     STRING,
    faixa        STRING,
    event_time   TIMESTAMP(3),
    source_partition INT,
    source_offset    BIGINT,
    -- event_id como chave: reprocessar a mesma mensagem sobrescreve o documento
    -- em vez de criar um duplicado.
    PRIMARY KEY (event_id) NOT ENFORCED
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

INSERT INTO sink_transactions
SELECT
    event_id,
    user_id,
    amount,
    COALESCE(currency, 'BRL') AS currency,
    CASE
        WHEN amount >= 10000 THEN 'alto'
        WHEN amount >= 1000  THEN 'medio'
        ELSE 'baixo'
    END AS faixa,
    event_time,
    `partition` AS source_partition,
    `offset`    AS source_offset
FROM source_transactions
WHERE user_id IS NOT NULL
  AND amount >= CAST('${MIN_AMOUNT:-100.00}' AS DECIMAL(10, 2));
