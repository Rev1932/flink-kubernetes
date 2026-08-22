# Imagem unica usada por TODOS os jobs da plataforma.
#
# O que diferencia um job de outro nao e a imagem: e o script .sql montado por
# ConfigMap e as variaveis de ambiente injetadas pelo FlinkDeployment. Por isso
# adicionar um pipeline ou um tenant nao exige rebuild.

# --- build -----------------------------------------------------------------
# O build acontece dentro do Docker: a maquina de desenvolvimento nao precisa
# de JDK nem de Maven instalados.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app

# Camada de dependencias separada do codigo, para que alterar o SqlRunner nao
# invalide o cache do download do Maven.
COPY pom.xml .
RUN mvn -q -B -e dependency:go-offline

COPY src ./src
RUN mvn -q -B -e clean package

# --- runtime ---------------------------------------------------------------
FROM flink:1.20.2-java17

ARG KAFKA_CONNECTOR=flink-sql-connector-kafka-3.4.0-1.20.jar
ARG KAFKA_CONNECTOR_SHA1=266330f8f26fec4957339b5ff31aa67b81c31462
ARG MONGO_CONNECTOR=flink-sql-connector-mongodb-2.0.0-1.20.jar
ARG MONGO_CONNECTOR_SHA1=74cc0ba81f3eff74061dfb8031b1db5888e831e0
ARG MAVEN_BASE=https://repo1.maven.org/maven2/org/apache/flink

USER root

# Conectores SQL (uber-jars). O do MongoDB e o teto de versao da plataforma:
# so existe release ate Flink 1.20. Ver README, secao "Teto de versao".
RUN set -eux; \
    curl -fsSL -o /opt/flink/lib/${KAFKA_CONNECTOR} \
      "${MAVEN_BASE}/flink-sql-connector-kafka/3.4.0-1.20/${KAFKA_CONNECTOR}"; \
    echo "${KAFKA_CONNECTOR_SHA1}  /opt/flink/lib/${KAFKA_CONNECTOR}" | sha1sum -c -; \
    curl -fsSL -o /opt/flink/lib/${MONGO_CONNECTOR} \
      "${MAVEN_BASE}/flink-sql-connector-mongodb/2.0.0-1.20/${MONGO_CONNECTOR}"; \
    echo "${MONGO_CONNECTOR_SHA1}  /opt/flink/lib/${MONGO_CONNECTOR}" | sha1sum -c -

# O reporter Prometheus ja vem ativavel na imagem 1.20, em
# /opt/flink/plugins/metrics-prometheus/ -- basta configura-lo no
# flinkConfiguration, nada a copiar aqui.
#
# O filesystem S3, por outro lado, vem em /opt/flink/opt (inativo). Filesystems
# sao carregados como plugins isolados, entao precisa de diretorio proprio em
# plugins/ -- copiar para lib/ nao funciona.
RUN mkdir -p /opt/flink/plugins/s3-fs-presto \
    && cp /opt/flink/opt/flink-s3-fs-presto-*.jar /opt/flink/plugins/s3-fs-presto/

# JAR do runner.
RUN mkdir -p /opt/flink/usrlib
COPY --from=build /app/target/sql-runner.jar /opt/flink/usrlib/sql-runner.jar

# Diretorio onde o ConfigMap com o .sql do pipeline e montado em runtime.
RUN mkdir -p /opt/flink/sql && chown -R flink:flink /opt/flink/usrlib /opt/flink/sql

USER flink
