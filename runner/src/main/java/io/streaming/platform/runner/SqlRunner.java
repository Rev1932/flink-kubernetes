/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.streaming.platform.runner;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executa um script Flink SQL parametrizado por variaveis de ambiente.
 *
 * <p>Derivado de {@code org.apache.flink.examples.SqlRunner}, do projeto
 * apache/flink-kubernetes-operator (Apache License 2.0). O parser de statements
 * -- incluindo o tratamento de {@code SET 'k' = 'v';}, de comentarios e de
 * blocos {@code EXECUTE STATEMENT SET ... END;} -- e mantido igual ao upstream.
 *
 * <p>A adicao desta plataforma e a <b>interpolacao de variaveis</b>: o mesmo
 * arquivo .sql serve a todos os tenants, e os placeholders {@code ${VAR}} sao
 * resolvidos a partir do ambiente do pod. E isso que materializa a padronizacao
 * N x K -- o pipeline {@code streaming_1} vira {@code N_streaming_1} lendo
 * {@code N_topico_1} para o tenant N, e {@code K_streaming_1} lendo
 * {@code K_topico_1} para o tenant K, sem duplicar SQL.
 *
 * <p>Uso: {@code SqlRunner <caminho-do-script.sql>}
 */
public class SqlRunner {

    private static final Logger LOG = LoggerFactory.getLogger(SqlRunner.class);

    private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
    private static final String LINE_DELIMITER = "\n";

    private static final String COMMENT_PATTERN = "(--.*)|(((\\/\\*)+?[\\w\\W]+?(\\*\\/)+))";

    private static final Pattern SET_STATEMENT_PATTERN =
            Pattern.compile("SET\\s+'(\\S+)'\\s+=\\s+'(.*)';", Pattern.CASE_INSENSITIVE);

    private static final String BEGIN_CERTIFICATE = "-----BEGIN CERTIFICATE-----";
    private static final String END_CERTIFICATE = "-----END CERTIFICATE-----";
    private static final String ESCAPED_BEGIN_CERTIFICATE = "======BEGIN CERTIFICATE=====";
    private static final String ESCAPED_END_CERTIFICATE = "=====END CERTIFICATE=====";

    /**
     * Placeholders no formato {@code ${NOME}} ou {@code ${NOME:-default}}.
     *
     * <p>O nome aceita apenas letras maiusculas, digitos e underscore, para nao
     * colidir com sintaxe SQL legitima que use chaves.
     */
    private static final Pattern VARIABLE_PATTERN =
            Pattern.compile("\\$\\{([A-Z_][A-Z0-9_]*)(?::-([^}]*))?}");

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new Exception("Exactly one argument is expected: the path to the SQL script.");
        }

        var rawScript = FileUtils.readFileUtf8(new File(args[0]));
        var script = resolveVariables(rawScript, System.getenv());
        var statements = parseStatements(script);

        var tableEnv = TableEnvironment.create(new Configuration());

        for (String statement : statements) {
            Matcher setMatcher = SET_STATEMENT_PATTERN.matcher(statement.trim());

            if (setMatcher.matches()) {
                // Handle SET statements
                String key = setMatcher.group(1);
                String value = setMatcher.group(2);
                LOG.info("Setting configuration: {} = {}", key, value);
                tableEnv.getConfig().getConfiguration().setString(key, value);
            } else {
                LOG.info("Executing:\n{}", statement);
                tableEnv.executeSql(statement);
            }
        }
    }

    /**
     * Substitui {@code ${VAR}} pelo valor correspondente em {@code environment}.
     *
     * <p>Uma variavel sem valor e sem default e erro fatal, e nao string vazia.
     * Falhar aqui e deliberado: um {@code ${TENANT}} nao resolvido produziria um
     * job que assina o topico literal {@code "${TENANT}_topico_1"} -- que nao
     * existe. O job subiria como RUNNING e ficaria em silencio para sempre, um
     * modo de falha bem pior do que um crash na inicializacao.
     *
     * <p>Todas as variaveis ausentes sao coletadas antes de lancar, para que o
     * operador veja de uma vez tudo o que falta em vez de descobrir uma por
     * restart.
     */
    static String resolveVariables(String script, Map<String, String> environment) {
        var missing = new LinkedHashSet<String>();
        var matcher = VARIABLE_PATTERN.matcher(script);
        var result = new StringBuilder();

        while (matcher.find()) {
            var name = matcher.group(1);
            var fallback = matcher.group(2);
            var value = environment.get(name);

            if (value == null || value.isEmpty()) {
                if (fallback == null) {
                    missing.add(name);
                    // Mantem o texto original: se houver outras faltando, todas
                    // aparecem no erro final.
                    matcher.appendReplacement(result, Matcher.quoteReplacement(matcher.group(0)));
                    continue;
                }
                value = fallback;
            }
            matcher.appendReplacement(result, Matcher.quoteReplacement(value));
        }
        matcher.appendTail(result);

        if (!missing.isEmpty()) {
            throw new IllegalStateException(
                    "Variaveis nao definidas no ambiente do pod: "
                            + String.join(", ", missing)
                            + ". Verifique o podTemplate do FlinkDeployment "
                            + "(charts/flink-pipeline/templates/flinkdeployment.yaml) "
                            + "e o campo `env` do pipeline.");
        }

        return result.toString();
    }

    /** Nomes de variaveis referenciados por um script. Usado nos testes. */
    static Set<String> referencedVariables(String script) {
        var names = new LinkedHashSet<String>();
        var matcher = VARIABLE_PATTERN.matcher(script);
        while (matcher.find()) {
            names.add(matcher.group(1));
        }
        return names;
    }

    public static List<String> parseStatements(String script) {
        var formatted =
                formatSqlFile(script)
                        .replaceAll(BEGIN_CERTIFICATE, ESCAPED_BEGIN_CERTIFICATE)
                        .replaceAll(END_CERTIFICATE, ESCAPED_END_CERTIFICATE)
                        .replaceAll(COMMENT_PATTERN, "")
                        .replaceAll(ESCAPED_BEGIN_CERTIFICATE, BEGIN_CERTIFICATE)
                        .replaceAll(ESCAPED_END_CERTIFICATE, END_CERTIFICATE);

        var statements = new ArrayList<String>();

        StringBuilder current = null;
        boolean statementSet = false;
        for (String line : formatted.split("\n")) {
            var trimmed = line.trim();
            if (trimmed.isBlank()) {
                continue;
            }
            if (current == null) {
                current = new StringBuilder();
            }
            if (trimmed.startsWith("EXECUTE STATEMENT SET")) {
                statementSet = true;
            }
            current.append(trimmed);
            current.append("\n");
            if (trimmed.endsWith(STATEMENT_DELIMITER)) {
                if (!statementSet || trimmed.equals("END;")) {
                    statements.add(current.toString());
                    current = null;
                    statementSet = false;
                }
            }
        }
        return statements;
    }

    public static String formatSqlFile(String content) {
        String trimmed = content.trim();
        StringBuilder formatted = new StringBuilder();
        formatted.append(trimmed);
        if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
            formatted.append(STATEMENT_DELIMITER);
        }
        formatted.append(LINE_DELIMITER);
        return formatted.toString();
    }
}
