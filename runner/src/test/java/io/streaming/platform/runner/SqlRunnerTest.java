/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.streaming.platform.runner;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cobre a interpolacao de variaveis -- a parte que esta plataforma adiciona
 * sobre o SqlRunner upstream. O parser de statements herdado nao e retestado
 * aqui; o que importa e que um placeholder nao resolvido nunca chegue ao Flink.
 */
class SqlRunnerTest {

    @Test
    void substituiVariaveisDoAmbiente() {
        var script = "CREATE TABLE t WITH ('topic' = '${TENANT}_topico_1');";
        var out = SqlRunner.resolveVariables(script, Map.of("TENANT", "N"));
        assertEquals("CREATE TABLE t WITH ('topic' = 'N_topico_1');", out);
    }

    @Test
    void mesmoScriptGeraNomesDiferentesPorTenant() {
        var script = "'topic' = '${TENANT}_topico_1', 'collection' = '${TENANT}_streaming_1'";

        var n = SqlRunner.resolveVariables(script, Map.of("TENANT", "N"));
        var k = SqlRunner.resolveVariables(script, Map.of("TENANT", "K"));

        assertTrue(n.contains("N_topico_1") && n.contains("N_streaming_1"));
        assertTrue(k.contains("K_topico_1") && k.contains("K_streaming_1"));
    }

    @Test
    void variavelAusenteFalhaEmVezDeProduzirTopicoLiteral() {
        var script = "'topic' = '${TENANT}_topico_1'";
        var erro =
                assertThrows(
                        IllegalStateException.class,
                        () -> SqlRunner.resolveVariables(script, Map.of()));
        assertTrue(erro.getMessage().contains("TENANT"));
    }

    @Test
    void variavelVaziaContaComoAusente() {
        // Um env var declarado mas vazio e quase sempre um Secret mal montado.
        var script = "'uri' = '${MONGO_URI}'";
        assertThrows(
                IllegalStateException.class,
                () -> SqlRunner.resolveVariables(script, Map.of("MONGO_URI", "")));
    }

    @Test
    void reportaTodasAsVariaveisAusentesDeUmaVez() {
        var script = "'a'='${ALPHA}' 'b'='${BETA}' 'c'='${GAMMA}'";
        var erro =
                assertThrows(
                        IllegalStateException.class,
                        () -> SqlRunner.resolveVariables(script, Map.of("BETA", "x")));

        assertTrue(erro.getMessage().contains("ALPHA"));
        assertTrue(erro.getMessage().contains("GAMMA"));
    }

    @Test
    void aplicaValorPadraoQuandoDeclarado() {
        var script = "'scan.startup.mode' = '${STARTUP_MODE:-group-offsets}'";
        var out = SqlRunner.resolveVariables(script, Map.of());
        assertEquals("'scan.startup.mode' = 'group-offsets'", out);
    }

    @Test
    void ambienteTemPrecedenciaSobreOPadrao() {
        var script = "'${MODE:-group-offsets}'";
        var out = SqlRunner.resolveVariables(script, Map.of("MODE", "earliest-offset"));
        assertEquals("'earliest-offset'", out);
    }

    @Test
    void naoConfundeSintaxeSqlComPlaceholder() {
        // Chaves em SQL legitimo (ex.: MAP) nao devem ser tocadas.
        var script = "SELECT MAP['k', v] FROM t WHERE x = '{nao_e_placeholder}';";
        assertEquals(script, SqlRunner.resolveVariables(script, Map.of()));
    }

    @Test
    void valorComCifraoNaoQuebraASubstituicao() {
        // Senhas com $ dentro da URI do Mongo passavam por Matcher.appendReplacement,
        // onde $ tem significado especial -- dai o quoteReplacement.
        var script = "'uri' = '${MONGO_URI}'";
        var uri = "mongodb://user:p$a$$w0rd@host:27017";
        var out = SqlRunner.resolveVariables(script, Map.of("MONGO_URI", uri));
        assertEquals("'uri' = '" + uri + "'", out);
    }

    @Test
    void listaVariaveisReferenciadas() {
        var script = "'${TENANT}' '${KAFKA_BOOTSTRAP}' '${TENANT}' '${OPT:-x}'";
        assertEquals(
                java.util.Set.of("TENANT", "KAFKA_BOOTSTRAP", "OPT"),
                SqlRunner.referencedVariables(script));
    }

    @Test
    void blocoStatementSetPermaneceUmUnicoStatement() {
        // Garante que multiplos INSERT num EXECUTE STATEMENT SET viram um unico
        // job Flink (um checkpoint coerente), e nao um job por INSERT.
        var script =
                "CREATE TABLE a (x INT) WITH ('connector'='datagen');\n"
                        + "EXECUTE STATEMENT SET\n"
                        + "BEGIN\n"
                        + "INSERT INTO a SELECT 1;\n"
                        + "INSERT INTO a SELECT 2;\n"
                        + "END;\n";

        var statements = SqlRunner.parseStatements(script);
        assertEquals(2, statements.size());
        assertTrue(statements.get(1).contains("INSERT INTO a SELECT 1;"));
        assertTrue(statements.get(1).contains("INSERT INTO a SELECT 2;"));
    }
}
