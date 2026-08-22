#!/usr/bin/env python3
"""Verifica que os manifestos de um tenant nao referenciam outro tenant.

A checagem e estrutural, e nao textual: o .sql do catalogo e compartilhado e
menciona ambos os tenants nos comentarios de exemplo, o que faria um grep
simples acusar vazamento onde nao ha. O que realmente importa sao os campos
efetivos -- nomes de recurso, topicos, valores de variavel de ambiente.

Uso: check_isolation.py <manifestos.yaml> <id-esperado> <id-proibido>
"""
import re
import sys

import yaml

# Campos cujo VALOR identifica um tenant. O corpo do ConfigMap com o SQL fica
# deliberadamente de fora: ele e o template compartilhado, ainda com ${TENANT}.
INTERESTING_KEYS = {"name", "topicName", "value", "collection", "namespace"}


def walk(node, path=""):
    """Emite (caminho, valor) para todo escalar do documento."""
    if isinstance(node, dict):
        for key, value in node.items():
            # `data` de ConfigMap carrega o SQL bruto, com comentarios.
            if key == "data":
                continue
            yield from walk(value, f"{path}.{key}")
    elif isinstance(node, list):
        for i, item in enumerate(node):
            yield from walk(item, f"{path}[{i}]")
    else:
        field = path.rsplit(".", 1)[-1]
        if field in INTERESTING_KEYS and isinstance(node, str):
            yield path, node


def main():
    manifest, expected, forbidden = sys.argv[1], sys.argv[2], sys.argv[3]

    with open(manifest) as fh:
        docs = [d for d in yaml.safe_load_all(fh) if d]

    # Casa <ID>_ ou <id>- como prefixo de identificador, nao a letra solta.
    pattern = re.compile(
        rf"(^|[^A-Za-z0-9]){re.escape(forbidden)}[_-]|"
        rf"(^|[^A-Za-z0-9]){re.escape(forbidden.lower())}[_-]"
    )

    leaks = [
        (path, value)
        for doc in docs
        for path, value in walk(doc)
        if pattern.search(value)
    ]

    if leaks:
        print(f"vazamento do tenant '{forbidden}' nos manifestos de '{expected}':")
        for path, value in leaks[:10]:
            print(f"    {path} = {value}")
        return 1

    # Contraprova: o tenant esperado precisa realmente aparecer, senao um
    # manifesto vazio passaria no teste de isolamento.
    own = re.compile(rf"(^|[^A-Za-z0-9])({re.escape(expected)}[_-]|{re.escape(expected.lower())}[_-])")
    if not any(own.search(v) for doc in docs for _, v in walk(doc)):
        print(f"nenhuma referencia ao proprio tenant '{expected}' -- manifesto suspeito")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
