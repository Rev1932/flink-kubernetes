#!/usr/bin/env python3
"""Detecta escalares que o YAML converteu em booleano ou numero onde o
Kubernetes exige string.

Motivacao concreta: o slug do tenant N e a letra `n`. Em YAML 1.1,
`n`/`y`/`no`/`yes`/`on`/`off` sao BOOLEANOS. Escrever `tenant: n` sem aspas
produz `tenant: false`, e a API rejeita o objeto com

    cannot unmarshal bool into Go struct field ObjectMeta.labels of type string

Este erro so aparece no `helm install`, nunca no `helm template`, porque a
renderizacao e sintaticamente valida. Dai um teste dedicado.

Uso: check_yaml_types.py <manifesto.yaml> [...]
"""
import sys

import yaml

# Campos do Kubernetes cujos valores tem de ser string.
#
# `selector` fica de fora de proposito: em Service ele e um mapa de strings, mas
# em PodMonitor/Deployment carrega `matchLabels` aninhado. Como matchLabels ja
# esta na lista, o caso util fica coberto sem falso-positivo.
STRING_MAPS = ("labels", "annotations", "matchLabels", "nodeSelector")


def scan(node, path=""):
    """Emite (caminho, valor) para todo valor nao-string dentro de um mapa que
    exige strings."""
    if isinstance(node, dict):
        for key, value in node.items():
            here = f"{path}.{key}"
            if key in STRING_MAPS and isinstance(value, dict):
                for k, v in value.items():
                    if not isinstance(v, str):
                        yield f"{here}.{k}", v
            else:
                yield from scan(value, here)
    elif isinstance(node, list):
        for i, item in enumerate(node):
            yield from scan(item, f"{path}[{i}]")


def main():
    problems = []
    for path in sys.argv[1:]:
        with open(path) as fh:
            for doc in yaml.safe_load_all(fh):
                if not doc:
                    continue
                kind = doc.get("kind", "?")
                name = doc.get("metadata", {}).get("name", "?")
                for where, value in scan(doc):
                    problems.append(f"{kind}/{name}{where} = {value!r} ({type(value).__name__})")

    if problems:
        print("valores que deveriam ser string (falta `| quote` no template):")
        for p in problems:
            print(f"    {p}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
