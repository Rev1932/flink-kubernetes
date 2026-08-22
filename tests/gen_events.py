#!/usr/bin/env python3
"""Gera eventos JSON de teste para os topicos de origem.

Uso: gen_events.py <pipeline> [--users N] [--per-window N] [--windows N]

O detalhe que faz este gerador existir em vez de um `echo`: o streaming_1 agrega
em janela de 1 minuto com watermark. Uma janela so e emitida quando o watermark
a ultrapassa, ou seja, quando chega um evento suficientemente a frente no tempo.
Por isso a ultima linha e sempre um evento deslocado varios minutos para o
futuro -- sem ele o job processa tudo corretamente e nao escreve nada, o que
parece uma falha do sink.
"""
import argparse
import json
import sys
from datetime import datetime, timedelta

BASE = datetime(2026, 1, 1, 12, 0, 0)


def iso(dt):
    # 'ISO-8601' no conector espera precisao de milissegundos.
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("pipeline", choices=["streaming_1", "streaming_2"])
    ap.add_argument("--users", type=int, default=3)
    ap.add_argument("--per-window", type=int, default=4)
    ap.add_argument("--windows", type=int, default=3)
    ap.add_argument("--tag", default="a", help="marca os event_id, para reenvio idempotente")
    args = ap.parse_args()

    out = []
    for w in range(args.windows):
        window_start = BASE + timedelta(minutes=w)
        for u in range(args.users):
            for i in range(args.per_window):
                ts = window_start + timedelta(seconds=i * 5)
                event = {
                    # event_id estavel entre execucoes com a mesma tag: e o que
                    # permite verificar o upsert reenviando o mesmo lote.
                    "event_id": f"evt-{args.tag}-{w}-{u}-{i}",
                    "user_id": f"user_{u}",
                    "amount": round(100.0 + (u * 50) + (i * 25), 2),
                    "event_time": iso(ts),
                }
                if args.pipeline == "streaming_2":
                    event["currency"] = "BRL"
                out.append(event)

    # Empurra o watermark bem alem da ultima janela para forcar a emissao.
    out.append(
        {
            "event_id": f"evt-{args.tag}-watermark",
            "user_id": "user_watermark",
            "amount": 1.0,
            "currency": "BRL",
            "event_time": iso(BASE + timedelta(minutes=args.windows + 10)),
        }
    )

    for event in out:
        sys.stdout.write(json.dumps(event) + "\n")


if __name__ == "__main__":
    main()
