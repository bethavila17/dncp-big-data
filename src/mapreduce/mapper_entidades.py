#!/usr/bin/env python3
"""Trabajo 1 de MapReduce: tipo de entidad informado por cada entidad compradora.

Emite entidad_id<TAB>tipo para cada proceso cuyo comprador trae el tipo en `parties`.
El reducer se queda con un valor por entidad y el resultado se distribuye al
trabajo 2 como archivo de caché (map-side join).
"""
import sys

from mapper import cargar, limpiar


def main():
    for linea in sys.stdin.buffer:
        linea = linea.strip()
        if not linea:
            continue
        try:
            r = cargar(linea)
        except ValueError:
            continue
        entidad_id = (r.get("buyer") or {}).get("id")
        if entidad_id is None or not r.get("tender"):
            continue
        for p in r.get("parties") or []:
            if "buyer" in (p.get("roles") or []):
                tipo = limpiar((p.get("details") or {}).get("level"))
                if tipo:
                    sys.stdout.write(f"{entidad_id}\t{tipo}\n")
                break


if __name__ == "__main__":
    main()
