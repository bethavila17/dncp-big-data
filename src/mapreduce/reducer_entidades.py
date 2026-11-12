#!/usr/bin/env python3
"""Reducer del trabajo 1: un tipo por entidad (el mayor alfabéticamente, como MAX en SQL)."""
import sys


def main():
    actual, tipo = None, None
    for linea in sys.stdin:
        entidad_id, valor = linea.rstrip("\n").split("\t", 1)
        if entidad_id != actual:
            if actual is not None:
                sys.stdout.write(f"{actual}\t{tipo}\n")
            actual, tipo = entidad_id, valor
        else:
            tipo = max(tipo, valor)
    if actual is not None:
        sys.stdout.write(f"{actual}\t{tipo}\n")


if __name__ == "__main__":
    main()
