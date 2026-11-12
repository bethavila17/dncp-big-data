#!/usr/bin/env python3
"""Reducer de Hadoop Streaming: suma los valores de cada clave.

Hadoop entrega las líneas ordenadas por clave; las claves son todos los campos menos
los valores numéricos del final. P1 lleva dos campos de clave y P2* uno.
"""
import sys

CAMPOS_CLAVE = {"P1": 3, "P2M": 2, "P2E": 2, "P2A": 2, "ERR": 2}


def separar(linea):
    partes = linea.rstrip("\n").split("\t")
    n = CAMPOS_CLAVE[partes[0]]
    return tuple(partes[:n]), partes[n:]


def emitir(clave, acumulado):
    valores = [repr(v) if isinstance(v, float) else str(v) for v in acumulado]
    sys.stdout.write("\t".join(list(clave) + valores) + "\n")


def main():
    actual, acumulado = None, None
    for linea in sys.stdin:
        clave, valores = separar(linea)
        numeros = [float(v) if "." in v or "e" in v else int(v) for v in valores]
        if clave != actual:
            if actual is not None:
                emitir(actual, acumulado)
            actual, acumulado = clave, numeros
        else:
            acumulado = [a + b for a, b in zip(acumulado, numeros)]
    if actual is not None:
        emitir(actual, acumulado)


if __name__ == "__main__":
    main()
