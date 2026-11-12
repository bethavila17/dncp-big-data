#!/usr/bin/env python3
"""Mapper de Hadoop Streaming para las preguntas 1 y 2.

Entrada: líneas del JSON Lines OCDS (un proceso por línea).
Salida:  clave<TAB>valores, con la clave compuesta por pregunta y dimensiones.

    P1  anio  modalidad      -> procesos, adjudicaciones, contratos, monto_adjudicado_pyg
    P2M modalidad            -> llamados_con_oferta, una_sola_oferta
    P2E tipo_entidad         -> llamados_con_oferta, una_sola_oferta
    P2A anio                 -> llamados_con_oferta, una_sola_oferta

Las definiciones replican las de src/01_aplanar_spark.py, pero el parseo del JSON
es independiente: si Spark y MapReduce coinciden, el aplanado es correcto.

El tipo de entidad faltante se completa con la tabla entidad -> tipo que genera el
trabajo 1 (mapper_entidades.py). Hadoop la distribuye a cada mapper con -files
(caché distribuida); el nombre del archivo puede pasarse como primer argumento.
"""
import os
import re
import sys

try:
    import orjson as _json
    cargar = _json.loads
except ImportError:  # en los nodos de Hadoop basta con la librería estándar
    import json
    cargar = json.loads

VALIDAS = ("active", "closed")
SIN_DATO = "(sin dato)"
# Clase \s de Java (solo ASCII), para coincidir con regexp_replace de Spark
ESPACIOS = re.compile(r"[ \t\n\x0b\f\r]+")


def limpiar(texto):
    """Espacios, tabuladores y saltos de línea seguidos -> un espacio."""
    return ESPACIOS.sub(" ", texto or "").strip(" ")


def numero(x):
    return float(x) if isinstance(x, (int, float)) and not isinstance(x, bool) else None


def cargar_tipos(ruta):
    tipos = {}
    if os.path.exists(ruta):
        with open(ruta, encoding="utf-8") as f:
            for linea in f:
                entidad_id, tipo = linea.rstrip("\r\n").split("\t", 1)
                tipos[entidad_id] = tipo
    return tipos


TIPOS = {}


def procesar(r):
    t = r.get("tender")
    if not t:
        return
    periodo = t.get("tenderPeriod") or {}
    fecha = periodo.get("startDate") or t.get("datePublished") or r.get("date") or ""
    anio = fecha[:4] if fecha[:4].isdigit() else SIN_DATO
    modalidad = limpiar(t.get("procurementMethodDetails")).lower() or SIN_DATO

    tipo_entidad = "Sin dato"
    for p in r.get("parties") or []:
        if "buyer" in (p.get("roles") or []):
            tipo_entidad = limpiar((p.get("details") or {}).get("level")) or "Sin dato"
            break
    entidad_id = (r.get("buyer") or {}).get("id")
    if entidad_id is not None:
        tipo_entidad = TIPOS.get(entidad_id, tipo_entidad)

    adjudicaciones, monto = 0, 0.0
    for a in r.get("awards") or []:
        if a.get("status") in VALIDAS:
            adjudicaciones += 1
            v = a.get("value") or {}
            if v.get("currency") == "PYG":
                monto += numero(v.get("amount")) or 0.0
    contratos = sum(1 for c in r.get("contracts") or [] if c.get("awardID") is not None)

    ids_bids = {x.get("id") for b in (r.get("bids") or {}).get("details") or []
                for x in b.get("tenderers") or []}
    n_oferentes = max(t.get("numberOfTenderers") or 0, len(t.get("tenderers") or []), len(ids_bids))

    yield f"P1\t{anio}\t{modalidad}", f"1\t{adjudicaciones}\t{contratos}\t{monto!r}"
    if n_oferentes >= 1:
        unica = 1 if n_oferentes == 1 else 0
        yield f"P2M\t{modalidad}", f"1\t{unica}"
        yield f"P2E\t{tipo_entidad}", f"1\t{unica}"
        yield f"P2A\t{anio}", f"1\t{unica}"


def main():
    TIPOS.update(cargar_tipos(sys.argv[1] if len(sys.argv) > 1 else "entidades_tipo.tsv"))
    salida = sys.stdout
    for linea in sys.stdin.buffer:
        linea = linea.strip()
        if not linea:
            continue
        try:
            registro = cargar(linea)
        except ValueError:
            salida.write("ERR\tjson_invalido\t1\n")
            continue
        for clave, valor in procesar(registro):
            salida.write(f"{clave}\t{valor}\n")


if __name__ == "__main__":
    main()
