"""Paso 3 (MapReduce): preguntas 1 y 2 con el mapper y reducer de Hadoop Streaming.

Dos modos:

  python src/03_mapreduce.py
      Ejecuta el mismo flujo que Hadoop Streaming en esta máquina: divide el JSON
      Lines en splits alineados a líneas (como los InputSplit de 128 MB de HDFS),
      corre un proceso mapper por split en paralelo, ordena la salida por clave
      (shuffle & sort) y la pasa por el reducer. Son dos trabajos encadenados:
        1. mapper_entidades.py / reducer_entidades.py: tabla entidad -> tipo
        2. mapper.py / reducer.py: P1 y P2, con la tabla anterior como caché

  python src/03_mapreduce.py --hadoop salida_hadoop.txt
      Convierte la salida real de `hadoop jar hadoop-streaming.jar` (por ejemplo,
      `hdfs dfs -getmerge /dncp/salida_mr salida_hadoop.txt` en Colab) a los CSV.
"""
import argparse
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

import comun

CARPETA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mapreduce")
TAM_SPLIT = 128 * 1024 * 1024


def splits(ruta):
    """Rangos de bytes de ~128 MB que terminan en fin de línea."""
    tam = os.path.getsize(ruta)
    rangos, inicio = [], 0
    with open(ruta, "rb") as f:
        while inicio < tam:
            f.seek(min(inicio + TAM_SPLIT, tam))
            f.readline()
            fin = min(f.tell(), tam)
            rangos.append((inicio, fin))
            inicio = fin
    return rangos


def tarea_map(args):
    ruta, (inicio, fin), mapper, argumentos = args
    proc = subprocess.Popen([sys.executable, os.path.join(CARPETA, mapper), *argumentos],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    def alimentar():
        with open(ruta, "rb") as f:
            f.seek(inicio)
            restante = fin - inicio
            while restante > 0:
                bloque = f.read(min(8 * 1024 * 1024, restante))
                proc.stdin.write(bloque)
                restante -= len(bloque)
        proc.stdin.close()

    with ThreadPoolExecutor(1) as ex:
        ex.submit(alimentar)
        salida = proc.stdout.read()
    if proc.wait() != 0:
        raise RuntimeError(f"el mapper falló en el split {inicio}-{fin}")
    return salida


def ordenar_y_reducir(lineas, reducer):
    """Ordena por los bytes de la clave (como Hadoop) y pasa las líneas por el reducer."""
    lineas = sorted(lineas, key=lambda l: l.encode("utf-8"))
    salida = subprocess.run([sys.executable, os.path.join(CARPETA, reducer)],
                            input="".join(lineas).encode("utf-8"), capture_output=True, check=True)
    return salida.stdout.decode("utf-8").replace("\r\n", "\n").splitlines(keepends=True)


CONTADORES = []


def trabajo(nombre, mapper, reducer, argumentos=()):
    """Un trabajo de Hadoop Streaming: map en paralelo por split, combiner, shuffle & sort, reduce.

    Como en el laboratorio de la clase 3, el reducer también sirve de combiner porque sus operaciones
    (suma y máximo) son asociativas: cada split se resume antes del shuffle y viajan menos registros.
    """
    rangos = splits(comun.JSONL)
    t0 = time.time()
    with ThreadPoolExecutor(os.cpu_count()) as ex:
        salidas = list(ex.map(tarea_map, [(comun.JSONL, r, mapper, list(argumentos)) for r in rangos]))
        por_split = [s.decode("utf-8").splitlines(keepends=True) for s in salidas]
        combinadas = list(ex.map(lambda lineas: ordenar_y_reducir(lineas, reducer), por_split))
    salida_map = sum(len(l) for l in por_split)
    salida_combiner = sum(len(l) for l in combinadas)
    reducido = ordenar_y_reducir([l for c in combinadas for l in c], reducer)
    CONTADORES.append({"trabajo": nombre, "splits": len(rangos), "map_output_records": salida_map,
                       "combine_output_records": salida_combiner, "reduce_output_records": len(reducido),
                       "segundos": round(time.time() - t0, 1)})
    print(f"{nombre}: {len(rangos)} splits, {os.cpu_count()} mappers en paralelo · map {salida_map:,} pares · "
          f"combiner {salida_combiner:,} · reduce {len(reducido):,} · {time.time() - t0:.0f} s")
    return "".join(reducido)


def ejecutar_local():
    tipos = trabajo("trabajo 1 (entidad -> tipo)", "mapper_entidades.py", "reducer_entidades.py")
    cache = os.path.join(comun.BASE, f"entidades_tipo_{comun.ALCANCE}.tsv")
    with open(cache, "w", encoding="utf-8", newline="\n") as f:
        f.write(tipos.replace("\r\n", "\n"))
    print(f"  caché distribuida: {len(tipos.splitlines()):,} entidades con tipo")
    return trabajo("trabajo 2 (P1 y P2)", "mapper.py", "reducer.py", [cache])


def a_csv(texto):
    filas = [l.split("\t") for l in texto.splitlines() if l.strip()]
    p1 = pd.DataFrame([f[1:] for f in filas if f[0] == "P1"],
                      columns=["anio", "modalidad", "procesos", "adjudicaciones", "contratos",
                               "monto_adjudicado_pyg"])
    for c in ["procesos", "adjudicaciones", "contratos"]:
        p1[c] = p1[c].astype(int)
    p1["monto_adjudicado_pyg"] = p1["monto_adjudicado_pyg"].astype(float)
    comun.guardar_csv(p1.sort_values(["anio", "modalidad"]), "mapreduce", "p1_anio_modalidad")

    for codigo, dim, nombre in [("P2M", "modalidad", "p2_unica_oferta_modalidad"),
                                ("P2E", "tipo_entidad", "p2_unica_oferta_tipo_entidad"),
                                ("P2A", "anio", "p2_unica_oferta_anio")]:
        df = pd.DataFrame([f[1:] for f in filas if f[0] == codigo],
                          columns=[dim, "llamados_con_oferta", "una_sola_oferta"])
        df[["llamados_con_oferta", "una_sola_oferta"]] = df[["llamados_con_oferta",
                                                             "una_sola_oferta"]].astype(int)
        df["pct_una_oferta"] = (100 * df["una_sola_oferta"] / df["llamados_con_oferta"]).round(2)
        comun.guardar_csv(df.sort_values(dim), "mapreduce", nombre)

    errores = sum(int(f[2]) for f in filas if f[0] == "ERR")
    print(f"líneas con JSON inválido: {errores}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--hadoop", help="archivo con la salida de Hadoop Streaming")
    args = parser.parse_args()
    if args.hadoop:
        with open(args.hadoop, encoding="utf-8") as f:
            texto = f.read()
    else:
        texto = ejecutar_local()
        comun.guardar_csv(pd.DataFrame(CONTADORES), "mapreduce", "00_contadores")
    a_csv(texto)
