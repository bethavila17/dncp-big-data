"""Paso 4 (Apache Hive): ejecuta src/hive/preguntas.hql y guarda cada exportable.

  python src/04_hive.py --beeline "jdbc:hive2://"
      Ejecuta el HiveQL en Apache Hive real (Colab: Hive 3.1 sobre HDFS) con beeline.

  python src/04_hive.py
      Sin Hive instalado (Windows), ejecuta el mismo HiveQL con el motor SQL de Spark
      (compatible con HiveQL). Las tablas externas se reemplazan por vistas temporales
      sobre el mismo Parquet; las consultas SELECT se ejecutan sin cambios.
"""
import argparse
import io
import re
import subprocess
import time

import pandas as pd

import comun

HQL = f"{comun.RAIZ}/src/hive/preguntas.hql"


def sentencias(ruta, parquet):
    """Divide el script en sentencias y asocia cada SELECT con su nombre de exportable."""
    texto = open(ruta, encoding="utf-8").read().replace("${PARQUET}", parquet)
    resultado, nombre, actual = [], None, []
    for linea in texto.splitlines():
        marca = re.match(r"\s*--\s*exportable:\s*(\S+)", linea)
        if marca:
            nombre = marca.group(1)
            continue
        if linea.strip().startswith("--"):
            continue
        actual.append(linea)
        if linea.rstrip().endswith(";"):
            sql = "\n".join(actual).strip().rstrip(";").strip()
            if sql:
                resultado.append((nombre, sql))
            nombre, actual = None, []
    return resultado


def con_spark():
    spark = comun.spark("04_hive_sql")
    for tabla in ["procesos", "adjudicaciones"]:
        comun.leer_parquet(spark, tabla).createOrReplaceTempView(tabla)
    for nombre, sql in sentencias(HQL, comun.PARQUET):
        inicial = sql.split()[0].upper()
        if inicial in ("USE", "DROP") or sql.upper().startswith(("CREATE DATABASE", "CREATE EXTERNAL")):
            continue  # el catálogo de Hive se reemplaza por vistas temporales
        if sql.upper().startswith("CREATE VIEW"):
            sql = re.sub(r"(?i)^CREATE VIEW", "CREATE OR REPLACE TEMPORARY VIEW", sql)
        t0 = time.time()
        df = spark.sql(sql)
        if nombre:
            comun.guardar_csv(df.toPandas(), "hive", nombre)
            print(f"   {time.time() - t0:.1f} s")
    spark.stop()


def con_beeline(url, parquet):
    def beeline(sql):
        r = subprocess.run(["beeline", "-u", url, "--silent=true", "--showHeader=true",
                            "--outputformat=csv2", "-e", sql + ";"],
                           capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(r.stderr[-2000:])
        return r.stdout

    for nombre, sql in sentencias(HQL, parquet):
        t0 = time.time()
        salida = beeline("USE dncp; " + sql if not sql.upper().startswith(("CREATE DATABASE", "USE")) else sql)
        if nombre:
            df = pd.read_csv(io.StringIO(salida))
            df.columns = [c.split(".")[-1] for c in df.columns]
            comun.guardar_csv(df, "hive", nombre)
            print(f"   {time.time() - t0:.1f} s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--beeline", help="URL JDBC de HiveServer2, p. ej. jdbc:hive2://")
    parser.add_argument("--parquet", default=comun.PARQUET, help="ruta de las tablas (p. ej. en HDFS)")
    args = parser.parse_args()
    if args.beeline:
        con_beeline(args.beeline, args.parquet)
    else:
        con_spark()
