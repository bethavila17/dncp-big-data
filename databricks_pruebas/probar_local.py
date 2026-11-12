"""Prueba local de los notebooks de Databricks (no se sube a Databricks).

Ejecuta cada notebook en formato fuente de Databricks celda por celda, con Spark 4 en
modo ANSI (como el cómputo serverless), resolviendo `%run ./00_config` y reemplazando las
tablas del catálogo por vistas temporales (ver guardar_tabla en 00_config.py).

Uso:  python databricks_pruebas/probar_local.py [01 02 03]
"""
import os
import sys
import time
import traceback

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CARPETA = os.path.join(RAIZ, "databricks")
os.environ.setdefault("JAVA_TOOL_OPTIONS", "-Djava.security.manager=allow")
os.environ.setdefault("DNCP_VOLUMEN_LOCAL", "C:/Users/betha/dncp_bigdata/prueba_volumen")

from pyspark.sql import SparkSession  # noqa: E402

NOTEBOOKS = {"01": "01_bronce.py", "02": "02_plata.py", "03": "03_oro_y_rendimiento.py"}


def celdas(ruta):
    texto = open(ruta, encoding="utf-8").read()
    return [c.strip("\n") for c in texto.split("# COMMAND ----------")]


def ejecutar(ruta, entorno):
    for i, celda in enumerate(celdas(ruta)):
        lineas = [l for l in celda.splitlines() if l.strip() and l.strip() != "# Databricks notebook source"]
        if not lineas:
            continue
        if all(l.startswith("# MAGIC") for l in lineas):
            comando = lineas[0][len("# MAGIC"):].strip()
            if comando.startswith("%run"):
                destino = comando.split()[1].replace("./", "")
                ejecutar(os.path.join(CARPETA, destino + ".py"), entorno)
            continue  # celdas %md
        t0 = time.time()
        try:
            exec(compile(celda, f"{os.path.basename(ruta)}[celda {i}]", "exec"), entorno)
        except Exception:
            traceback.print_exc()
            raise SystemExit(f"FALLÓ {os.path.basename(ruta)} celda {i}")
        print(f"   · celda {i} ok ({time.time() - t0:.1f} s)")


def main():
    spark = (SparkSession.builder.master("local[*]").appName("prueba_databricks")
             .config("spark.driver.memory", "22g").config("spark.driver.maxResultSize", "0").config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark {spark.version} · ANSI = {spark.conf.get('spark.sql.ansi.enabled')}")

    # En Windows con Python 3.13 los workers de Python de Spark local fallan. Databricks serverless
    # envía las listas locales como Arrow; aquí se hace lo mismo pasando por pandas + Arrow.
    import pandas as pd
    from pyspark.sql.types import _parse_datatype_string
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
    original = spark.createDataFrame

    def crear(datos, schema=None, *args, **kwargs):
        if isinstance(datos, list):
            esquema = _parse_datatype_string(schema) if isinstance(schema, str) else schema
            datos = pd.DataFrame(datos, columns=esquema.fieldNames())
            return original(datos, schema=esquema)
        return original(datos, schema, *args, **kwargs)

    spark.createDataFrame = crear
    entorno = {"spark": spark, "__name__": "__notebook__"}
    for clave in sys.argv[1:] or list(NOTEBOOKS) + ["dash"]:
        if clave == "dash":
            probar_dashboard(spark)
            continue
        print(f"\n==================== {NOTEBOOKS[clave]} ====================")
        ejecutar(os.path.join(CARPETA, NOTEBOOKS[clave]), entorno)
    exportar(spark, entorno.get("_VISTAS_LOCALES", set()))
    print("\nTODOS LOS NOTEBOOKS SE EJECUTARON SIN ERRORES")


def exportar(spark, vistas):
    """Guarda en CSV las tablas chicas (oro, calidad, control) para revisar los resultados de la prueba."""
    destino = os.path.join(RAIZ, "databricks_pruebas", "resultados_locales")
    os.makedirs(destino, exist_ok=True)
    for nombre in sorted(vistas):
        if nombre.startswith(("oro_", "plata_calidad", "bronce_control", "cuarentena_procesos")):
            df = spark.table(nombre)
            if "plan" in df.columns:
                df = df.drop("plan")
            df.toPandas().to_csv(os.path.join(destino, f"{nombre}.csv"), index=False, encoding="utf-8-sig")
    print(f"tablas exportadas a {destino}")


def probar_dashboard(spark):
    """Ejecuta cada consulta del dashboard contra las tablas oro (vistas temporales locales)."""
    import json
    tablero = json.load(open(os.path.join(CARPETA, "dashboard_dncp.lvdash.json"), encoding="utf-8"))
    print("\n==================== dashboard_dncp.lvdash.json ====================")
    for d in tablero["datasets"]:
        sql = "".join(d["queryLines"]).replace("workspace.dncp.", "")
        filas = spark.sql(sql).collect()
        print(f"   · dataset {d['name']}: {len(filas)} filas · primera: {filas[0].asDict() if filas else '-'}")


if __name__ == "__main__":
    main()
