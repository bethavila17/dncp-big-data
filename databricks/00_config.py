# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuración
# MAGIC
# MAGIC **Proyecto final · Contrataciones públicas de la DNCP · Avance 1: pipeline en Databricks**
# MAGIC Bethania Avila, Arturo Fernandez, Gustavo Bogado
# MAGIC
# MAGIC Los demás notebooks lo ejecutan con `%run ./00_config`; no hace falta abrirlo. Sigue la estructura del
# MAGIC laboratorio de la clase 8: un catálogo y un esquema propios, un **volumen** `crudos` donde aterrizan los
# MAGIC archivos tal como llegan, y la función `tabla()` para armar el nombre completo de cada tabla.

# COMMAND ----------

import contextlib
import io
import os
import re
import time

from pyspark.sql import Window
from pyspark.sql import functions as F

# Dentro de Databricks existe `dbutils`; fuera (prueba local del grupo) las tablas son vistas temporales
try:
    dbutils  # noqa: F821
    EN_DATABRICKS = True
except NameError:
    EN_DATABRICKS = False

CATALOGO = "workspace"   # cambiar si se usa otro catálogo
ESQUEMA = "dncp"

RUTA_CRUDOS = (f"/Volumes/{CATALOGO}/{ESQUEMA}/crudos" if EN_DATABRICKS
               else os.environ.get("DNCP_VOLUMEN_LOCAL", "crudos"))

# Un archivo JSON Lines comprimido por año en el OCP Data Registry
ANIOS = list(range(2011, 2027))
URL_DESCARGA = "https://data.open-contracting.org/en/publication/63/download?name={anio}.jsonl.gz"


def tabla(nombre):
    """Nombre completo de una tabla del proyecto: catálogo.esquema.tabla."""
    return f"{CATALOGO}.{ESQUEMA}.{nombre}" if EN_DATABRICKS else nombre


print("Archivos crudos en:", RUTA_CRUDOS)
print("Tablas en:", f"{CATALOGO}.{ESQUEMA}")

# COMMAND ----------

def sql_databricks(sentencia):
    """SQL propio de Databricks (esquemas, volúmenes, Delta): en la prueba local se omite."""
    if EN_DATABRICKS:
        return spark.sql(sentencia)  # noqa: F821
    print(f"[local] se omite: {' '.join(sentencia.split())[:100]}")


_VISTAS_LOCALES = globals().get("_VISTAS_LOCALES", set())  # solo para la prueba local


def tabla_existe(nombre):
    if EN_DATABRICKS:
        return spark.catalog.tableExists(tabla(nombre))  # noqa: F821
    return nombre in _VISTAS_LOCALES


def guardar_tabla(df, nombre, particion=None, comentario=None, modo="overwrite"):
    """Guarda el DataFrame como tabla Delta del catálogo con saveAsTable y devuelve sus filas."""
    if EN_DATABRICKS:
        escritor = df.write.mode(modo)
        if modo == "overwrite":
            escritor = escritor.option("overwriteSchema", "true")
        if particion:
            escritor = escritor.partitionBy(particion)
        escritor.saveAsTable(tabla(nombre))
        if comentario:
            spark.sql(f"COMMENT ON TABLE {tabla(nombre)} IS '{comentario.replace(chr(39), '')}'")  # noqa: F821
    else:
        datos = df.toArrow()
        if modo == "append" and tabla_existe(nombre):
            previo = spark.table(nombre).toArrow()  # noqa: F821
            import pyarrow as pa
            datos = pa.concat_tables([previo, datos.select(previo.column_names).cast(previo.schema)])
        spark.createDataFrame(datos).createOrReplaceTempView(nombre)  # noqa: F821
        _VISTAS_LOCALES.add(nombre)
    filas = spark.read.table(tabla(nombre)).count()  # noqa: F821
    print(f"{tabla(nombre)}: {filas:,} filas" + (f" · particionada por {particion}" if particion else ""))
    return filas


def leer(nombre):
    return spark.read.table(tabla(nombre))  # noqa: F821


def mostrar(df, n=20):
    """display() en Databricks; texto en la prueba local."""
    if EN_DATABRICKS:
        display(df)  # noqa: F821
    else:
        print(df.limit(n).toPandas().to_string())

# COMMAND ----------

# Rendimiento (clase 7): medir, diagnosticar con explain(), cambiar una cosa, volver a medir
REGISTRO_RENDIMIENTO = []


def plan_de(df):
    """Texto de df.explain(mode='formatted'): el plan físico, sin ejecutar la consulta."""
    salida = io.StringIO()
    with contextlib.redirect_stdout(salida):
        df.explain(mode="formatted")
    return salida.getvalue()


def resumir_plan(plan):
    """Lo que interesa del plan: lecturas, shuffles (Exchange), tipo de join y poda de particiones."""
    # Solo el árbol del plan; con AQE el texto repite el plan inicial después del actual
    arbol = plan.split("\n\n")[0].split("== Initial Plan ==")[0]
    lecturas = len(re.findall(r"Scan", arbol))
    shuffles = len([l for l in arbol.splitlines()
                    if "Exchange" in l and "ExchangeSource" not in l and "Broadcast" not in l])
    joins = sorted(set(re.findall(r"(\w*(?:BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin|"
                                  r"BroadcastNestedLoopJoin))", arbol)))
    filtros = [f for f in re.findall(r"PartitionFilters: \[([^\]]*)\]", plan) if f.strip()]
    partes = [f"{lecturas} lectura(s)", f"{shuffles} shuffle(s)"]
    if joins:
        partes.append("join: " + ", ".join(joins))
    if "Window" in arbol:
        partes.append("ventana")
    partes.append("poda de particiones: " + (re.sub(r"#\d+", "", filtros[0])[:60] if filtros else "no"))
    return "; ".join(partes)


def medir(consulta, df, repeticiones=2, etapa="consulta"):
    """Ejecuta la consulta varias veces y registra cada tiempo y el resumen del plan.

    La primera ejecución puede incluir el arranque del cómputo; por eso se mide más de una vez.
    """
    tiempos, filas = [], None
    for _ in range(repeticiones):
        inicio = time.perf_counter()
        filas = df.collect()
        tiempos.append(round(time.perf_counter() - inicio, 3))
    plan = plan_de(df)
    REGISTRO_RENDIMIENTO.append((consulta, etapa, len(filas), tiempos[0], tiempos[-1], min(tiempos),
                                 resumir_plan(plan), plan[:20000]))
    print(f"{consulta} [{etapa}]: {len(filas):,} filas · {tiempos} s · {resumir_plan(plan)}")
    return df
