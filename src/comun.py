"""Configuración y utilidades compartidas por todos los scripts del pipeline.

Variables de entorno:
    DNCP_BASE           carpeta de trabajo con los datos (por defecto C:/Users/betha/dncp_bigdata)
    ALCANCE             "full" (histórico 2011-2026) o un año ("2025")
    DNCP_JSONL          ruta del JSON Lines para Spark (en Colab: hdfs://localhost:9000/...)
    DNCP_PARQUET        ruta de las tablas Parquet para Spark/Hive (en Colab: en HDFS)
    DNCP_PARQUET_LOCAL  copia local de las tablas para Elasticsearch, HBase y la verificación
    SPARK_MEMORIA       memoria del driver de Spark (por defecto 12g)
"""
import os
import platform

BASE = os.environ.get("DNCP_BASE", "C:/Users/betha/dncp_bigdata").replace("\\", "/")
ALCANCE = os.environ.get("ALCANCE", "full")

JSONL_GZ = f"{BASE}/datos/{ALCANCE}.jsonl.gz"
# Spark no puede dividir un .gz entre tareas; se trabaja sobre el .jsonl descomprimido
# para que cada bloque de 128 MB sea una tarea, igual que un InputSplit de HDFS.
JSONL = os.environ.get("DNCP_JSONL", f"{BASE}/datos/{ALCANCE}.jsonl")
PARQUET = os.environ.get("DNCP_PARQUET", f"{BASE}/parquet_{ALCANCE}")
PARQUET_LOCAL = os.environ.get("DNCP_PARQUET_LOCAL", PARQUET)

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RESULTADOS = os.path.join(RAIZ, "resultados", ALCANCE)

TABLAS = ["procesos", "adjudicaciones", "contratos", "items_adjudicados"]

# Términos de la pregunta 7 (singular y plural, sin tildes)
TERMINOS_P7 = {
    "combustible": ["combustible", "combustibles"],
    "medicamentos": ["medicamento", "medicamentos"],
    "alimentos": ["alimento", "alimentos"],
}

ES_WINDOWS = platform.system() == "Windows"


def carpeta_resultados(herramienta):
    ruta = os.path.join(RESULTADOS, herramienta)
    os.makedirs(ruta, exist_ok=True)
    return ruta


def spark(app, memoria="12g"):
    """SparkSession local con todos los núcleos."""
    if ES_WINDOWS:
        # Java 23 necesita este permiso para el cliente de Hadoop que trae Spark
        os.environ.setdefault("JAVA_TOOL_OPTIONS", "-Djava.security.manager=allow")
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[*]")
        .appName(app)
        .config("spark.driver.memory", os.environ.get("SPARK_MEMORIA", memoria))
        .config("spark.sql.shuffle.partitions", "48")
        .config("spark.sql.session.timeZone", "America/Asuncion")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Datos públicos con valores sucios: un campo inválido queda nulo en vez de abortar
        .config("spark.sql.ansi.enabled", "false")
        # ...pero size(null) debe ser nulo y no -1 (comportamiento heredado sin ANSI)
        .config("spark.sql.legacy.sizeOfNull", "false")
        .getOrCreate()
    )


def escribir_parquet(df, ruta, particion=None):
    """Escribe un DataFrame de Spark como carpeta Parquet y devuelve la cantidad de filas.

    En Linux (Colab, clúster) usa el escritor distribuido de Spark. En Windows ese
    escritor exige winutils.exe de Hadoop y los workers de Python fallan con
    Python 3.13, así que el resultado se trae al driver como Arrow (JVM -> Python,
    sin workers) y se guarda con PyArrow. Con `particion` se escribe un archivo por
    valor de esa columna para no traer toda la tabla de una vez.
    """
    if not ES_WINDOWS:
        df.write.mode("overwrite").parquet(ruta)
        return df.sparkSession.read.parquet(ruta).count()
    import shutil

    import pyarrow.parquet as pq
    from pyspark.sql import functions as F

    shutil.rmtree(ruta, ignore_errors=True)
    os.makedirs(ruta)
    if particion is None:
        grupos = [(None, df)]
    else:
        valores = [r[0] for r in df.select(particion).distinct().collect()]
        grupos = [(v, df.where(F.col(particion).eqNullSafe(v))) for v in valores]
    total = 0
    for i, (valor, parte) in enumerate(grupos):
        tabla = parte.toArrow()
        pq.write_table(tabla, f"{ruta}/part-{i:05d}.parquet", compression="snappy")
        total += tabla.num_rows
    return total


def leer_parquet(spark, tabla):
    """Lee una tabla Parquet del pipeline. En Windows se pasan los archivos uno por uno,
    porque listar la carpeta con Hadoop requiere su DLL nativa (hadoop.dll)."""
    ruta = f"{PARQUET}/{tabla}"
    if not ES_WINDOWS:
        return spark.read.parquet(ruta)
    import glob

    archivos = sorted(glob.glob(f"{ruta}/*.parquet"))
    return spark.read.parquet(*[a.replace("\\", "/") for a in archivos])


def guardar_csv(pdf, herramienta, nombre):
    """Guarda un DataFrame de pandas como exportable CSV (UTF-8 con BOM para Excel)."""
    ruta = os.path.join(carpeta_resultados(herramienta), f"{nombre}.csv")
    pdf.to_csv(ruta, index=False, encoding="utf-8-sig")
    print(f"  -> {os.path.relpath(ruta, RAIZ)} ({len(pdf):,} filas)")
    return ruta
