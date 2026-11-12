# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Carga de los datos y capa bronce
# MAGIC
# MAGIC **Pasos 1 y 2 del Avance 1:** cargar los datos en un volumen y convertirlos en una tabla del catálogo.
# MAGIC
# MAGIC **Los datos.** Contrataciones públicas de la Dirección Nacional de Contrataciones Públicas (DNCP) del
# MAGIC Paraguay, publicadas en formato OCDS por el [OCP Data Registry](https://data.open-contracting.org/en/publication/63)
# MAGIC (licencia CC BY 4.0). Llegan como **un archivo JSON Lines comprimido por año** (`2011.jsonl.gz` a
# MAGIC `2026.jsonl.gz`, 1,7 GB en total). Cada línea es un proceso de contratación con sus etapas anidadas:
# MAGIC planificación, llamado, ofertas, adjudicaciones y contratos.
# MAGIC
# MAGIC **Cómo se cargan.** Los archivos se suben al volumen `crudos` (*Catalog › Add data › Upload to volume*).
# MAGIC Databricks Free Edition restringe la salida a internet; si el cómputo la tiene, la celda de descarga los
# MAGIC baja directamente desde la fuente.
# MAGIC
# MAGIC **Capa bronce (clase 8).** Guarda lo que llegó **sin corregir**, más de qué archivo vino cada fila
# MAGIC (`_archivo`) y cuándo se cargó (`_ingesta`). La carga es **idempotente**: recuerda qué archivos ya cargó,
# MAGIC así que ejecutar el notebook dos veces no duplica filas, y si llega un año nuevo solo se carga ese.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

sql_databricks(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
sql_databricks(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.crudos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Archivos en el volumen
# MAGIC Si faltan años, se intenta descargarlos. Si la red está bloqueada, la celda lo informa: subir los archivos
# MAGIC a mano al volumen y volver a ejecutar.

# COMMAND ----------

import socket
import urllib.request

socket.setdefaulttimeout(20)   # si la red está bloqueada, no quedarse esperando


def archivos_crudos():
    return sorted(f for f in os.listdir(RUTA_CRUDOS) if re.fullmatch(r"\d{4}\.jsonl\.gz", f))

for anio in [a for a in ANIOS if f"{a}.jsonl.gz" not in archivos_crudos()]:
    destino = f"{RUTA_CRUDOS}/{anio}.jsonl.gz"
    try:
        urllib.request.urlretrieve(URL_DESCARGA.format(anio=anio), destino + ".parcial")
        os.replace(destino + ".parcial", destino)
        print(f"{anio}: descargado")
    except Exception as error:
        if os.path.exists(destino + ".parcial"):
            os.remove(destino + ".parcial")
        print(f"Sin acceso a la fuente ({type(error).__name__}): subir los archivos al volumen desde la interfaz")
        break

ARCHIVOS = archivos_crudos()
MB = {f: round(os.path.getsize(f"{RUTA_CRUDOS}/{f}") / 1024**2, 1) for f in ARCHIVOS}
print(f"{len(ARCHIVOS)} archivos, {sum(MB.values()):,.0f} MB comprimidos: {', '.join(ARCHIVOS)}")
assert ARCHIVOS, f"No hay archivos AAAA.jsonl.gz en {RUTA_CRUDOS}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Esquema de lectura
# MAGIC Se declara el esquema en lugar de inferirlo: inferir obliga a leer los 18 GB descomprimidos dos veces.
# MAGIC Solo se declaran las ramas OCDS que usa el proyecto; el resto se descarta al leer. Cada `.gz` lo lee
# MAGIC una tarea distinta, así que los 16 años se procesan en paralelo. Una línea con JSON inválido no
# MAGIC detiene la carga: queda en `_corrupt_record`.

# COMMAND ----------

ESQUEMA_OCDS = """
ocid STRING,
date STRING,
buyer STRUCT<id: STRING, name: STRING>,
parties ARRAY<STRUCT<id: STRING, name: STRING, roles: ARRAY<STRING>,
                     details: STRUCT<level: STRING, entityType: STRING>>>,
tender STRUCT<
    status: STRING,
    datePublished: STRING,
    procurementMethod: STRING,
    procurementMethodDetails: STRING,
    mainProcurementCategoryDetails: STRING,
    value: STRUCT<amount: DOUBLE, currency: STRING>,
    numberOfTenderers: BIGINT,
    tenderers: ARRAY<STRUCT<id: STRING>>,
    tenderPeriod: STRUCT<startDate: STRING, endDate: STRING>>,
bids STRUCT<details: ARRAY<STRUCT<tenderers: ARRAY<STRUCT<id: STRING>>>>>,
awards ARRAY<STRUCT<
    id: STRING, status: STRING, date: STRING,
    value: STRUCT<amount: DOUBLE, currency: STRING>,
    suppliers: ARRAY<STRUCT<id: STRING, name: STRING>>,
    items: ARRAY<STRUCT<
        id: STRING, description: STRING, quantity: DOUBLE,
        unit: STRUCT<value: STRUCT<amount: DOUBLE, currency: STRING>>,
        classification: STRUCT<id: STRING, description: STRING>>>>>,
contracts ARRAY<STRUCT<
    id: STRING, awardID: STRING, status: STRING, dateSigned: STRING,
    value: STRUCT<amount: DOUBLE, currency: STRING>,
    period: STRUCT<startDate: STRING>>>,
_corrupt_record STRING
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Carga incremental e idempotente a `bronce_ocds`
# MAGIC Como en la clase 8, se comparan los archivos del volumen con los `_archivo` ya cargados. La diferencia es
# MAGIC que aquí se descartan **antes de leerlos** (en la clase se leían y luego se hacía un `left_anti`): así no
# MAGIC se descomprimen de nuevo 1,7 GB solo para descubrir que ya estaban.
# MAGIC
# MAGIC **Particionado por `anio_archivo`.** Cada archivo anual queda en su propia carpeta: se puede auditar o
# MAGIC recargar un año sin leer los demás, y son solo 16 particiones de tamaño parecido (55 a 207 MB comprimidos).

# COMMAND ----------

T_BRONCE = "bronce_ocds"


def cargar_bronce():
    rutas = {f"{RUTA_CRUDOS}/{f}": f for f in ARCHIVOS}
    if tabla_existe(T_BRONCE):
        ya_cargados = {r[0].split("/")[-1] for r in leer(T_BRONCE).select("_archivo").distinct().collect()}
        rutas = {ruta: f for ruta, f in rutas.items() if f not in ya_cargados}
    if not rutas:
        return 0
    llegados = (spark.read.schema(ESQUEMA_OCDS)
                .option("mode", "PERMISSIVE")
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .json(list(rutas))
                .select("*",
                        F.col("_metadata.file_path").alias("_archivo"),     # ruta del archivo de origen
                        F.current_timestamp().alias("_ingesta"))            # momento de la carga
                .withColumn("anio_archivo", F.expr(r"try_cast(regexp_extract(_archivo, '(\\d{4})\\.jsonl', 1) AS INT)")))
    antes = leer(T_BRONCE).count() if tabla_existe(T_BRONCE) else 0
    despues = guardar_tabla(llegados, T_BRONCE, particion="anio_archivo", modo="append",
                            comentario="Registros OCDS de la DNCP tal como llegaron, con archivo de origen e ingesta")
    return despues - antes


print("Filas nuevas en bronce:", f"{cargar_bronce():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Prueba de idempotencia:** se vuelve a ejecutar la carga. Debe dar 0 filas nuevas.

# COMMAND ----------

print("Filas nuevas en bronce (segunda ejecución):", f"{cargar_bronce():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Control de la carga

# COMMAND ----------

control = (leer(T_BRONCE)
           .groupBy("anio_archivo", "_archivo")
           .agg(F.count("*").alias("procesos"),
                F.countDistinct("ocid").alias("ocid_distintos"),
                F.sum(F.col("_corrupt_record").isNotNull().cast("int")).alias("lineas_invalidas"),
                F.min("_ingesta").alias("ingesta"))
           .withColumn("archivo", F.element_at(F.split("_archivo", "/"), -1))
           .join(spark.createDataFrame([(f, mb) for f, mb in MB.items()], "archivo STRING, mb_comprimido DOUBLE"),
                 "archivo", "left")
           .select("anio_archivo", "archivo", "mb_comprimido", "procesos", "ocid_distintos", "lineas_invalidas", "ingesta")
           .orderBy("anio_archivo"))
guardar_tabla(control, "bronce_control_carga", comentario="Filas cargadas por archivo anual")
mostrar(leer("bronce_control_carga"))

total = leer(T_BRONCE).agg(F.count("*").alias("filas"), F.countDistinct("ocid").alias("ocid")).first()
print(f"bronce_ocds: {total.filas:,} filas · {total.ocid:,} procesos distintos · "
      f"{total.filas - total.ocid:,} repetidos entre archivos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. La tabla es Delta
# MAGIC `DESCRIBE DETAIL` muestra el formato, las columnas de partición y la cantidad de archivos;
# MAGIC `DESCRIBE HISTORY` muestra cada escritura (la segunda carga no agregó una versión porque no había nada nuevo).

# COMMAND ----------

if EN_DATABRICKS:
    display(spark.sql(f"DESCRIBE DETAIL {tabla(T_BRONCE)}")
            .select("format", "partitionColumns", "numFiles", "sizeInBytes"))
    display(spark.sql(f"DESCRIBE HISTORY {tabla(T_BRONCE)}")
            .select("version", "timestamp", "operation", "operationMetrics"))
