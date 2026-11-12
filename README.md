# Tarea 1 · Contrataciones públicas de la DNCP con herramientas de Big Data

Maestría en Ciencia de Datos · Universidad Comunera · Análisis y procesamiento de datos masivos
Autores: Bethania Avila, Arturo Fernandez, Gustavo Bogado

Responde las 7 preguntas de la propuesta sobre el histórico OCDS de la DNCP (2011–2026, 246.686 procesos,
18 GB de JSON descomprimido). Cada exportable se calcula con una herramienta y se comprueba con otra.

**Entregables:**
- Tarea 1: `Tarea 1 - desarrollo DNCP.docx` (la propuesta original con el desarrollo agregado al final).
- Proyecto final, Avance 1 (Databricks): carpeta `databricks/` (notebooks bronce, plata y oro, y dashboard).
**Verificación:** 29 de 29 comparaciones OK en el histórico completo (`resultados/full/verificacion.csv`).

| Pregunta | Cálculo | Comprobación |
|---|---|---|
| P1 Procesos, adjudicaciones y contratos por año y modalidad | Spark | MapReduce (parseo independiente del JSON), Hive y DuckDB |
| P2 Llamados con una sola oferta | Spark | MapReduce, Hive y Polars |
| P3 Desvío entre monto estimado y adjudicado | Spark | Hive y DuckDB |
| P4 Contratos de un proveedor (RUC) en milisegundos | Spark | Elasticsearch y HBase |
| P5 Concentración (top 10 y HHI) | Spark | Hive y DuckDB |
| P6 Plazo de convocatoria y oferentes | Spark (MLlib) | Hive, Polars y pandas |
| P7 Gasto en combustible, medicamentos y alimentos | Spark | Elasticsearch |

## Estructura

```
src/
  comun.py                rutas, sesión de Spark y utilidades
  01_aplanar_spark.py     Spark: JSON Lines OCDS -> 4 tablas Parquet
  02_preguntas_spark.py   Spark: las 7 preguntas
  03_mapreduce.py         MapReduce: P1 y P2 (Hadoop Streaming, o el mismo flujo en local)
  mapreduce/mapper_entidades.py, reducer_entidades.py   trabajo 1: tabla entidad -> tipo
  mapreduce/mapper.py, reducer.py                       trabajo 2: P1 y P2 (usa la tabla del trabajo 1
                                                        como caché distribuida)
  hive/preguntas.hql      HiveQL: tablas externas y consultas de P1, P2, P3, P5, P6
  04_hive.py              ejecuta el HiveQL (beeline en Hive real, o motor SQL de Spark)
  05_elasticsearch.py     Elasticsearch: índices y consultas de P4 y P7
  05b_hbase.py            HBase: tabla por RUC y consultas de P4 (Colab)
  06_verificar.py         comparación cruzada de todos los exportables
  07_duckdb_polars.py     DuckDB y Polars (clase 4) y comparación de formatos (clase 3)
informe/
  graficos.py             figuras del informe
logs/                     salida de la última ejecución de cada paso
colab/
  tarea1_bigdata_dncp_colab.ipynb   pipeline completo con Hadoop, Hive, HBase, Spark y Elasticsearch reales
databricks/                Avance 1 en Databricks: notebooks bronce, plata y oro, y dashboard
databricks_pruebas/        prueba local de los notebooks de Databricks (no se sube a Databricks)
resultados/<alcance>/<herramienta>/*.csv   exportables
resultados/<alcance>/verificacion.csv      estado de cada comparación
```

## Ejecución local (Windows)

Requisitos: Python 3.13, Java 23, `pip install pyspark==4.0.1 duckdb orjson elasticsearch pandas pyarrow`
y Elasticsearch 9.5.1 para Windows (`bin\elasticsearch.bat`).

```bash
export ALCANCE=full            # o 2025
./run.sh src/01_aplanar_spark.py
./run.sh src/02_preguntas_spark.py
./run.sh src/03_mapreduce.py
./run.sh src/04_hive.py
./run.sh src/05_elasticsearch.py
./run.sh src/07_duckdb_polars.py
./run.sh src/06_verificar.py
python informe/graficos.py
```

`run.sh` solo filtra el ruido de logs de Spark. Los datos (`datos/full.jsonl`, 18 GB) y el Parquet quedan
fuera de OneDrive, en `C:/Users/betha/dncp_bigdata` (variable `DNCP_BASE`).

Limitaciones de Windows y cómo se resolvieron:

- Hadoop, Hive y HBase no corren de forma nativa en Windows. En local, MapReduce se ejecuta con el mismo
  mapper y reducer de Hadoop Streaming (splits de 128 MB, mappers en paralelo, ordenamiento por clave y
  reducer); el HiveQL se ejecuta con el motor SQL de Spark. HBase solo se ejecuta en Colab.
- Spark en Windows necesita `winutils.exe` para escribir con Hadoop y sus workers de Python fallan con
  Python 3.13. Las tablas se escriben con PyArrow desde el driver y no se usan UDF de Python.
- Java 23 requiere `-Djava.security.manager=allow` para el cliente de Hadoop de Spark (lo fija `comun.py`).

## Ejecución en Colab con las herramientas reales

Subir `colab/tarea1_bigdata_dncp_colab.ipynb` a Colab y ejecutar todas las celdas. Instala Java 8,
Hadoop 3.3.6 (HDFS + MapReduce), Hive 3.1.3, HBase 2.5.10, Spark 3.5 y Elasticsearch 9.5.1, carga los datos
en HDFS y corre los mismos scripts. Al final descarga `resultados_<alcance>.zip` con la verificación.

## Ejecución en Databricks (Avance 1)

Subir los 16 archivos anuales (`2011.jsonl.gz` a `2026.jsonl.gz`) al volumen `workspace.dncp.crudos`,
importar los notebooks de `databricks/` en una misma carpeta y ejecutar `01_bronce`, `02_plata` y
`03_oro_y_rendimiento` en ese orden con cómputo serverless. El dashboard se importa desde
`databricks/dashboard_dncp.lvdash.json` (Dashboards › Import dashboard from file).

Los scripts del notebook son los mismos que se ejecutaron y verificaron localmente, pero las celdas de
instalación y arranque de Hadoop, Hive y HBase no pudieron probarse en Windows: conviene correr el notebook
con `ALCANCE = "2025"` y comparar su `verificacion.csv` y sus CSV con los de `resultados/2025/`.

Fuente de datos: DNCP vía OCP Data Registry, https://data.open-contracting.org/en/publication/63 (CC BY 4.0).
