# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Capa oro: consultas de la propuesta y rendimiento
# MAGIC
# MAGIC **Pasos 4, 5 y 6 del Avance 1.** Responde las siete preguntas de la propuesta (clase 4) sobre las tablas
# MAGIC de plata, con Spark SQL (P1, P2, P3, P6) y con la API de DataFrame (P4, P5, P7). Cada resultado se guarda
# MAGIC como tabla `oro_*` con `overwrite` (idempotente): son las tablas que usa el dashboard.
# MAGIC
# MAGIC **Rendimiento (clase 7).** Cada consulta se ejecuta dos veces (la primera puede incluir el arranque del
# MAGIC cómputo) y se registra su plan físico con `explain(mode="formatted")`. Debajo de cada celda, el enlace
# MAGIC *See performance* abre el perfil de la consulta de Databricks. Al final se aplican cuatro optimizaciones
# MAGIC con el método de la clase: medir, diagnosticar, **cambiar una sola cosa** y volver a medir.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

PROCESOS = tabla("plata_procesos")
GRUPOS_P7 = ["combustible", "medicamentos", "alimentos"]

if EN_DATABRICKS:
    dbutils.widgets.text("ruc", "80001556-8", "RUC a consultar (P4)")  # noqa: F821
    RUC_CONSULTA = dbutils.widgets.get("ruc").strip()  # noqa: F821
else:
    RUC_CONSULTA = os.environ.get("RUC_CONSULTA", "80001556-8")

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1. ¿Cuántos procesos, adjudicaciones y contratos hay por año y por modalidad, y cómo evolucionó el monto adjudicado?
# MAGIC Los procesos que solo llegaron a la planificación (sin llamado publicado) se cuentan aparte.

# COMMAND ----------

p1 = spark.sql(f"""
    SELECT CAST(anio AS STRING)                                AS anio,
           COALESCE(modalidad, '(solo planificación)')         AS modalidad,
           COUNT(*)                                            AS procesos,
           SUM(CASE WHEN solo_planificacion THEN 0 ELSE 1 END) AS llamados,
           SUM(n_adjudicaciones)                               AS adjudicaciones,
           SUM(n_contratos)                                    AS contratos,
           SUM(monto_adjudicado_pyg)                           AS monto_adjudicado_pyg
    FROM {PROCESOS}
    GROUP BY 1, 2
    ORDER BY 1, 2""")
medir("P1 año x modalidad", p1)
guardar_tabla(p1, "oro_p1_anio_modalidad", comentario="P1: procesos, adjudicaciones, contratos y monto por anio y modalidad")

guardar_tabla(spark.sql(f"""
    SELECT anio,
           SUM(llamados)                              AS llamados,
           SUM(procesos) - SUM(llamados)              AS solo_planificacion,
           SUM(adjudicaciones)                        AS adjudicaciones,
           SUM(contratos)                             AS contratos,
           ROUND(SUM(monto_adjudicado_pyg) / 1e12, 3) AS monto_billones_pyg
    FROM {tabla('oro_p1_anio_modalidad')}
    GROUP BY anio ORDER BY anio"""), "oro_p1_anio", comentario="P1: totales por anio")
mostrar(leer("oro_p1_anio"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2. ¿Qué proporción de los llamados recibió una sola oferta, y cómo varía por modalidad y tipo de entidad?

# COMMAND ----------

def p2_por(dimension, minimo):
    return spark.sql(f"""
        SELECT COALESCE(CAST({dimension} AS STRING), '(sin dato)')               AS {dimension},
               COUNT(*)                                                         AS llamados_con_oferta,
               SUM(CASE WHEN una_sola_oferta THEN 1 ELSE 0 END)                 AS una_sola_oferta,
               ROUND(100 * AVG(CASE WHEN una_sola_oferta THEN 1 ELSE 0 END), 2) AS pct_una_oferta
        FROM {PROCESOS}
        WHERE n_oferentes >= 1
        GROUP BY 1
        HAVING COUNT(*) >= {minimo}
        ORDER BY pct_una_oferta DESC""")


for dimension, minimo in [("modalidad", 100), ("tipo_entidad", 0), ("anio", 0)]:
    df = p2_por(dimension, minimo)
    medir(f"P2 una sola oferta por {dimension}", df)
    guardar_tabla(df, f"oro_p2_{dimension}", comentario=f"P2: porcentaje de llamados con una sola oferta por {dimension}")
mostrar(leer("oro_p2_tipo_entidad"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3. ¿Cuál es la diferencia porcentual entre el monto estimado y el adjudicado, y en qué categorías se concentran los desvíos más altos?
# MAGIC Se informan medianas y proporciones porque hay valores extremos (montos estimados incompletos o en otra unidad).

# COMMAND ----------

p3_resumen = spark.sql(f"""
    SELECT COUNT(*)                                                          AS procesos,
           ROUND(PERCENTILE(desvio_pct, 0.25), 2)                            AS p25,
           ROUND(PERCENTILE(desvio_pct, 0.5), 2)                             AS mediana,
           ROUND(PERCENTILE(desvio_pct, 0.75), 2)                            AS p75,
           ROUND(100 * AVG(CASE WHEN desvio_pct > 0 THEN 1 ELSE 0 END), 2)   AS pct_sobre_estimado,
           ROUND(100 * AVG(CASE WHEN desvio_pct > 10 THEN 1 ELSE 0 END), 2)  AS pct_sobre_10,
           ROUND(100 * AVG(CASE WHEN desvio_pct < -10 THEN 1 ELSE 0 END), 2) AS pct_bajo_10
    FROM {PROCESOS}
    WHERE desvio_pct IS NOT NULL""")
medir("P3 resumen del desvío", p3_resumen)
guardar_tabla(p3_resumen, "oro_p3_resumen", comentario="P3: distribucion del desvio entre monto estimado y adjudicado")

p3_categoria = spark.sql(f"""
    SELECT categoria,
           COUNT(*)                                                         AS procesos,
           ROUND(PERCENTILE(desvio_pct, 0.5), 2)                            AS mediana,
           ROUND(100 * AVG(CASE WHEN desvio_pct > 10 THEN 1 ELSE 0 END), 2) AS pct_sobre_10,
           ROUND(100 * (SUM(monto_adjudicado_pyg) - SUM(monto_estimado_pyg)) / SUM(monto_estimado_pyg), 2)
                                                                            AS desvio_agregado_pct
    FROM {PROCESOS}
    WHERE desvio_pct IS NOT NULL AND categoria IS NOT NULL
    GROUP BY categoria
    HAVING COUNT(*) >= 100
    ORDER BY pct_sobre_10 DESC""")
medir("P3 desvío por categoría", p3_categoria)
guardar_tabla(p3_categoria, "oro_p3_categoria", comentario="P3: categorias con mayor proporcion de sobrecostos")
mostrar(leer("oro_p3_categoria"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4. Dado un proveedor (RUC), ¿qué contratos obtuvo, con qué entidades y por qué montos?
# MAGIC El RUC se elige en el widget de arriba del notebook. La optimización D acelera esta búsqueda.

# COMMAND ----------

contratos = leer("plata_contratos")
p4_top = (contratos.where(F.col("ruc").isNotNull())
          .groupBy("ruc")
          .agg(F.max("proveedor").alias("proveedor"),
               F.count("*").alias("contratos"),
               F.countDistinct("entidad_id").alias("entidades"),
               F.round(F.sum("monto_pyg") / 1e9, 3).alias("monto_miles_millones_pyg"))
          .orderBy(F.desc("monto_miles_millones_pyg"))
          .limit(50))
medir("P4 ranking de proveedores", p4_top)
guardar_tabla(p4_top, "oro_p4_top_proveedores", comentario="P4: 50 proveedores con mayor monto contratado")

p4_ruc = (contratos.where(F.col("ruc") == RUC_CONSULTA)
          .select("ruc", "proveedor", "ocid", "contrato_id", "fecha_firma", "entidad", "tipo_entidad", "modalidad",
                  "categoria", "monto", "moneda")
          .orderBy(F.desc("monto")))
medir(f"P4 contratos del RUC {RUC_CONSULTA}", p4_ruc)
guardar_tabla(p4_ruc, "oro_p4_contratos_ruc", comentario=f"P4: contratos del RUC {RUC_CONSULTA}")
mostrar(leer("oro_p4_contratos_ruc"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5. ¿Qué tan concentradas están las adjudicaciones en pocos proveedores (top 10 e índice Herfindahl-Hirschman)?
# MAGIC HHI = suma de los cuadrados de la participación (%) de cada proveedor en el monto adjudicado de la entidad
# MAGIC (de 0 a 10.000; más de 2.500 es concentración alta). Se usan funciones de ventana para la participación y
# MAGIC el puesto de cada proveedor. Se clasifican las entidades con 20 adjudicaciones o más.

# COMMAND ----------

adj = leer("plata_adjudicaciones").where(F.col("valida") & (F.col("monto_atribuido_pyg") > 0)
                                         & F.col("ruc").isNotNull() & F.col("entidad_id").isNotNull())


def concentracion(df, claves):
    por_proveedor = df.groupBy(*claves, "ruc").agg(F.sum("monto_atribuido_pyg").alias("monto"), F.count("*").alias("n"))
    total = Window.partitionBy(*claves)
    orden = Window.partitionBy(*claves).orderBy(F.desc("monto"), "ruc")
    cuotas = (por_proveedor.withColumn("cuota", 100 * F.col("monto") / F.sum("monto").over(total))
              .withColumn("puesto", F.row_number().over(orden)))
    return cuotas.groupBy(*claves).agg(
        F.sum("n").alias("adjudicaciones"),
        F.count("*").alias("proveedores"),
        F.round(F.sum("monto") / 1e9, 3).alias("monto_miles_millones_pyg"),
        F.round(F.sum(F.when(F.col("puesto") == 1, F.col("cuota"))), 2).alias("cuota_top1"),
        F.round(F.sum(F.when(F.col("puesto") <= 10, F.col("cuota"))), 2).alias("cuota_top10"),
        F.round(F.sum(F.col("cuota") * F.col("cuota")), 1).alias("hhi"))


nombres = leer("ref_entidades").select("entidad_id", "entidad", "tipo_entidad")
p5_entidades = (concentracion(adj, ["entidad_id"]).join(F.broadcast(nombres), "entidad_id", "left")
                .withColumn("nivel_hhi", F.when(F.col("hhi") < 1500, "1. no concentrado (<1500)")
                            .when(F.col("hhi") <= 2500, "2. moderado (1500-2500)").otherwise("3. alto (>2500)"))
                .orderBy(F.desc("monto_miles_millones_pyg")))
medir("P5 concentración por entidad", p5_entidades)
guardar_tabla(p5_entidades, "oro_p5_entidades", comentario="P5: cuota del top 1 y del top 10 e HHI por entidad")

guardar_tabla(leer("oro_p5_entidades").where(F.col("adjudicaciones") >= 20)
              .groupBy("nivel_hhi").agg(F.count("*").alias("entidades"),
                                        F.round(F.median("cuota_top10"), 2).alias("cuota_top10_mediana"))
              .orderBy("nivel_hhi"), "oro_p5_niveles_hhi", comentario="P5: entidades por nivel de concentracion")

p5_categorias = (concentracion(adj.where(F.col("categoria").isNotNull()), ["categoria"])
                 .where(F.col("adjudicaciones") >= 100).orderBy(F.desc("hhi")))
medir("P5 concentración por categoría", p5_categorias)
guardar_tabla(p5_categorias, "oro_p5_categorias", comentario="P5: concentracion por categoria")

total_estado = adj.agg(F.sum("monto_atribuido_pyg")).first()[0]
guardar_tabla(adj.groupBy("ruc").agg(F.max("proveedor").alias("proveedor"), F.sum("monto_atribuido_pyg").alias("monto"))
              .withColumn("cuota_pct", F.round(100 * F.col("monto") / F.lit(total_estado), 2))
              .withColumn("monto_miles_millones_pyg", F.round(F.col("monto") / 1e9, 3)).drop("monto")
              .orderBy(F.desc("cuota_pct")).limit(10),
              "oro_p5_top10_estado", comentario="P5: los 10 mayores proveedores del Estado")
mostrar(leer("oro_p5_niveles_hhi"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## P6. ¿Cuántos días dura la convocatoria por modalidad, y los plazos más cortos se asocian a menos oferentes?
# MAGIC La asociación se mide con la correlación de Spearman (la correlación de los rangos, con rango promedio en
# MAGIC los empates), calculada en SQL con funciones de ventana.

# COMMAND ----------

p6_modalidad = spark.sql(f"""
    SELECT modalidad,
           COUNT(*)                            AS procesos,
           ROUND(AVG(dias_convocatoria), 1)    AS media,
           PERCENTILE(dias_convocatoria, 0.25) AS p25,
           PERCENTILE(dias_convocatoria, 0.5)  AS mediana,
           PERCENTILE(dias_convocatoria, 0.75) AS p75
    FROM {PROCESOS}
    WHERE dias_convocatoria IS NOT NULL AND modalidad IS NOT NULL
    GROUP BY modalidad
    HAVING COUNT(*) >= 100
    ORDER BY procesos DESC""")
medir("P6 días por modalidad", p6_modalidad)
guardar_tabla(p6_modalidad, "oro_p6_dias_modalidad", comentario="P6: dias de convocatoria por modalidad")

p6_tramos = spark.sql(f"""
    SELECT tramo_dias,
           COUNT(*)                                                         AS llamados,
           ROUND(AVG(n_oferentes), 2)                                       AS oferentes_promedio,
           ROUND(100 * AVG(CASE WHEN una_sola_oferta THEN 1 ELSE 0 END), 2) AS pct_una_oferta
    FROM {PROCESOS}
    WHERE dias_convocatoria IS NOT NULL AND n_oferentes >= 1
    GROUP BY tramo_dias ORDER BY tramo_dias""")
medir("P6 oferentes por tramo de días", p6_tramos)
guardar_tabla(p6_tramos, "oro_p6_tramos", comentario="P6: oferta unica y oferentes por tramo de dias")

p6_spearman = spark.sql(f"""
    WITH base AS (
        SELECT modalidad, CAST(dias_convocatoria AS DOUBLE) AS x, CAST(n_oferentes AS DOUBLE) AS y
        FROM {PROCESOS} WHERE dias_convocatoria IS NOT NULL AND n_oferentes >= 1),
    principales AS (
        SELECT modalidad FROM base WHERE modalidad IS NOT NULL
        GROUP BY modalidad ORDER BY COUNT(*) DESC LIMIT 6),
    grupos AS (
        SELECT '(todas)' AS grupo, x, y FROM base
        UNION ALL
        SELECT b.modalidad AS grupo, x, y FROM base b JOIN principales p ON b.modalidad = p.modalidad),
    rangos AS (
        SELECT grupo,
               RANK() OVER (PARTITION BY grupo ORDER BY x) + (COUNT(*) OVER (PARTITION BY grupo, x) - 1) / 2.0 AS rx,
               RANK() OVER (PARTITION BY grupo ORDER BY y) + (COUNT(*) OVER (PARTITION BY grupo, y) - 1) / 2.0 AS ry
        FROM grupos)
    SELECT grupo AS modalidad, COUNT(*) AS llamados, ROUND(CORR(rx, ry), 4) AS spearman
    FROM rangos GROUP BY grupo ORDER BY llamados DESC""")
medir("P6 correlación de Spearman", p6_spearman)
guardar_tabla(p6_spearman, "oro_p6_spearman", comentario="P6: correlacion de Spearman entre dias y oferentes")
mostrar(leer("oro_p6_tramos"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## P7. ¿Qué se compra realmente? Gasto en ítems que mencionan "combustible", "medicamentos" o "alimentos"
# MAGIC Se compara la búsqueda literal en la descripción (lo que plantea la pregunta) con la clasificación del
# MAGIC catálogo UNSPSC (`grupo_catalogo`, agregada en plata con un join a `ref_grupos_gasto`).

# COMMAND ----------

items = leer("plata_items").where(F.col("adjudicacion_valida") & F.col("monto_pyg").isNotNull())

partes = []
for grupo in GRUPOS_P7:
    for metodo, condicion in [("texto de la descripción", F.col(f"menciona_{grupo}")),
                              ("catálogo UNSPSC", F.col("grupo_catalogo") == grupo)]:
        partes.append(items.where(condicion).agg(
            F.lit(grupo).alias("grupo"), F.lit(metodo).alias("metodo"),
            F.count("*").alias("items"), F.countDistinct("ocid").alias("procesos"),
            F.round(F.sum("monto_pyg") / 1e12, 3).alias("monto_billones_pyg")))
p7_resumen = partes[0]
for parte in partes[1:]:
    p7_resumen = p7_resumen.unionByName(parte)
medir("P7 texto vs catálogo", p7_resumen)
guardar_tabla(p7_resumen, "oro_p7_resumen", comentario="P7: items y monto segun el texto de la descripcion y segun el catalogo")

por_grupo = Window.partitionBy("grupo_catalogo")
orden = Window.partitionBy("grupo_catalogo").orderBy(F.desc("monto"))
p7_entidades = (items.where(F.col("grupo_catalogo").isNotNull() & F.col("entidad").isNotNull())
                .groupBy("grupo_catalogo", "entidad").agg(F.sum("monto_pyg").alias("monto"))
                .withColumn("cuota_pct", F.round(100 * F.col("monto") / F.sum("monto").over(por_grupo), 2))
                .withColumn("puesto", F.row_number().over(orden))
                .where(F.col("puesto") <= 5)
                .withColumn("monto_billones_pyg", F.round(F.col("monto") / 1e12, 3))
                .select(F.col("grupo_catalogo").alias("grupo"), "puesto", "entidad", "monto_billones_pyg", "cuota_pct")
                .orderBy("grupo", "puesto"))
medir("P7 entidades que concentran el gasto", p7_entidades)
guardar_tabla(p7_entidades, "oro_p7_top_entidades", comentario="P7: las 5 entidades con mayor gasto por grupo (catalogo)")

guardar_tabla(items.agg(*[F.sum(F.col(f"menciona_{g}").cast("int")).alias(f"{g}_texto") for g in GRUPOS_P7],
                        *[F.sum((F.col(f"menciona_{g}") & (F.col("grupo_catalogo") == g)).cast("int"))
                          .alias(f"{g}_texto_y_catalogo") for g in GRUPOS_P7]),
              "oro_p7_cruce", comentario="P7: items hallados por texto que el catalogo clasifica en el mismo grupo")
mostrar(leer("oro_p7_resumen"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimizaciones: medir, diagnosticar, cambiar una cosa, volver a medir
# MAGIC
# MAGIC | | Consulta | Diagnóstico | Antes | Después (un solo cambio) |
# MAGIC |---|---|---|---|---|
# MAGIC | A | P7: buscar las palabras | La expresión regular se evalúa sobre 4,5 millones de descripciones en cada consulta | Regex al vuelo sobre la descripción | Marcas `menciona_*` calculadas una vez en plata (sin `cache()` en serverless, se materializa en Delta) |
# MAGIC | B | Procesos de 2025 por modalidad | Sin partición se leen todos los años | Copia sin particionar | Tabla particionada por `anio`: `PartitionFilters` lee solo 2025 |
# MAGIC | C | Gasto de los ítems por tipo de entidad | Join de 4,5 millones de ítems con una tabla de 377 entidades | `SortMergeJoin` forzado (2 shuffles) | `F.broadcast(ref_entidades)`: `BroadcastHashJoin`, los ítems no se mueven |
# MAGIC | D | Contratos de un RUC (P4) | El filtro por RUC recorre todos los archivos | Tabla particionada por año | *Liquid clustering* por `ruc` (`CLUSTER BY` + `OPTIMIZE`): Delta saltea archivos con las estadísticas |

# COMMAND ----------

def regex_al_vuelo():
    crudo = leer("plata_items").where(F.col("adjudicacion_valida") & (F.col("moneda") == "PYG"))
    texto = F.translate(F.lower(F.col("descripcion")), "áéíóúüñàèìòù", "aeiouunaeiou")
    palabras = {"combustible": "combustibles?", "medicamentos": "medicamentos?", "alimentos": "alimentos?"}
    return crudo.agg(*[F.sum(F.when(texto.rlike(r"(?U)\b(" + p + r")\b"), F.col("monto_pyg"))).alias(g)
                       for g, p in palabras.items()])


def marcas_precalculadas():
    return items.agg(*[F.sum(F.when(F.col(f"menciona_{g}"), F.col("monto_pyg"))).alias(g) for g in GRUPOS_P7])


medir("A · P7 búsqueda de texto", regex_al_vuelo(), repeticiones=3, etapa="antes")
medir("A · P7 búsqueda de texto", marcas_precalculadas(), repeticiones=3, etapa="después")
print("Mismo resultado:", [round(v or 0) for v in regex_al_vuelo().first()] ==
      [round(v or 0) for v in marcas_precalculadas().first()])

# COMMAND ----------

guardar_tabla(leer("plata_procesos"), "plata_procesos_sin_particion",
              comentario="Copia sin particionar de plata_procesos, para la optimizacion B")


def un_anio(nombre):
    return (leer(nombre).where(F.col("anio") == 2025)
            .groupBy("modalidad").agg(F.count("*").alias("procesos"), F.sum("monto_adjudicado_pyg").alias("monto")))


medir("B · procesos de 2025 por modalidad", un_anio("plata_procesos_sin_particion"), repeticiones=3, etapa="antes")
medir("B · procesos de 2025 por modalidad", un_anio("plata_procesos"), repeticiones=3, etapa="después")

# COMMAND ----------

def gasto_por_tipo(ref):
    return (items.join(ref, "entidad_id")
            .groupBy(F.col("tipo_entidad_ref")).agg(F.sum("monto_pyg").alias("monto")))


ref = leer("ref_entidades").select("entidad_id", F.col("tipo_entidad").alias("tipo_entidad_ref"))
medir("C · gasto por tipo de entidad (join)", gasto_por_tipo(ref.hint("merge")), repeticiones=3, etapa="antes")
medir("C · gasto por tipo de entidad (join)", gasto_por_tipo(F.broadcast(ref)), repeticiones=3, etapa="después")

# COMMAND ----------

sql_databricks(f"CREATE OR REPLACE TABLE {tabla('plata_contratos_por_ruc')} CLUSTER BY (ruc) "
               f"AS SELECT * FROM {tabla('plata_contratos')}")
sql_databricks(f"OPTIMIZE {tabla('plata_contratos_por_ruc')}")
CLUSTERIZADA = "plata_contratos_por_ruc" if EN_DATABRICKS else "plata_contratos"


def contratos_de(nombre):
    return (leer(nombre).where(F.col("ruc") == RUC_CONSULTA)
            .agg(F.count("*").alias("contratos"), F.countDistinct("entidad_id").alias("entidades"),
                 F.sum("monto_pyg").alias("monto_pyg")))


medir("D · contratos de un RUC", contratos_de("plata_contratos"), repeticiones=3, etapa="antes")
medir("D · contratos de un RUC", contratos_de(CLUSTERIZADA), repeticiones=3, etapa="después")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultados de rendimiento

# COMMAND ----------

guardar_tabla(spark.createDataFrame(REGISTRO_RENDIMIENTO,
                                    "consulta STRING, etapa STRING, filas_resultado BIGINT, tiempo_1_s DOUBLE, "
                                    "tiempo_2_s DOUBLE, tiempo_min_s DOUBLE, resumen_plan STRING, plan STRING"),
              "oro_rendimiento_consultas", comentario="Tiempos de cada ejecucion y plan fisico de cada consulta")

guardar_tabla(spark.sql(f"""
    SELECT consulta                                                         AS optimizacion,
           MAX(CASE WHEN etapa = 'antes' THEN tiempo_min_s END)             AS tiempo_antes_s,
           MAX(CASE WHEN etapa = 'después' THEN tiempo_min_s END)           AS tiempo_despues_s,
           ROUND(100 * (1 - MAX(CASE WHEN etapa = 'después' THEN tiempo_min_s END)
                          / MAX(CASE WHEN etapa = 'antes' THEN tiempo_min_s END)), 1) AS mejora_pct,
           MAX(CASE WHEN etapa = 'antes' THEN resumen_plan END)             AS plan_antes,
           MAX(CASE WHEN etapa = 'después' THEN resumen_plan END)           AS plan_despues
    FROM {tabla('oro_rendimiento_consultas')}
    WHERE etapa IN ('antes', 'después')
    GROUP BY consulta ORDER BY consulta"""),
              "oro_rendimiento_optimizaciones", comentario="Optimizaciones: tiempo antes y despues de cambiar una sola cosa")
mostrar(leer("oro_rendimiento_consultas").drop("plan"), n=50)
mostrar(leer("oro_rendimiento_optimizaciones"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Planes físicos para el informe
# MAGIC Dos planes completos: la optimización B (buscar `PartitionFilters`, que muestra que solo se lee 2025) y la
# MAGIC optimización C (`SortMergeJoin` con dos `Exchange` frente a `BroadcastHashJoin` con un `BroadcastExchange`).
# MAGIC Los planes de todas las consultas quedan en la columna `plan` de `oro_rendimiento_consultas`.

# COMMAND ----------

un_anio("plata_procesos").explain(mode="formatted")

# COMMAND ----------

gasto_por_tipo(ref.hint("merge")).explain()
gasto_por_tipo(F.broadcast(ref)).explain()
