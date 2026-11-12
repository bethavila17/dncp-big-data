# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Capa plata: limpieza inicial y enriquecimiento
# MAGIC
# MAGIC **Paso 3 del Avance 1.** Lee `bronce_ocds` (que no se modifica) y escribe tablas **nuevas**:
# MAGIC
# MAGIC | Tabla | Una fila por | Cuarentena |
# MAGIC |---|---|---|
# MAGIC | `plata_procesos` | proceso de contratación (ocid) | `cuarentena_procesos` |
# MAGIC | `plata_adjudicaciones` | adjudicación y proveedor | `cuarentena_adjudicaciones` |
# MAGIC | `plata_contratos` | contrato | `cuarentena_contratos` |
# MAGIC | `plata_items` | ítem adjudicado | `cuarentena_items` |
# MAGIC
# MAGIC Siguiendo la clase 8, cada tabla pasa por cuatro pasos:
# MAGIC 1. **Aplanar y tipar:** el JSON anidado se convierte en columnas; fechas y años con `try_cast`, que
# MAGIC    devuelve nulo en lugar de detener todo (con el modo ANSI, un `cast` común falla ante un valor inválido).
# MAGIC 2. **Validar con `REGLAS`:** cada regla es una condición que debe cumplir una fila válida. Las que no la
# MAGIC    cumplen van a **cuarentena** con la lista de reglas que fallan (`_errores`); no se borran.
# MAGIC 3. **Corregir valores:** errores que no invalidan la fila (un plazo negativo, un texto con tabuladores)
# MAGIC    se corrigen o se dejan nulos, y se cuentan. Mandar a cuarentena un proceso por una fecha mal cargada
# MAGIC    lo sacaría también de las preguntas que no usan esa fecha.
# MAGIC 4. **Enriquecer (data augmentation):** columnas derivadas y dos tablas de referencia unidas con joins.
# MAGIC
# MAGIC Todo lo que se corrigió, cuántas filas afectó y por qué queda en `plata_calidad`. Plata se recalcula
# MAGIC completa desde bronce en cada ejecución (`overwrite`), así que también es idempotente.
# MAGIC
# MAGIC **Particionado por `anio`** (año de inicio de la convocatoria): las preguntas 1, 2 y 6 y el dashboard
# MAGIC agrupan y filtran por año; son 17 valores con particiones de tamaño parecido. No conviene particionar por
# MAGIC entidad, modalidad o proveedor: tienen cientos o miles de valores y generarían miles de archivos chicos.
# MAGIC Databricks recomienda no particionar tablas de menos de 1 TB; aquí el beneficio esperado es la poda de
# MAGIC particiones al filtrar por año, que se mide en el notebook 03.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

CALIDAD = []   # (tabla, tipo, paso, filas afectadas, filas totales, motivo)


def anotar(tabla_, tipo, paso, filas, total, motivo):
    CALIDAD.append((tabla_, tipo, paso, int(filas or 0), int(total), round(100 * (filas or 0) / max(total, 1), 2), motivo))
    print(f"[{tabla_}] {paso}: {int(filas or 0):,} de {total:,}")


def validar(df, reglas):
    """Agrega `_errores`: la lista de reglas que la fila NO cumple (vacía si es válida). Igual que en la clase 8."""
    fallas = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(nombre)) for nombre, cond in reglas.items()]
    return df.withColumn("_errores", F.array_compact(F.array(*fallas)))


def separar(validado, nombre, reglas, clave=None):
    """Filas inválidas a cuarentena (con `_errores`); devuelve las válidas sin duplicados por `clave`.

    Sin cache() en el cómputo serverless, cada conteo vuelve a leer bronce: los conteos por regla se
    toman de la tabla de cuarentena ya guardada, que es chica.
    """
    n_cuarentena = guardar_tabla(validado.where(F.size("_errores") > 0), f"cuarentena_{nombre}",
                                 comentario=f"Filas de {nombre} que no cumplen las reglas de calidad")
    validos = validado.where(F.size("_errores") == 0).drop("_errores")
    n_validos = validos.count()
    total = n_validos + n_cuarentena
    por_regla = {r["regla"]: r["count"] for r in
                 leer(f"cuarentena_{nombre}").select(F.explode("_errores").alias("regla")).groupBy("regla").count().collect()}
    for regla in reglas:
        anotar(nombre, "regla (cuarentena)", regla, por_regla.get(regla, 0), total, "La fila no cumple la regla")
    if clave:
        sin_duplicados = validos.dropDuplicates(clave)
        anotar(nombre, "corrección", f"duplicados por {', '.join(clave)} eliminados",
               n_validos - sin_duplicados.count(), total, "Cada registro debe aparecer una sola vez")
        validos = sin_duplicados
    return validos


def limpiar(col):
    """Espacios, tabuladores y saltos de línea seguidos -> un espacio; texto vacío -> nulo."""
    texto = F.trim(F.regexp_replace(col, r"\s+", " "))
    return F.when(texto != "", texto)


def contar(df, **condiciones):
    """Cuenta en una sola pasada las filas que cumplen cada condición."""
    fila = df.agg(*[F.sum(c.cast("int")).alias(n) for n, c in condiciones.items()]).first()
    return {n: (fila[n] or 0) for n in condiciones}


bronce = leer("bronce_ocds").where(F.col("_corrupt_record").isNull())
anio = F.expr("try_cast(substring(coalesce(tender.tenderPeriod.startDate, tender.datePublished, date), 1, 4) AS INT)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. plata_procesos

# COMMAND ----------

comprador = F.filter("parties", lambda p: F.array_contains(p.roles, "buyer"))
validas = F.filter("awards", lambda a: a.status.isin("active", "closed"))
validas_pyg = F.filter(validas, lambda a: a.value.currency == "PYG")
t_ = F.col("tender")

procesos_tipados = bronce.select(
    "ocid", anio.alias("anio"), "_archivo",
    t_.isNotNull().alias("tiene_llamado"),
    F.expr("try_cast(substring(tender.tenderPeriod.startDate, 1, 10) AS DATE)").alias("fecha_inicio"),
    F.expr("try_cast(substring(tender.tenderPeriod.endDate, 1, 10) AS DATE)").alias("fecha_fin"),
    t_.procurementMethodDetails.alias("modalidad_original"),
    t_.mainProcurementCategoryDetails.alias("categoria_original"),
    t_.status.alias("estado_llamado"),
    F.col("buyer.id").alias("entidad_id"),
    F.col("buyer.name").alias("entidad_original"),
    F.try_element_at(comprador, F.lit(1)).details.level.alias("tipo_entidad_original"),
    t_.value.currency.alias("moneda_estimado"),
    t_.value.amount.alias("monto_estimado"),
    t_.numberOfTenderers.alias("oferentes_declarados"),
    F.coalesce(F.size(t_.tenderers), F.lit(0)).alias("oferentes_listados"),
    F.coalesce(F.size(F.array_distinct(F.flatten(F.transform("bids.details", lambda d: d.tenderers.id)))),
               F.lit(0)).alias("oferentes_en_ofertas"),
    F.coalesce(F.size(validas), F.lit(0)).alias("n_adjudicaciones"),
    F.coalesce(F.size(validas_pyg), F.lit(0)).alias("n_adjudicaciones_pyg"),
    F.coalesce(F.aggregate(validas_pyg, F.lit(0.0), lambda acc, a: acc + F.coalesce(a.value.amount, F.lit(0.0))),
               F.lit(0.0)).alias("monto_adjudicado_pyg"),
    F.coalesce(F.size(F.filter("contracts", lambda c: c.awardID.isNotNull())), F.lit(0)).alias("n_contratos"))

REGLAS_PROCESOS = {
    "ocid_presente": F.col("ocid").isNotNull(),
    "llamado_presente": F.col("tiene_llamado"),
    "anio_valido": F.col("anio").between(2010, 2026),
}
# Sin regla para la entidad: los llamados consolidados de varias entidades no traen un comprador único y
# concentran miles de adjudicaciones; mandarlos a cuarentena quitaría el 12% de las adjudicaciones.
procesos_validos = separar(validar(procesos_tipados, REGLAS_PROCESOS), "procesos", REGLAS_PROCESOS, clave=["ocid"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tabla de referencia `ref_entidades` (enriquecimiento)
# MAGIC Muchos procesos no traen el tipo de entidad, pero la misma entidad lo informa en otros procesos. Esta tabla
# MAGIC guarda, por entidad, su nombre más reciente y su tipo; se une a los procesos con un join (*broadcast*,
# MAGIC porque es chica: Spark la copia a cada máquina y la tabla grande no se mueve).

# COMMAND ----------

ref_entidades = (procesos_validos.groupBy("entidad_id")
                 .agg(F.max_by(limpiar(F.col("entidad_original")), F.col("anio")).alias("entidad"),
                      F.max(limpiar(F.col("tipo_entidad_original"))).alias("tipo_entidad"),
                      F.countDistinct(limpiar(F.col("tipo_entidad_original"))).alias("tipos_distintos"),
                      F.count("*").alias("procesos"), F.min("anio").alias("primer_anio"),
                      F.max("anio").alias("ultimo_anio")))
guardar_tabla(ref_entidades, "ref_entidades", comentario="Entidades compradoras: nombre mas reciente y tipo de entidad")
print("Entidades con más de un tipo informado:", leer("ref_entidades").where("tipos_distintos > 1").count())

# COMMAND ----------

entidades = leer("ref_entidades").select("entidad_id", F.col("entidad").alias("entidad_ref"),
                                         F.col("tipo_entidad").alias("tipo_ref"))
oferentes = F.greatest(F.coalesce(F.col("oferentes_declarados"), F.lit(0)),
                       F.col("oferentes_listados"), F.col("oferentes_en_ofertas"))
dias = F.datediff("fecha_fin", "fecha_inicio")

plata_procesos = (procesos_validos
    .join(F.broadcast(entidades), "entidad_id", "left")
    # correcciones
    .withColumn("modalidad", F.lower(limpiar(F.col("modalidad_original"))))
    .withColumn("categoria", limpiar(F.col("categoria_original")))
    .withColumn("entidad", F.coalesce(F.col("entidad_ref"), limpiar(F.col("entidad_original")),
                                      F.lit("(sin comprador único)")))
    .withColumn("tipo_entidad", F.coalesce(limpiar(F.col("tipo_entidad_original")), F.col("tipo_ref"), F.lit("Sin dato")))
    .withColumn("dias_convocatoria", F.when(dias.between(0, 365), dias))
    .withColumn("monto_estimado_pyg", F.when((F.col("moneda_estimado") == "PYG") & (F.col("monto_estimado") > 0),
                                             F.col("monto_estimado")))
    # columnas derivadas
    .withColumn("solo_planificacion", F.col("modalidad").isNull())
    .withColumn("n_oferentes", oferentes)
    .withColumn("una_sola_oferta", F.when(oferentes >= 1, oferentes == 1))
    .withColumn("tramo_dias", F.when(dias.between(0, 7), "0-7").when(dias.between(8, 14), "08-14")
                .when(dias.between(15, 21), "15-21").when(dias.between(22, 30), "22-30")
                .when(dias.between(31, 60), "31-60").when(dias.between(61, 365), "61-365"))
    .withColumn("desvio_pct", F.when((F.col("n_adjudicaciones_pyg") > 0) & (F.col("monto_adjudicado_pyg") > 0),
                                     F.try_divide(100 * (F.col("monto_adjudicado_pyg") - F.col("monto_estimado_pyg")),
                                                  F.col("monto_estimado_pyg"))))
    .withColumn("dias_convocatoria_original", dias)
    .drop("entidad_ref", "tipo_ref", "tiene_llamado"))

guardar_tabla(plata_procesos, "plata_procesos", particion="anio",
              comentario="Procesos validados, con modalidad normalizada, tipo de entidad completado y columnas derivadas")

total = leer("plata_procesos").count()
c = contar(leer("plata_procesos"),
           modalidad=F.col("modalidad_original").isNotNull() & (F.col("modalidad") != F.col("modalidad_original")),
           tipo=limpiar(F.col("tipo_entidad_original")).isNull() & (F.col("tipo_entidad") != "Sin dato"),
           sin_tipo=F.col("tipo_entidad") == "Sin dato",
           sin_comprador=F.col("entidad_id").isNull(),
           oferentes=F.col("oferentes_declarados").isNull() & (F.col("n_oferentes") > 0),
           sin_fechas=F.col("dias_convocatoria_original").isNull(),
           negativos=F.col("dias_convocatoria_original") < 0,
           mayor_365=F.col("dias_convocatoria_original") > 365,
           otra_moneda=F.col("moneda_estimado").isNotNull() & (F.col("moneda_estimado") != "PYG"),
           no_positivo=(F.col("moneda_estimado") == "PYG") & (F.col("monto_estimado") <= 0),
           planificacion=F.col("solo_planificacion"),
           desvio=F.col("desvio_pct").isNotNull())
for paso, clave, tipo, motivo in [
    ("modalidad normalizada", "modalidad", "corrección", "La misma modalidad venía con distintas mayúsculas, espacios o tabuladores"),
    ("tipo de entidad completado con ref_entidades", "tipo", "enriquecimiento", "El registro no traía el tipo; la entidad lo informa en otros procesos"),
    ("tipo de entidad sin dato", "sin_tipo", "corrección", "La entidad nunca informa su tipo: queda 'Sin dato'"),
    ("sin comprador único (llamados consolidados)", "sin_comprador", "corrección",
     "Licitaciones de varias entidades sin buyer: se conservan con entidad '(sin comprador único)'"),
    ("oferentes recuperados de la lista de ofertas", "oferentes", "corrección", "numberOfTenderers vacío, pero hay oferentes en tenderers o bids"),
    ("sin fechas de convocatoria", "sin_fechas", "corrección", "No se puede calcular el plazo: queda nulo"),
    ("plazo negativo -> nulo", "negativos", "corrección", "La fecha de cierre es anterior a la de inicio"),
    ("plazo mayor a 365 días -> nulo", "mayor_365", "corrección", "Valor fuera de rango"),
    ("monto estimado en otra moneda", "otra_moneda", "corrección", "Se analizan montos en guaraníes; no se convierten monedas"),
    ("monto estimado cero o negativo -> nulo", "no_positivo", "corrección", "Valor fuera de rango"),
    ("solo planificación (sin llamado publicado)", "planificacion", "enriquecimiento", "Columna solo_planificacion: se cuentan aparte en P1"),
    ("desvío estimado/adjudicado calculado", "desvio", "enriquecimiento", "Columna desvio_pct para P3"),
]:
    anotar("procesos", tipo, paso, c[clave], total, motivo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. plata_adjudicaciones y plata_contratos

# COMMAND ----------

atributos = leer("plata_procesos").select("ocid", "modalidad", "categoria", "entidad", "tipo_entidad")

# Una fila por adjudicación, con los campos que se usan. Se extraen ANTES de expandir proveedores o ítems:
# si se expande `a.items` en el mismo paso en que se lee `a.id`, Spark copia la adjudicación completa (con
# todos sus ítems) en cada fila expandida, y una adjudicación de miles de ítems crece de forma cuadrática.
adjudicaciones_base = (bronce.select("ocid", anio.alias("anio"), F.col("buyer.id").alias("entidad_id"),
                                     F.explode("awards").alias("a"))
                       .select("ocid", "anio", "entidad_id",
                               F.col("a.id").alias("adjudicacion_id"),
                               F.col("a.status").alias("estado"),
                               F.expr("try_cast(substring(a.date, 1, 10) AS DATE)").alias("fecha"),
                               F.col("a.value.amount").alias("monto"),
                               F.col("a.value.currency").alias("moneda"),
                               F.col("a.suppliers").alias("proveedores"),
                               F.col("a.items").alias("items")))

adj_tipadas = (adjudicaciones_base.drop("items")
               .withColumn("n_proveedores", F.coalesce(F.size("proveedores"), F.lit(0)))
               .select("*", F.explode_outer("proveedores").alias("s"))
               .select("*", F.col("s.id").alias("proveedor_id"), F.col("s.name").alias("proveedor_original"))
               .drop("s", "proveedores"))
REGLAS_ADJUDICACIONES = {
    "adjudicacion_presente": F.col("adjudicacion_id").isNotNull(),
    "proveedor_presente": F.col("proveedor_id").isNotNull(),
    "monto_no_negativo": F.col("monto").isNull() | (F.col("monto") >= 0),
    "anio_valido": F.col("anio").between(2010, 2026),
}
adj_validas = separar(validar(adj_tipadas, REGLAS_ADJUDICACIONES), "adjudicaciones", REGLAS_ADJUDICACIONES,
                      clave=["ocid", "adjudicacion_id", "proveedor_id"])
plata_adjudicaciones = (adj_validas
    .withColumn("ruc", F.regexp_replace("proveedor_id", "^PY-RUC-", ""))
    .withColumn("proveedor", limpiar(F.col("proveedor_original")))
    .withColumn("valida", F.col("estado").isin("active", "closed"))
    .withColumn("monto_pyg", F.when(F.col("moneda") == "PYG", F.col("monto")))
    .withColumn("monto_atribuido_pyg", F.col("monto_pyg") / F.greatest(F.col("n_proveedores"), F.lit(1)))
    .join(atributos, "ocid", "left"))
guardar_tabla(plata_adjudicaciones, "plata_adjudicaciones", particion="anio",
              comentario="Adjudicaciones validadas por proveedor, con RUC y monto en PYG atribuido")
total = leer("plata_adjudicaciones").count()
c = contar(leer("plata_adjudicaciones"), no_validas=~F.col("valida") | F.col("valida").isNull(),
           otra_moneda=F.col("moneda").isNotNull() & (F.col("moneda") != "PYG"), varios=F.col("n_proveedores") > 1)
anotar("adjudicaciones", "corrección", "canceladas o sin estado (valida = false)", c["no_validas"], total,
       "Se excluyen de los análisis de montos")
anotar("adjudicaciones", "corrección", "monto en otra moneda (monto_pyg nulo)", c["otra_moneda"], total,
       "Se analizan montos en guaraníes")
anotar("adjudicaciones", "enriquecimiento", "monto repartido entre varios proveedores", c["varios"], total,
       "Columna monto_atribuido_pyg: evita contar dos veces un monto")

proveedor_de_adjudicacion = leer("plata_adjudicaciones").select("ocid", "adjudicacion_id", "ruc", "proveedor").dropDuplicates()
con_tipados = (bronce.select("ocid", anio.alias("anio"), F.col("buyer.id").alias("entidad_id"), F.explode("contracts").alias("c"))
               .select("ocid", "anio", "entidad_id",
                       F.col("c.id").alias("contrato_id"),
                       F.col("c.awardID").alias("adjudicacion_id"),
                       F.col("c.status").alias("estado"),
                       F.expr("try_cast(substring(coalesce(c.dateSigned, c.period.startDate), 1, 10) AS DATE)").alias("fecha_firma"),
                       F.col("c.value.amount").alias("monto"),
                       F.col("c.value.currency").alias("moneda")))
REGLAS_CONTRATOS = {
    "contrato_presente": F.col("contrato_id").isNotNull(),
    "adjudicacion_asociada": F.col("adjudicacion_id").isNotNull(),
    "monto_no_negativo": F.col("monto").isNull() | (F.col("monto") >= 0),
}
con_validos = separar(validar(con_tipados, REGLAS_CONTRATOS), "contratos", REGLAS_CONTRATOS,
                      clave=["ocid", "contrato_id"])
plata_contratos = (con_validos
    .join(proveedor_de_adjudicacion, ["ocid", "adjudicacion_id"], "left")
    .withColumn("monto_pyg", F.when(F.col("moneda") == "PYG", F.col("monto")))
    .join(atributos, "ocid", "left"))
guardar_tabla(plata_contratos, "plata_contratos", particion="anio",
              comentario="Contratos validados con el RUC del proveedor y monto en PYG")
total = leer("plata_contratos").count()
c = contar(leer("plata_contratos"), sin_proveedor=F.col("ruc").isNull())
anotar("contratos", "enriquecimiento", "proveedor tomado de la adjudicación (sin proveedor: nulo)",
       total - c["sin_proveedor"], total, "Join con plata_adjudicaciones por ocid y adjudicación")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. plata_items y la tabla de referencia `ref_grupos_gasto`
# MAGIC Los 8 primeros dígitos del código de catálogo de la DNCP son el código UNSPSC. `ref_grupos_gasto` indica
# MAGIC qué familias corresponden a los grupos de la pregunta 7. También se agregan la descripción normalizada
# MAGIC (minúsculas, sin tildes) y marcas de si menciona cada palabra: calcularlas una vez aquí evita repetir la
# MAGIC búsqueda de texto en cada consulta (optimización A del notebook 03).

# COMMAND ----------

ref_grupos = spark.createDataFrame([
    ("combustible", "1510", "Combustibles: petroleo y destilados (gasoil, nafta, kerosene)"),
    ("medicamentos", "51", "Medicamentos y productos farmaceuticos"),
    ("alimentos", "50", "Alimentos, bebidas y tabaco"),
], "grupo STRING, prefijo_unspsc STRING, descripcion STRING")
guardar_tabla(ref_grupos, "ref_grupos_gasto", comentario="Grupos de gasto de la pregunta 7 segun el clasificador UNSPSC")

items_tipados = (adjudicaciones_base
                 .select("ocid", "anio", "entidad_id", "adjudicacion_id",
                         F.col("estado").alias("estado_adjudicacion"), F.explode("items").alias("i"))
                 .select("ocid", "anio", "entidad_id", "adjudicacion_id", "estado_adjudicacion",
                         F.col("i.id").alias("item_id"),
                         F.col("i.description").alias("descripcion"),
                         F.col("i.classification.id").alias("catalogo_id"),
                         F.col("i.classification.description").alias("catalogo"),
                         F.col("i.quantity").alias("cantidad"),
                         F.col("i.unit.value.amount").alias("precio_unitario"),
                         F.col("i.unit.value.currency").alias("moneda")))
REGLAS_ITEMS = {
    "cantidad_no_negativa": F.col("cantidad").isNull() | (F.col("cantidad") >= 0),
    "precio_no_negativo": F.col("precio_unitario").isNull() | (F.col("precio_unitario") >= 0),
    "descripcion_o_catalogo": F.col("descripcion").isNotNull() | F.col("catalogo_id").isNotNull(),
}
items_validos = separar(validar(items_tipados, REGLAS_ITEMS), "items", REGLAS_ITEMS)

PALABRAS = {"combustible": ["combustible", "combustibles"],
            "medicamentos": ["medicamento", "medicamentos"],
            "alimentos": ["alimento", "alimentos"]}
grupos = leer("ref_grupos_gasto").select(F.col("grupo").alias("grupo_catalogo"), "prefijo_unspsc")
plata_items = (items_validos
    .withColumn("adjudicacion_valida", F.col("estado_adjudicacion").isin("active", "closed"))
    .withColumn("descripcion_norm", F.translate(F.lower("descripcion"), "áéíóúüñàèìòù", "aeiouunaeiou"))
    .withColumn("unspsc", F.when(F.col("catalogo_id").rlike(r"^\d{8}"), F.substring("catalogo_id", 1, 8)))
    .withColumn("monto_pyg", F.when(F.col("moneda") == "PYG", F.col("cantidad") * F.col("precio_unitario")))
    .join(F.broadcast(grupos), F.col("unspsc").startswith(F.col("prefijo_unspsc")), "left")
    .drop("prefijo_unspsc")
    .join(atributos.select("ocid", "entidad", "tipo_entidad"), "ocid", "left"))
for grupo, palabras in PALABRAS.items():
    # (?U): límites de palabra Unicode (desde Java 19, \b es solo ASCII)
    patron = r"(?U)\b(" + "|".join(palabras) + r")\b"
    plata_items = plata_items.withColumn(f"menciona_{grupo}", F.coalesce(F.col("descripcion_norm").rlike(patron), F.lit(False)))
guardar_tabla(plata_items, "plata_items", particion="anio",
              comentario="Items adjudicados con monto en PYG, codigo UNSPSC, grupo de gasto y marcas de texto")

total = leer("plata_items").count()
c = contar(leer("plata_items"), no_validas=~F.col("adjudicacion_valida") | F.col("adjudicacion_valida").isNull(),
           sin_cantidad=F.col("cantidad").isNull(), otra_moneda=F.col("moneda").isNotNull() & (F.col("moneda") != "PYG"),
           sin_unspsc=F.col("unspsc").isNull(), con_grupo=F.col("grupo_catalogo").isNotNull())
anotar("items", "corrección", "de adjudicaciones canceladas o sin estado", c["no_validas"], total, "Se excluyen del gasto")
anotar("items", "corrección", "sin cantidad (contratos abiertos por monto): monto nulo", c["sin_cantidad"], total,
       "No se puede calcular cantidad x precio")
anotar("items", "corrección", "precio en otra moneda", c["otra_moneda"], total, "Se analizan montos en guaraníes")
anotar("items", "corrección", "código de catálogo sin formato UNSPSC", c["sin_unspsc"], total, "No se puede clasificar")
anotar("items", "enriquecimiento", "clasificados en un grupo de gasto (join con ref_grupos_gasto)", c["con_grupo"], total,
       "Columna grupo_catalogo para P7")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Informe de calidad

# COMMAND ----------

calidad = spark.createDataFrame(CALIDAD, "tabla STRING, tipo STRING, paso STRING, filas_afectadas BIGINT, "
                                         "filas_totales BIGINT, pct DOUBLE, motivo STRING")
guardar_tabla(calidad, "plata_calidad", comentario="Reglas, correcciones y enriquecimientos de la capa plata, con filas afectadas")
mostrar(leer("plata_calidad"), n=60)

resumen = spark.createDataFrame(
    [(n, leer("bronce_ocds").count() if n == "procesos" else None, leer(f"plata_{n}").count(), leer(f"cuarentena_{n}").count())
     for n in ["procesos", "adjudicaciones", "contratos", "items"]],
    "tabla STRING, filas_bronce BIGINT, filas_plata BIGINT, filas_cuarentena BIGINT")
mostrar(resumen)
