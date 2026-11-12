"""Paso 1 (Apache Spark): aplanar el JSON Lines OCDS de la DNCP en cuatro tablas Parquet.

Cada línea del archivo es un proceso de contratación (ocid) con sus etapas anidadas.
Se leen solo los campos necesarios con un esquema explícito y se generan:

    procesos           una fila por proceso (llamado)
    adjudicaciones     una fila por adjudicación y proveedor
    contratos          una fila por contrato y proveedor
    items_adjudicados  una fila por ítem de adjudicación válida

Uso:  python src/01_aplanar_spark.py
"""
import time

from pyspark.sql import functions as F

import comun

ESQUEMA = """
ocid STRING,
date STRING,
buyer STRUCT<id: STRING, name: STRING>,
parties ARRAY<STRUCT<id: STRING, roles: ARRAY<STRING>,
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

ESTADOS_VALIDOS = ["active", "closed"]  # adjudicaciones vigentes o finalizadas


def limpiar(col):
    """Espacios, tabuladores y saltos de línea seguidos -> un espacio; vacío -> nulo."""
    texto = F.trim(F.regexp_replace(col, r"\s+", " "))
    return F.when(texto != "", texto)


def ruc(col):
    """PY-RUC-80000519-8 -> 80000519-8 (otros esquemas quedan como vienen)."""
    return F.regexp_replace(col, "^PY-RUC-", "")


def main():
    spark = comun.spark("01_aplanar")
    inicio = time.time()

    crudo = (spark.read.schema(ESQUEMA)
             .option("mode", "PERMISSIVE")
             .option("columnNameOfCorruptRecord", "_corrupt_record")
             .json(comun.JSONL))

    t = F.col("tender")
    fecha_ref = F.coalesce(t.tenderPeriod.startDate, t.datePublished, F.col("date"))
    oferentes_bids = F.array_distinct(F.flatten(F.transform("bids.details", lambda d: d.tenderers.id)))
    comprador = F.filter("parties", lambda p: F.array_contains(p.roles, "buyer"))

    base = crudo.select(
        "ocid",
        F.substring(fecha_ref, 1, 4).cast("int").alias("anio"),
        F.to_date(F.substring(t.tenderPeriod.startDate, 1, 10)).alias("fecha_inicio"),
        F.to_date(F.substring(t.tenderPeriod.endDate, 1, 10)).alias("fecha_fin"),
        F.lower(limpiar(t.procurementMethodDetails)).alias("modalidad"),
        t.procurementMethod.alias("metodo"),
        limpiar(t.mainProcurementCategoryDetails).alias("categoria"),
        F.col("buyer.id").alias("entidad_id"),
        limpiar(F.col("buyer.name")).alias("entidad"),
        F.coalesce(limpiar(F.element_at(comprador, 1).details.level), F.lit("Sin dato")).alias("tipo_entidad"),
        t.status.alias("estado_llamado"),
        t.value.currency.alias("moneda_estimado"),
        t.value.amount.alias("monto_estimado"),
        F.greatest(F.coalesce(t.numberOfTenderers, F.lit(0)),
                   F.coalesce(F.size(t.tenderers), F.lit(0)),
                   F.coalesce(F.size(oferentes_bids), F.lit(0))).alias("n_oferentes"),
        "awards", "contracts", "_corrupt_record",
        F.col("tender").isNull().alias("sin_llamado"),
    ).persist()

    corruptos = base.where(F.col("_corrupt_record").isNotNull()).count()
    sin_llamado = base.where(F.col("sin_llamado") & F.col("_corrupt_record").isNull()).count()
    base = base.where(F.col("_corrupt_record").isNull() & ~F.col("sin_llamado"))                .drop("_corrupt_record", "sin_llamado")

    # Tipo de entidad: hay registros que no lo traen en `parties`. Se toma el informado en
    # otros procesos de la misma entidad (ninguna entidad tiene dos tipos distintos).
    tipos = (base.where(F.col("tipo_entidad") != "Sin dato").groupBy("entidad_id")
             .agg(F.max("tipo_entidad").alias("tipo_canonico")))
    base = base.join(F.broadcast(tipos), "entidad_id", "left")
    imputados = base.where((F.col("tipo_entidad") == "Sin dato") & F.col("tipo_canonico").isNotNull()).count()
    base = (base.withColumn("tipo_entidad", F.coalesce("tipo_canonico", "tipo_entidad"))
            .drop("tipo_canonico"))
    atributos = ["ocid", "anio", "modalidad", "categoria", "entidad_id", "entidad", "tipo_entidad"]

    # --- adjudicaciones (adjudicación x proveedor) ---
    adj = (base.select(*atributos, F.explode("awards").alias("a"))
           .select(*atributos,
                   F.col("a.id").alias("adjudicacion_id"),
                   F.col("a.status").alias("estado"),
                   F.col("a.status").isin(ESTADOS_VALIDOS).alias("valida"),
                   F.to_date(F.substring("a.date", 1, 10)).alias("fecha"),
                   F.col("a.value.amount").alias("monto"),
                   F.col("a.value.currency").alias("moneda"),
                   F.coalesce(F.size("a.suppliers"), F.lit(0)).alias("n_proveedores"),
                   F.explode_outer("a.suppliers").alias("s"))
           .select("*", ruc(F.col("s.id")).alias("ruc"), limpiar(F.col("s.name")).alias("proveedor"))
           .withColumn("monto_atribuido", F.col("monto") / F.greatest(F.col("n_proveedores"), F.lit(1)))
           .drop("s"))

    # --- contratos (contrato x proveedor, el proveedor sale de la adjudicación) ---
    proveedores_adj = adj.select("ocid", "adjudicacion_id", "ruc", "proveedor").distinct()
    contratos = (base.select(*atributos, F.explode("contracts").alias("c"))
                 .where(F.col("c.awardID").isNotNull())
                 .select(*atributos,
                         F.col("c.id").alias("contrato_id"),
                         F.col("c.awardID").alias("adjudicacion_id"),
                         F.col("c.status").alias("estado"),
                         F.to_date(F.substring(F.coalesce("c.dateSigned", "c.period.startDate"), 1, 10))
                          .alias("fecha_firma"),
                         F.col("c.value.amount").alias("monto"),
                         F.col("c.value.currency").alias("moneda"))
                 .join(proveedores_adj, ["ocid", "adjudicacion_id"], "left"))

    # --- ítems de adjudicaciones válidas ---
    items = (base.select(*atributos, F.explode("awards").alias("a"))
             .where(F.col("a.status").isin(ESTADOS_VALIDOS))
             .select(*atributos, F.col("a.id").alias("adjudicacion_id"), F.explode("a.items").alias("i"))
             .select(*atributos, "adjudicacion_id",
                     F.col("i.id").alias("item_id"),
                     F.col("i.description").alias("descripcion"),
                     F.col("i.classification.id").alias("catalogo_id"),
                     F.col("i.classification.description").alias("catalogo"),
                     F.col("i.quantity").alias("cantidad"),
                     F.col("i.unit.value.amount").alias("precio_unitario"),
                     F.col("i.unit.value.currency").alias("moneda"))
             .withColumn("monto", F.col("cantidad") * F.col("precio_unitario")))

    # --- procesos (una fila por llamado) ---
    adj_validas = F.filter("awards", lambda a: a.status.isin(ESTADOS_VALIDOS))
    adj_pyg = F.filter(adj_validas, lambda a: a.value.currency == "PYG")
    procesos = base.select(
        "ocid", "anio", "fecha_inicio", "fecha_fin",
        F.datediff("fecha_fin", "fecha_inicio").alias("dias_convocatoria"),
        "modalidad", "metodo", "categoria", "entidad_id", "entidad", "tipo_entidad",
        "estado_llamado", "moneda_estimado", "monto_estimado", "n_oferentes",
        F.coalesce(F.size(adj_validas), F.lit(0)).alias("n_adjudicaciones"),
        F.coalesce(F.size(F.filter("contracts", lambda c: c.awardID.isNotNull())), F.lit(0))
         .alias("n_contratos"),
        F.coalesce(F.aggregate(adj_pyg, F.lit(0.0),
                               lambda acc, a: acc + F.coalesce(a.value.amount, F.lit(0.0))), F.lit(0.0))
         .alias("monto_adjudicado_pyg"),
        F.coalesce(F.size(adj_pyg), F.lit(0)).alias("n_adjudicaciones_pyg"),
    )

    filas = {}
    for nombre, df in [("procesos", procesos), ("adjudicaciones", adj),
                       ("contratos", contratos), ("items_adjudicados", items)]:
        t0 = time.time()
        filas[nombre] = comun.escribir_parquet(df, f"{comun.PARQUET}/{nombre}", particion="anio")
        print(f"{nombre}: {filas[nombre]:,} filas ({time.time() - t0:.0f} s)")

    import pandas as pd
    resumen = pd.DataFrame([{"tabla": k, "filas": v} for k, v in filas.items()]
                           + [{"tabla": "registros_corruptos", "filas": corruptos},
                              {"tabla": "procesos_sin_llamado", "filas": sin_llamado},
                              {"tabla": "procesos_tipo_entidad_imputado", "filas": imputados}])
    comun.guardar_csv(resumen, "spark", "00_tablas_aplanadas")
    print(f"Total: {time.time() - inicio:.0f} s")
    spark.stop()


if __name__ == "__main__":
    main()
