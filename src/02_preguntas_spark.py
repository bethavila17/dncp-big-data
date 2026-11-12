"""Paso 2 (Apache Spark): responder las 7 preguntas sobre las tablas Parquet.

Cada exportable se guarda en resultados/<alcance>/spark/ y luego se compara con
MapReduce, Hive y Elasticsearch en src/06_verificar.py.

Uso:  python src/02_preguntas_spark.py
"""
import time

import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import Window
from pyspark.sql import functions as F

import comun

SIN_DATO = "(sin dato)"
MIN_ADJ_ENTIDAD = 20      # entidades con menos adjudicaciones no se clasifican por HHI
MIN_PROCESOS_CATEGORIA = 30
TRAMOS_DIAS = [(0, 7, "0-7"), (8, 14, "08-14"), (15, 21, "15-21"), (22, 30, "22-30"),
               (31, 60, "31-60"), (61, 365, "61-365")]

# P7 por catálogo: los 8 primeros dígitos del código DNCP son el código UNSPSC
PREFIJOS_UNSPSC = {
    "combustible": "1510",   # Combustibles: petróleo y destilados (gasoil, nafta, kerosene)
    "medicamentos": "51",    # Medicamentos y productos farmacéuticos
    "alimentos": "50",       # Alimentos, bebidas y tabaco
}


def texto(col):
    return F.coalesce(col.cast("string"), F.lit(SIN_DATO))


def guardar(df, nombre):
    return comun.guardar_csv(df.toPandas(), "spark", nombre)


def mediana_y_cuartiles(col):
    return [F.percentile(col, 0.25).alias("p25"), F.percentile(col, 0.5).alias("mediana"),
            F.percentile(col, 0.75).alias("p75")]


def normalizar(col):
    """minúsculas y sin tildes, para buscar palabras completas."""
    return F.translate(F.lower(col), "áéíóúüñàèìòù", "aeiouunaeiou")


def p1(procesos):
    print("P1: procesos, adjudicaciones y contratos por año y modalidad")
    df = (procesos.groupBy(texto(F.col("anio")).alias("anio"), texto(F.col("modalidad")).alias("modalidad"))
          .agg(F.count("*").alias("procesos"),
               F.sum("n_adjudicaciones").alias("adjudicaciones"),
               F.sum("n_contratos").alias("contratos"),
               F.sum("monto_adjudicado_pyg").alias("monto_adjudicado_pyg"))
          .orderBy("anio", "modalidad"))
    guardar(df, "p1_anio_modalidad")
    guardar(df.groupBy("anio").agg(*[F.sum(c).alias(c) for c in
                                     ["procesos", "adjudicaciones", "contratos", "monto_adjudicado_pyg"]])
            .orderBy("anio"), "p1_anio")


def p2(procesos):
    print("P2: llamados con una sola oferta")
    con_oferta = procesos.where(F.col("n_oferentes") >= 1)
    for dim, nombre in [("modalidad", "p2_unica_oferta_modalidad"),
                        ("tipo_entidad", "p2_unica_oferta_tipo_entidad"),
                        ("anio", "p2_unica_oferta_anio")]:
        df = (con_oferta.groupBy(texto(F.col(dim)).alias(dim))
              .agg(F.count("*").alias("llamados_con_oferta"),
                   F.sum((F.col("n_oferentes") == 1).cast("int")).alias("una_sola_oferta"))
              .withColumn("pct_una_oferta",
                          F.round(100 * F.col("una_sola_oferta") / F.col("llamados_con_oferta"), 2))
              .orderBy(dim))
        guardar(df, nombre)


def p3(procesos):
    print("P3: desvío entre monto estimado y adjudicado")
    base = (procesos
            .where((F.col("moneda_estimado") == "PYG") & (F.col("monto_estimado") > 0)
                   & (F.col("n_adjudicaciones_pyg") > 0) & (F.col("monto_adjudicado_pyg") > 0))
            .withColumn("desvio_pct", 100 * (F.col("monto_adjudicado_pyg") - F.col("monto_estimado"))
                        / F.col("monto_estimado")))

    def resumen(df):
        return df.agg(
            F.count("*").alias("procesos"),
            *mediana_y_cuartiles(F.col("desvio_pct")),
            F.percentile("desvio_pct", 0.9).alias("p90"),
            F.round(100 * F.avg((F.col("desvio_pct") > 0).cast("int")), 2).alias("pct_sobre_estimado"),
            F.round(100 * F.avg((F.col("desvio_pct") > 10).cast("int")), 2).alias("pct_sobre_10"),
            F.round(100 * F.avg((F.col("desvio_pct") < -10).cast("int")), 2).alias("pct_bajo_10"),
            F.sum("monto_estimado").alias("monto_estimado_pyg"),
            F.sum("monto_adjudicado_pyg").alias("monto_adjudicado_pyg"))

    guardar(resumen(base), "p3_desvio_resumen")
    for dim, nombre in [("categoria", "p3_desvio_categoria"), ("modalidad", "p3_desvio_modalidad")]:
        df = (resumen(base.groupBy(texto(F.col(dim)).alias(dim)))
              .withColumn("desvio_agregado_pct",
                          F.round(100 * (F.col("monto_adjudicado_pyg") - F.col("monto_estimado_pyg"))
                                  / F.col("monto_estimado_pyg"), 2))
              .where(F.col("procesos") >= MIN_PROCESOS_CATEGORIA)
              .orderBy(F.desc("pct_sobre_10")))
        guardar(df, nombre)


def p4(contratos):
    print("P4: contratos por proveedor (RUC)")
    pyg = contratos.where((F.col("moneda") == "PYG") & F.col("ruc").isNotNull())
    top = (pyg.groupBy("ruc")
           .agg(F.max("proveedor").alias("proveedor"),
                F.count("*").alias("contratos"),
                F.countDistinct("entidad_id").alias("entidades"),
                F.sum("monto").alias("monto_pyg"))
           .orderBy(F.desc("monto_pyg")))
    top.cache()
    guardar(top.limit(50), "p4_top_proveedores")

    # Muestra de RUC para comparar la latencia de Spark con Elasticsearch y HBase
    por_monto = [r.ruc for r in top.limit(5).collect()]
    por_cantidad = [r.ruc for r in top.orderBy(F.desc("contratos"), "ruc").limit(5).collect()]
    al_azar = [r.ruc for r in top.where(F.col("contratos") >= 3)
               .orderBy(F.xxhash64("ruc"), "ruc").limit(5).collect()]
    muestra = list(dict.fromkeys(por_monto + por_cantidad + al_azar))
    comun.guardar_csv(pd.DataFrame({"ruc": muestra}), "spark", "p4_rucs_muestra")

    filas = []
    for ruc in muestra:
        t0 = time.perf_counter()
        r = (contratos.where(F.col("ruc") == ruc)
             .agg(F.count("*").alias("contratos"),
                  F.countDistinct("entidad_id").alias("entidades"),
                  F.sum(F.when(F.col("moneda") == "PYG", F.col("monto"))).alias("monto_pyg"))
             .collect()[0])
        filas.append({"ruc": ruc, "contratos": r.contratos, "entidades": r.entidades,
                      "monto_pyg": r.monto_pyg, "ms_spark": round(1000 * (time.perf_counter() - t0), 1)})
    comun.guardar_csv(pd.DataFrame(filas), "spark", "p4_consulta_ruc")

    ejemplo = muestra[0]
    guardar(contratos.where(F.col("ruc") == ejemplo)
            .select("ruc", "proveedor", "ocid", "contrato_id", "fecha_firma", "entidad", "tipo_entidad",
                    "modalidad", "categoria", "monto", "moneda")
            .orderBy(F.desc("monto")), "p4_ejemplo_contratos_ruc")


def concentracion(adj, claves):
    """Participación del top 1 y top 10 de proveedores e índice Herfindahl-Hirschman."""
    por_prov = adj.groupBy(*claves, "ruc").agg(F.sum("monto_atribuido").alias("monto"),
                                               F.count("*").alias("n"))
    w_total = Window.partitionBy(*claves)
    w_rank = Window.partitionBy(*claves).orderBy(F.desc("monto"), "ruc")
    con_share = (por_prov.withColumn("total", F.sum("monto").over(w_total))
                 .withColumn("share", 100 * F.col("monto") / F.col("total"))
                 .withColumn("rango", F.row_number().over(w_rank)))
    return (con_share.groupBy(*claves)
            .agg(F.sum("n").alias("adjudicaciones"),
                 F.count("*").alias("proveedores"),
                 F.first("total").alias("monto_pyg"),
                 F.round(F.sum(F.when(F.col("rango") == 1, F.col("share"))), 4).alias("share_top1"),
                 F.round(F.sum(F.when(F.col("rango") <= 10, F.col("share"))), 4).alias("share_top10"),
                 F.round(F.sum(F.col("share") * F.col("share")), 4).alias("hhi")))


def nivel_hhi(col):
    return (F.when(col < 1500, "1. no concentrado (<1500)")
            .when(col <= 2500, "2. moderado (1500-2500)")
            .otherwise("3. alto (>2500)"))


def p5(adjudicaciones):
    print("P5: concentración de proveedores (top 10 y HHI)")
    adj = adjudicaciones.where(F.col("valida") & (F.col("moneda") == "PYG")
                               & (F.col("monto_atribuido") > 0) & F.col("ruc").isNotNull()
                               & F.col("entidad_id").isNotNull())
    # Se agrupa por entidad_id: el nombre de una entidad cambia entre años
    nombres = adj.groupBy("entidad_id").agg(F.max("entidad").alias("entidad"),
                                            F.max("tipo_entidad").alias("tipo_entidad"))
    entidades = (concentracion(adj, ["entidad_id"]).join(nombres, "entidad_id")
                 .select("entidad_id", "entidad", "tipo_entidad", "adjudicaciones", "proveedores", "monto_pyg",
                         "share_top1", "share_top10", "hhi")
                 .orderBy(F.desc("monto_pyg")))
    entidades.cache()
    guardar(entidades, "p5_concentracion_entidad")
    guardar(concentracion(adj.withColumn("categoria", texto(F.col("categoria"))), ["categoria"])
            .orderBy(F.desc("hhi")), "p5_concentracion_categoria")

    clasificables = entidades.where(F.col("adjudicaciones") >= MIN_ADJ_ENTIDAD)
    guardar(clasificables.groupBy(nivel_hhi(F.col("hhi")).alias("nivel_hhi"))
            .agg(F.count("*").alias("entidades"), F.sum("monto_pyg").alias("monto_pyg"))
            .orderBy("nivel_hhi"), "p5_entidades_por_nivel_hhi")
    guardar(clasificables.groupBy("tipo_entidad")
            .agg(F.count("*").alias("entidades"),
                 F.percentile("hhi", 0.5).alias("hhi_mediana"),
                 F.percentile("share_top10", 0.5).alias("share_top10_mediana"))
            .orderBy(F.desc("hhi_mediana")), "p5_hhi_tipo_entidad")

    total = adj.agg(F.sum("monto_atribuido")).collect()[0][0]
    guardar(adj.groupBy("ruc").agg(F.max("proveedor").alias("proveedor"),
                                   F.sum("monto_atribuido").alias("monto_pyg"))
            .withColumn("share_pct", F.round(100 * F.col("monto_pyg") / F.lit(total), 4))
            .orderBy(F.desc("monto_pyg")).limit(10), "p5_top10_proveedores_estado")


def p6(procesos):
    print("P6: duración de la convocatoria y cantidad de oferentes")
    validos = procesos.where(F.col("dias_convocatoria").between(0, 365))
    guardar(procesos.agg(
        F.count("*").alias("procesos"),
        F.sum(F.col("dias_convocatoria").isNull().cast("int")).alias("sin_fechas"),
        F.sum((F.col("dias_convocatoria") < 0).cast("int")).alias("dias_negativos"),
        F.sum((F.col("dias_convocatoria") > 365).cast("int")).alias("mas_de_365")), "p6_calidad_fechas")

    guardar(validos.groupBy(texto(F.col("modalidad")).alias("modalidad"))
            .agg(F.count("*").alias("procesos"), F.round(F.avg("dias_convocatoria"), 2).alias("media"),
                 *mediana_y_cuartiles(F.col("dias_convocatoria")))
            .where(F.col("procesos") >= MIN_PROCESOS_CATEGORIA)
            .orderBy(F.desc("procesos")), "p6_dias_modalidad")

    con_oferta = validos.where(F.col("n_oferentes") >= 1)
    tramo = F.lit(None)
    for desde, hasta, etiqueta in reversed(TRAMOS_DIAS):
        tramo = F.when(F.col("dias_convocatoria").between(desde, hasta), etiqueta).otherwise(tramo)
    guardar(con_oferta.groupBy(tramo.alias("tramo_dias"))
            .agg(F.count("*").alias("llamados"),
                 F.round(F.avg("n_oferentes"), 4).alias("oferentes_promedio"),
                 F.percentile("n_oferentes", 0.5).alias("oferentes_mediana"),
                 F.round(100 * F.avg((F.col("n_oferentes") == 1).cast("int")), 2).alias("pct_una_oferta"))
            .orderBy("tramo_dias"), "p6_tramos_dias_oferentes")

    # Spearman con MLlib: global y dentro de las modalidades más frecuentes
    vector = VectorAssembler(inputCols=["dias_convocatoria", "n_oferentes"], outputCol="v")
    grupos = [("(todas)", con_oferta)] + [
        (r.modalidad, con_oferta.where(F.col("modalidad") == r.modalidad))
        for r in con_oferta.groupBy("modalidad").count().orderBy(F.desc("count")).limit(6).collect()]
    filas = []
    for nombre, df in grupos:
        d = df.select(F.col("dias_convocatoria").cast("double"), F.col("n_oferentes").cast("double"))
        rho = Correlation.corr(vector.transform(d), "v", "spearman").collect()[0][0][0, 1]
        filas.append({"modalidad": nombre, "llamados": d.count(), "spearman": round(float(rho), 4)})
    comun.guardar_csv(pd.DataFrame(filas), "spark", "p6_spearman")


def p7(items):
    print("P7: gasto en combustible, medicamentos y alimentos")
    base = items.where((F.col("moneda") == "PYG") & F.col("monto").isNotNull())
    desc = normalizar(F.col("descripcion"))
    marcados = base
    for grupo, palabras in comun.TERMINOS_P7.items():
        # (?U): límites de palabra Unicode, como Elasticsearch (desde Java 19, \b es solo ASCII)
        patron = r"(?U)\b(" + "|".join(palabras) + r")\b"
        marcados = marcados.withColumn(f"txt_{grupo}", F.coalesce(desc.rlike(patron), F.lit(False)))
        marcados = marcados.withColumn(f"cat_{grupo}", F.coalesce(
            F.col("catalogo_id").startswith(PREFIJOS_UNSPSC[grupo]), F.lit(False)))
    marcados.cache()

    for metodo, sufijo in [("txt", "texto"), ("cat", "catalogo")]:
        resumen, entidades = [], []
        for grupo in comun.TERMINOS_P7:
            sel = marcados.where(F.col(f"{metodo}_{grupo}"))
            r = sel.agg(F.count("*").alias("items"), F.countDistinct("ocid").alias("procesos"),
                        F.sum("monto").alias("monto_pyg")).collect()[0]
            resumen.append({"grupo": grupo, "items": r.items, "procesos": r.procesos, "monto_pyg": r.monto_pyg})
            top = (sel.where(F.col("entidad").isNotNull())
                   .groupBy("entidad").agg(F.sum("monto").alias("monto_pyg"), F.count("*").alias("items"))
                   .orderBy(F.desc("monto_pyg"), "entidad").limit(10).toPandas())
            top.insert(0, "grupo", grupo)
            entidades.append(top)
        comun.guardar_csv(pd.DataFrame(resumen), "spark", f"p7_{sufijo}_resumen")
        comun.guardar_csv(pd.concat(entidades), "spark", f"p7_{sufijo}_top_entidades")

    # ¿Cuántos ítems hallados por texto son realmente de la familia del catálogo?
    filas = []
    for grupo in comun.TERMINOS_P7:
        r = marcados.agg(
            F.sum(F.col(f"txt_{grupo}").cast("int")).alias("solo_texto_total"),
            F.sum((F.col(f"txt_{grupo}") & F.col(f"cat_{grupo}")).cast("int")).alias("texto_y_catalogo"),
            F.sum(F.col(f"cat_{grupo}").cast("int")).alias("catalogo_total")).collect()[0]
        filas.append({"grupo": grupo, "items_texto": r.solo_texto_total, "items_catalogo": r.catalogo_total,
                      "items_en_ambos": r.texto_y_catalogo})
    comun.guardar_csv(pd.DataFrame(filas), "spark", "p7_texto_vs_catalogo")

    # Ejemplos de lo que encuentra la búsqueda por texto, para el informe
    ejemplos = (marcados.where(F.col("txt_combustible"))
                .groupBy(F.coalesce("catalogo", F.lit(SIN_DATO)).alias("catalogo"))
                .agg(F.count("*").alias("items"), F.sum("monto").alias("monto_pyg"))
                .orderBy(F.desc("items")).limit(15))
    guardar(ejemplos, "p7_texto_combustible_por_catalogo")


def main():
    spark = comun.spark("02_preguntas")
    t = {n: comun.leer_parquet(spark, n) for n in comun.TABLAS}
    inicio = time.time()
    for paso, args in [(p1, ["procesos"]), (p2, ["procesos"]), (p3, ["procesos"]), (p4, ["contratos"]),
                       (p5, ["adjudicaciones"]), (p6, ["procesos"]), (p7, ["items_adjudicados"])]:
        t0 = time.time()
        paso(*[t[a] for a in args])
        print(f"   {time.time() - t0:.0f} s")
    print(f"Total: {time.time() - inicio:.0f} s")
    spark.stop()


if __name__ == "__main__":
    main()
