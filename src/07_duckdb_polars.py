"""Paso 7 (DuckDB y Polars, clase 4; formatos, clase 3): otra comprobación en una sola máquina.

DuckDB (SQL sobre los archivos Parquet, sin servidor) y Polars (DataFrames con evaluación perezosa)
recalculan P1, P2, P3, P5 y P6 sobre las mismas tablas que generó Spark. Sus CSV se comparan con los de
Spark en src/06_verificar.py.

Además mide, como en el laboratorio de formatos de la clase 3, cuánto ocupa la tabla de ítems y cuánto
tarda la misma consulta en CSV, CSV comprimido con gzip y Parquet con Snappy y con Zstd.

Uso:  python src/07_duckdb_polars.py
"""
import glob
import os
import time

import duckdb
import pandas as pd
import polars as pl

import comun

SIN_DATO = "(sin dato)"


def archivos(tabla):
    return sorted(glob.glob(f"{comun.PARQUET_LOCAL}/{tabla}/*.parquet"))


def ruta_duckdb(tabla):
    return "[" + ", ".join(f"'{a}'" for a in archivos(tabla)) + "]"


def con_duckdb():
    con = duckdb.connect()
    for tabla in ["procesos", "adjudicaciones", "items_adjudicados"]:
        con.sql(f"CREATE VIEW {tabla} AS SELECT * FROM read_parquet({ruta_duckdb(tabla)}, union_by_name = true)")
    return con


def duckdb_preguntas(con):
    print("DuckDB: P1, P3 y P5")
    p1 = con.sql(f"""
        SELECT COALESCE(CAST(anio AS VARCHAR), '{SIN_DATO}') AS anio,
               COALESCE(modalidad, '{SIN_DATO}')             AS modalidad,
               COUNT(*)                                       AS procesos,
               SUM(n_adjudicaciones)                          AS adjudicaciones,
               SUM(n_contratos)                               AS contratos,
               SUM(monto_adjudicado_pyg)                      AS monto_adjudicado_pyg
        FROM procesos GROUP BY ALL ORDER BY ALL""").df()
    comun.guardar_csv(p1, "duckdb", "p1_anio_modalidad")

    p3 = con.sql("""
        WITH d AS (
            SELECT monto_estimado, monto_adjudicado_pyg,
                   100 * (monto_adjudicado_pyg - monto_estimado) / monto_estimado AS desvio
            FROM procesos
            WHERE moneda_estimado = 'PYG' AND monto_estimado > 0
              AND n_adjudicaciones_pyg > 0 AND monto_adjudicado_pyg > 0)
        SELECT COUNT(*) AS procesos,
               quantile_cont(desvio, 0.25) AS p25, quantile_cont(desvio, 0.5) AS mediana,
               quantile_cont(desvio, 0.75) AS p75, quantile_cont(desvio, 0.9) AS p90,
               ROUND(100 * AVG(CASE WHEN desvio > 0 THEN 1 ELSE 0 END), 2)   AS pct_sobre_estimado,
               ROUND(100 * AVG(CASE WHEN desvio > 10 THEN 1 ELSE 0 END), 2)  AS pct_sobre_10,
               ROUND(100 * AVG(CASE WHEN desvio < -10 THEN 1 ELSE 0 END), 2) AS pct_bajo_10,
               SUM(monto_estimado) AS monto_estimado_pyg, SUM(monto_adjudicado_pyg) AS monto_adjudicado_pyg
        FROM d""").df()
    comun.guardar_csv(p3, "duckdb", "p3_desvio_resumen")

    p5 = con.sql("""
        WITH por_proveedor AS (
            SELECT entidad_id, ruc, SUM(monto_atribuido) AS monto, COUNT(*) AS n
            FROM adjudicaciones
            WHERE valida AND moneda = 'PYG' AND monto_atribuido > 0 AND ruc IS NOT NULL AND entidad_id IS NOT NULL
            GROUP BY ALL),
        cuotas AS (
            SELECT *, 100 * monto / SUM(monto) OVER (PARTITION BY entidad_id) AS cuota,
                   ROW_NUMBER() OVER (PARTITION BY entidad_id ORDER BY monto DESC, ruc) AS puesto
            FROM por_proveedor)
        SELECT entidad_id, SUM(n) AS adjudicaciones, COUNT(*) AS proveedores, SUM(monto) AS monto_pyg,
               ROUND(SUM(CASE WHEN puesto = 1 THEN cuota END), 4)   AS share_top1,
               ROUND(SUM(CASE WHEN puesto <= 10 THEN cuota END), 4) AS share_top10,
               ROUND(SUM(cuota * cuota), 4)                         AS hhi
        FROM cuotas GROUP BY entidad_id ORDER BY monto_pyg DESC""").df()
    comun.guardar_csv(p5, "duckdb", "p5_concentracion_entidad")


def polars_preguntas():
    print("Polars (lazy): P2 y P6")
    procesos = pl.scan_parquet(archivos("procesos"))
    con_oferta = procesos.filter(pl.col("n_oferentes") >= 1)
    for dim in ["modalidad", "tipo_entidad", "anio"]:
        df = (con_oferta
              .group_by(pl.col(dim).cast(pl.Utf8).fill_null(SIN_DATO).alias(dim))
              .agg(pl.len().alias("llamados_con_oferta"),
                   (pl.col("n_oferentes") == 1).sum().alias("una_sola_oferta"))
              .with_columns((100 * pl.col("una_sola_oferta") / pl.col("llamados_con_oferta")).round(2)
                            .alias("pct_una_oferta"))
              .sort(dim).collect())
        comun.guardar_csv(df.to_pandas(), "polars", f"p2_unica_oferta_{dim}")

    validos = procesos.filter(pl.col("dias_convocatoria").is_between(0, 365))
    dias = pl.col("dias_convocatoria")
    p6 = (validos.group_by(pl.col("modalidad").fill_null(SIN_DATO))
          .agg(pl.len().alias("procesos"), dias.mean().round(2).alias("media"),
               dias.quantile(0.25, "linear").alias("p25"), dias.quantile(0.5, "linear").alias("mediana"),
               dias.quantile(0.75, "linear").alias("p75"))
          .filter(pl.col("procesos") >= 30).sort("procesos", descending=True).collect())
    comun.guardar_csv(p6.to_pandas(), "polars", "p6_dias_modalidad")

    tramo = (pl.when(dias <= 7).then(pl.lit("0-7")).when(dias <= 14).then(pl.lit("08-14"))
             .when(dias <= 21).then(pl.lit("15-21")).when(dias <= 30).then(pl.lit("22-30"))
             .when(dias <= 60).then(pl.lit("31-60")).otherwise(pl.lit("61-365")))
    tramos = (validos.filter(pl.col("n_oferentes") >= 1).with_columns(tramo.alias("tramo_dias"))
              .group_by("tramo_dias")
              .agg(pl.len().alias("llamados"), pl.col("n_oferentes").mean().round(4).alias("oferentes_promedio"),
                   pl.col("n_oferentes").median().alias("oferentes_mediana"),
                   ((pl.col("n_oferentes") == 1).mean() * 100).round(2).alias("pct_una_oferta"))
              .sort("tramo_dias").collect())
    comun.guardar_csv(tramos.to_pandas(), "polars", "p6_tramos_dias_oferentes")


def formatos(con):
    """Clase 3: la misma tabla en cuatro formatos; tamaño y tiempo de la misma consulta."""
    print("Formatos: CSV, CSV gzip, Parquet Snappy y Parquet Zstd")
    carpeta = os.path.join(comun.BASE, f"formatos_{comun.ALCANCE}")
    os.makedirs(carpeta, exist_ok=True)
    destinos = {
        "CSV": (f"{carpeta}/items.csv", "(FORMAT csv, HEADER true)"),
        "CSV gzip": (f"{carpeta}/items.csv.gz", "(FORMAT csv, HEADER true, COMPRESSION gzip)"),
        "Parquet Snappy": (f"{carpeta}/items_snappy.parquet", "(FORMAT parquet, COMPRESSION snappy)"),
        "Parquet Zstd": (f"{carpeta}/items_zstd.parquet", "(FORMAT parquet, COMPRESSION zstd)"),
    }
    filas = []
    for formato, (ruta, opciones) in destinos.items():
        t0 = time.perf_counter()
        con.sql(f"COPY (SELECT * FROM items_adjudicados) TO '{ruta}' {opciones}")
        escritura = time.perf_counter() - t0
        lector = f"read_csv('{ruta}')" if formato.startswith("CSV") else f"read_parquet('{ruta}')"
        consulta = f"SELECT anio, SUM(monto) AS monto FROM {lector} GROUP BY anio"
        tiempos = []
        for _ in range(3):
            t0 = time.perf_counter()
            con.sql(consulta).fetchall()
            tiempos.append(time.perf_counter() - t0)
        filas.append({"formato": formato, "mb": round(os.path.getsize(ruta) / 1024**2, 1),
                      "escritura_s": round(escritura, 2), "consulta_monto_por_anio_s": round(min(tiempos), 3)})
    df = pd.DataFrame(filas)
    df["veces_mas_chico_que_csv"] = (df["mb"].iloc[0] / df["mb"]).round(1)
    comun.guardar_csv(df, "formatos", "comparacion_formatos_items")
    print(df.to_string(index=False))


if __name__ == "__main__":
    con = con_duckdb()
    duckdb_preguntas(con)
    polars_preguntas()
    formatos(con)
