"""Paso 6: verificación cruzada de los exportables entre herramientas.

Para cada exportable se comparan las mismas filas (por claves) y columnas entre
Spark y las demás herramientas que lo calcularon:

    P1, P2      Spark  vs MapReduce (parseo independiente del JSON)  vs Hive
    P3, P5, P6  Spark  vs Hive
    P1, P3, P5  Spark  vs DuckDB      P2, P6  Spark  vs Polars        (clase 4)
    P4          Spark  vs Elasticsearch  (vs HBase si existe resultados/<alcance>/hbase)
    P6 Spearman Spark MLlib vs pandas (recalculado desde el Parquet)
    P7          Spark  vs Elasticsearch

Conteos: deben ser idénticos. Montos: diferencia relativa < 1e-9 (la suma en punto
flotante depende del orden). Porcentajes y percentiles: diferencia < 0.011 puntos
(Hive calcula el percentil del desvío en centésimas). Resultado:
resultados/<alcance>/verificacion.csv
"""
import glob
import os

import numpy as np
import pandas as pd

import comun

EXACTO, MONTO, PCT = "exacto", "monto", "pct"
TOL = {EXACTO: 0.0, MONTO: 1e-9, PCT: 0.011}


def leer(herramienta, nombre):
    ruta = os.path.join(comun.RESULTADOS, herramienta, f"{nombre}.csv")
    if not os.path.exists(ruta):
        return None
    return pd.read_csv(ruta, dtype=str, keep_default_na=False)


def comparar(pregunta, nombre, a, b, claves, columnas, tol_extra=None):
    """Compara la herramienta `a` (referencia) con `b` en las columnas dadas."""
    da, db = leer(a, nombre), leer(b, nombre)
    fila = {"pregunta": pregunta, "exportable": nombre, "referencia": a, "comparada_con": b}
    if da is None or db is None:
        return {**fila, "estado": "SIN DATOS", "detalle": f"falta el CSV de {a if da is None else b}"}
    if not claves:  # exportable de una sola fila
        da, db, claves = da.assign(_fila="1"), db.assign(_fila="1"), ["_fila"]
    unido = da.merge(db, on=claves, how="outer", suffixes=("_a", "_b"), indicator=True)
    solo = unido["_merge"] != "both"
    fila.update(filas_ref=len(da), filas_comp=len(db), claves_sin_par=int(solo.sum()))
    peores, max_dif = [], 0.0
    ambos = unido[~solo]
    for col, tipo in columnas.items():
        x = pd.to_numeric(ambos[f"{col}_a"], errors="coerce")
        y = pd.to_numeric(ambos[f"{col}_b"], errors="coerce")
        nulos_distintos = int((x.isna() != y.isna()).sum())
        if tipo == MONTO:
            dif = (x - y).abs() / np.maximum(x.abs(), 1.0)
        else:
            dif = (x - y).abs()
        tol = (tol_extra or {}).get(col, TOL[tipo])
        malos = int((dif > tol).sum()) + nulos_distintos
        d = float(dif.max()) if len(dif.dropna()) else 0.0
        max_dif = max(max_dif, d)
        if malos:
            peores.append(f"{col}: {malos} filas (máx. dif. {d:.4g})")
    ok = not peores and fila["claves_sin_par"] == 0
    fila.update(columnas=", ".join(columnas), max_dif=max_dif, estado="OK" if ok else "DIFERENCIA",
                detalle="; ".join(peores) or ("" if ok else f"{fila['claves_sin_par']} claves sin par"))
    return fila


def spearman_pandas():
    """Recalcula el rho de Spearman de P6 con pandas, sin Spark."""
    archivos = glob.glob(f"{comun.PARQUET_LOCAL}/procesos/*.parquet")
    df = pd.concat(pd.read_parquet(a, columns=["modalidad", "dias_convocatoria", "n_oferentes"])
                   for a in archivos)
    df = df[df["dias_convocatoria"].between(0, 365) & (df["n_oferentes"] >= 1)]
    spark = leer("spark", "p6_spearman")
    filas = []
    for _, r in spark.iterrows():
        d = df if r["modalidad"] == "(todas)" else df[df["modalidad"] == r["modalidad"]]
        filas.append({"modalidad": r["modalidad"], "llamados": len(d),
                      "spearman": round(d["dias_convocatoria"].corr(d["n_oferentes"], method="spearman"), 4)})
    comun.guardar_csv(pd.DataFrame(filas), "pandas", "p6_spearman")


def main():
    spearman_pandas()
    P1_COLS = {"procesos": EXACTO, "adjudicaciones": EXACTO, "contratos": EXACTO, "monto_adjudicado_pyg": MONTO}
    P2_COLS = {"llamados_con_oferta": EXACTO, "una_sola_oferta": EXACTO, "pct_una_oferta": PCT}
    checks = []
    for otra in ["mapreduce", "hive"]:
        checks.append(comparar("P1", "p1_anio_modalidad", "spark", otra, ["anio", "modalidad"], P1_COLS))
        for dim in ["modalidad", "tipo_entidad", "anio"]:
            checks.append(comparar("P2", f"p2_unica_oferta_{dim}", "spark", otra, [dim], P2_COLS))
    checks += [
        comparar("P3", "p3_desvio_resumen", "spark", "hive", [],
                 {"procesos": EXACTO, "pct_sobre_estimado": PCT, "pct_sobre_10": PCT, "pct_bajo_10": PCT,
                  "monto_estimado_pyg": MONTO, "monto_adjudicado_pyg": MONTO,
                  "p25": PCT, "mediana": PCT, "p75": PCT, "p90": PCT}),
        comparar("P3", "p3_desvio_categoria", "spark", "hive", ["categoria"],
                 {"procesos": EXACTO, "pct_sobre_10": PCT, "desvio_agregado_pct": PCT,
                  "mediana": PCT, "p90": PCT}),
        comparar("P5", "p5_concentracion_entidad", "spark", "hive", ["entidad_id"],
                 {"adjudicaciones": EXACTO, "proveedores": EXACTO, "monto_pyg": MONTO,
                  "share_top1": PCT, "share_top10": PCT, "hhi": PCT}, {"hhi": 0.011}),
        comparar("P5", "p5_entidades_por_nivel_hhi", "spark", "hive", ["nivel_hhi"],
                 {"entidades": EXACTO, "monto_pyg": MONTO}),
        comparar("P6", "p6_dias_modalidad", "spark", "hive", ["modalidad"],
                 {"procesos": EXACTO, "media": PCT, "p25": EXACTO, "mediana": EXACTO, "p75": EXACTO}),
        comparar("P6", "p6_tramos_dias_oferentes", "spark", "hive", ["tramo_dias"],
                 {"llamados": EXACTO, "oferentes_promedio": PCT, "oferentes_mediana": EXACTO,
                  "pct_una_oferta": PCT}),
        comparar("P6", "p6_spearman", "spark", "pandas", ["modalidad"],
                 {"llamados": EXACTO, "spearman": PCT}, {"spearman": 0.0001}),
    ]
    for otra in ["elasticsearch", "hbase"]:
        if otra == "hbase" and not os.path.isdir(os.path.join(comun.RESULTADOS, "hbase")):
            continue
        checks.append(comparar("P4", "p4_consulta_ruc", "spark", otra, ["ruc"],
                               {"contratos": EXACTO, "entidades": EXACTO, "monto_pyg": MONTO}))
    checks.append(comparar("P4", "p4_ejemplo_contratos_ruc", "spark", "elasticsearch",
                           ["ocid", "contrato_id"], {"monto": MONTO}))
    for metodo in ["texto", "catalogo"]:
        checks.append(comparar("P7", f"p7_{metodo}_resumen", "spark", "elasticsearch", ["grupo"],
                               {"items": EXACTO, "procesos": EXACTO, "monto_pyg": MONTO}))
        checks.append(comparar("P7", f"p7_{metodo}_top_entidades", "spark", "elasticsearch",
                               ["grupo", "entidad"], {"monto_pyg": MONTO, "items": EXACTO}))

    # DuckDB (SQL sobre Parquet) y Polars (lazy), clase 4
    checks.append(comparar("P1", "p1_anio_modalidad", "spark", "duckdb", ["anio", "modalidad"], P1_COLS))
    checks.append(comparar("P3", "p3_desvio_resumen", "spark", "duckdb", [],
                           {"procesos": EXACTO, "pct_sobre_estimado": PCT, "pct_sobre_10": PCT, "pct_bajo_10": PCT,
                            "monto_estimado_pyg": MONTO, "monto_adjudicado_pyg": MONTO,
                            "p25": PCT, "mediana": PCT, "p75": PCT, "p90": PCT}))
    checks.append(comparar("P5", "p5_concentracion_entidad", "spark", "duckdb", ["entidad_id"],
                           {"adjudicaciones": EXACTO, "proveedores": EXACTO, "monto_pyg": MONTO,
                            "share_top1": PCT, "share_top10": PCT, "hhi": PCT}))
    for dim in ["modalidad", "tipo_entidad", "anio"]:
        checks.append(comparar("P2", f"p2_unica_oferta_{dim}", "spark", "polars", [dim], P2_COLS))
    checks.append(comparar("P6", "p6_dias_modalidad", "spark", "polars", ["modalidad"],
                           {"procesos": EXACTO, "media": PCT, "p25": EXACTO, "mediana": EXACTO, "p75": EXACTO}))
    checks.append(comparar("P6", "p6_tramos_dias_oferentes", "spark", "polars", ["tramo_dias"],
                           {"llamados": EXACTO, "oferentes_promedio": PCT, "oferentes_mediana": EXACTO,
                            "pct_una_oferta": PCT}))

    df = pd.DataFrame(checks)
    ruta = os.path.join(comun.RESULTADOS, "verificacion.csv")
    df.to_csv(ruta, index=False, encoding="utf-8-sig")
    pd.set_option("display.width", 200)
    print(df[["pregunta", "exportable", "referencia", "comparada_con", "filas_ref", "filas_comp",
              "estado", "detalle"]].to_string(index=False))
    print(f"\n{(df['estado'] == 'OK').sum()} de {len(df)} comparaciones OK -> {os.path.relpath(ruta, comun.RAIZ)}")


if __name__ == "__main__":
    main()
