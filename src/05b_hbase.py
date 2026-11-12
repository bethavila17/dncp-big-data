"""Paso 5b (Apache HBase): almacén clave-valor para la pregunta 4.

Tabla `dncp_contratos_por_proveedor`, familia de columnas `c`.
Clave de fila: <ruc>|<ocid>|<contrato_id>. Al estar ordenadas por clave, todas las
filas de un proveedor quedan contiguas y se leen con un scan por prefijo, sin
recorrer la tabla: la respuesta queda en milisegundos aunque haya millones de filas.

Requiere HBase con el servidor Thrift activo (en Colab: `hbase-daemon.sh start thrift`)
y la librería happybase.

Uso:  python src/05b_hbase.py [--host localhost] [--sin-carga]
"""
import argparse
import glob
import math
import time

import pandas as pd
import pyarrow.parquet as pq

import comun

TABLA = f"dncp_contratos_por_proveedor_{comun.ALCANCE}"
COLUMNAS = ["ruc", "proveedor", "ocid", "contrato_id", "fecha_firma", "entidad_id", "entidad",
            "tipo_entidad", "modalidad", "categoria", "anio", "monto", "moneda"]


def cargar(conexion):
    if TABLA.encode() in conexion.tables():
        conexion.delete_table(TABLA, disable=True)
    conexion.create_table(TABLA, {"c": dict(max_versions=1)})
    tabla = conexion.table(TABLA)
    t0, n = time.time(), 0
    with tabla.batch(batch_size=5000) as lote:
        for archivo in sorted(glob.glob(f"{comun.PARQUET_LOCAL}/contratos/*.parquet")):
            for bloque in pq.ParquetFile(archivo).iter_batches(batch_size=20_000, columns=COLUMNAS):
                for f in bloque.to_pylist():
                    if not f["ruc"]:
                        continue
                    clave = f"{f['ruc']}|{f['ocid']}|{f['contrato_id']}".encode()
                    datos = {f"c:{k}".encode(): str(v).encode() for k, v in f.items()
                             if v is not None and not (isinstance(v, float) and math.isnan(v))}
                    lote.put(clave, datos)
                    n += 1
    print(f"  {TABLA}: {n:,} filas en {time.time() - t0:.0f} s")
    return n, time.time() - t0


def consultar(tabla, ruc):
    contratos, entidades, monto = 0, set(), None
    for _, fila in tabla.scan(row_prefix=f"{ruc}|".encode(), batch_size=1000):
        contratos += 1
        if b"c:entidad_id" in fila:
            entidades.add(fila[b"c:entidad_id"])
        if fila.get(b"c:moneda") == b"PYG" and b"c:monto" in fila:
            monto = (monto or 0.0) + float(fila[b"c:monto"])
    return contratos, len(entidades), monto


def main():
    import happybase

    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--sin-carga", action="store_true")
    args = parser.parse_args()
    conexion = happybase.Connection(args.host, timeout=600_000)
    if not args.sin_carga:
        n, s = cargar(conexion)
        comun.guardar_csv(pd.DataFrame([{"tabla": TABLA, "filas": n, "segundos": round(s, 1)}]),
                          "hbase", "00_carga_tabla")

    print("P4: consulta por RUC (scan por prefijo de clave)")
    tabla = conexion.table(TABLA)
    muestra = pd.read_csv(f"{comun.RESULTADOS}/spark/p4_rucs_muestra.csv", dtype=str)["ruc"]
    filas = []
    for ruc in muestra:
        t0 = time.perf_counter()
        contratos, entidades, monto = consultar(tabla, ruc)
        filas.append({"ruc": ruc, "contratos": contratos, "entidades": entidades, "monto_pyg": monto,
                      "ms_hbase": round(1000 * (time.perf_counter() - t0), 1)})
    comun.guardar_csv(pd.DataFrame(filas), "hbase", "p4_consulta_ruc")


if __name__ == "__main__":
    main()
