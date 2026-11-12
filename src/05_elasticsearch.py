"""Paso 5 (Elasticsearch): indexación y búsqueda para las preguntas 4 y 7.

  - Índice `dncp_contratos`: un documento por contrato y proveedor. P4: dado un RUC,
    sus contratos, entidades y montos, con el tiempo de respuesta en milisegundos.
  - Índice `dncp_items`: un documento por ítem adjudicado, con la descripción analizada
    (minúsculas y sin tildes). P7: gasto en ítems que mencionan combustible,
    medicamentos o alimentos, y entidades que concentran ese gasto.

Ambos índices tienen un único shard, así las agregaciones `terms` son exactas.

Uso:  python src/05_elasticsearch.py [--url http://localhost:9200] [--sin-carga]
"""
import argparse
import glob
import math
import time

import pandas as pd
import pyarrow.parquet as pq
from elasticsearch import Elasticsearch, helpers

import comun

IDX_CONTRATOS = f"dncp_contratos_{comun.ALCANCE}"
IDX_ITEMS = f"dncp_items_{comun.ALCANCE}"
PREFIJOS_UNSPSC = {"combustible": "1510", "medicamentos": "51", "alimentos": "50"}

AJUSTES = {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "-1",  # se reactiva al terminar la carga
    "analysis": {
        # El tokenizador estándar no corta "COMBUSTIBLE.MOVIL" ni "prov.combustibles":
        # punto, dos puntos y apóstrofos pasan a ser espacios antes de tokenizar.
        "char_filter": {"puntuacion_a_espacio": {"type": "mapping", "mappings": [
            ". => \\u0020", ": => \\u0020", "' => \\u0020", "’ => \\u0020"]}},
        "analyzer": {"es_sin_tildes": {
            "type": "custom", "char_filter": ["puntuacion_a_espacio"], "tokenizer": "standard",
            "filter": ["lowercase", "asciifolding"]}}},
}

MAPEO_CONTRATOS = {"properties": {
    "ruc": {"type": "keyword"}, "proveedor": {"type": "text", "fields": {"k": {"type": "keyword"}}},
    "ocid": {"type": "keyword"}, "contrato_id": {"type": "keyword"}, "fecha_firma": {"type": "date"},
    "entidad_id": {"type": "keyword"}, "entidad": {"type": "keyword"}, "tipo_entidad": {"type": "keyword"},
    "modalidad": {"type": "keyword"}, "categoria": {"type": "keyword"}, "anio": {"type": "integer"},
    "monto": {"type": "double"}, "moneda": {"type": "keyword"}}}

MAPEO_ITEMS = {"properties": {
    "descripcion": {"type": "text", "analyzer": "es_sin_tildes"},
    "catalogo": {"type": "text", "analyzer": "es_sin_tildes"},
    "catalogo_id": {"type": "keyword"}, "ocid": {"type": "keyword"}, "entidad": {"type": "keyword"},
    "anio": {"type": "integer"}, "monto": {"type": "double"}, "moneda": {"type": "keyword"}}}


def documentos(tabla, columnas, indice, filtro=None):
    for archivo in sorted(glob.glob(f"{comun.PARQUET_LOCAL}/{tabla}/*.parquet")):
        for lote in pq.ParquetFile(archivo).iter_batches(batch_size=20_000, columns=columnas):
            for fila in lote.to_pylist():
                if filtro and not filtro(fila):
                    continue
                doc = {k: v for k, v in fila.items()
                       if v is not None and not (isinstance(v, float) and math.isnan(v))}
                if "fecha_firma" in doc:
                    doc["fecha_firma"] = doc["fecha_firma"].isoformat()
                yield {"_index": indice, "_source": doc}


def cargar(es, indice, mapeo, acciones):
    if es.indices.exists(index=indice):
        es.indices.delete(index=indice)
    es.indices.create(index=indice, settings=AJUSTES, mappings=mapeo)
    t0 = time.time()
    ok = 0
    for exito, _ in helpers.parallel_bulk(es, acciones, thread_count=4, chunk_size=5000,
                                          request_timeout=300):
        ok += exito
    es.indices.put_settings(index=indice, settings={"refresh_interval": "1s"})
    es.indices.refresh(index=indice)
    es.indices.forcemerge(index=indice, max_num_segments=1, request_timeout=1800)
    print(f"  {indice}: {ok:,} documentos en {time.time() - t0:.0f} s")
    return ok, time.time() - t0


def p4(es):
    print("P4: consulta por RUC")
    muestra = pd.read_csv(f"{comun.RESULTADOS}/spark/p4_rucs_muestra.csv", dtype=str)["ruc"]
    filas = []
    for ruc in muestra:
        cuerpo = {
            "query": {"term": {"ruc": ruc}},
            "size": 0,
            "track_total_hits": True,
            "aggs": {"entidades": {"cardinality": {"field": "entidad_id", "precision_threshold": 40000}},
                     "pyg": {"filter": {"term": {"moneda": "PYG"}},
                             "aggs": {"monto": {"sum": {"field": "monto"}}}}},
        }
        t0 = time.perf_counter()
        r = es.search(index=IDX_CONTRATOS, body=cuerpo, request_cache=False)
        ms_total = 1000 * (time.perf_counter() - t0)
        filas.append({"ruc": ruc, "contratos": r["hits"]["total"]["value"],
                      "entidades": r["aggregations"]["entidades"]["value"],
                      "monto_pyg": r["aggregations"]["pyg"]["monto"]["value"],
                      "ms_es_servidor": r["took"], "ms_es_total": round(ms_total, 1)})
    comun.guardar_csv(pd.DataFrame(filas), "elasticsearch", "p4_consulta_ruc")

    # Listado completo para el RUC de ejemplo (el mismo que exporta Spark)
    ejemplo = muestra.iloc[0]
    t0 = time.perf_counter()
    docs = [h["_source"] for h in helpers.scan(es, index=IDX_CONTRATOS,
                                               query={"query": {"term": {"ruc": ejemplo}}})]
    print(f"  listado de {len(docs)} contratos del RUC {ejemplo}: "
          f"{1000 * (time.perf_counter() - t0):.0f} ms")
    df = pd.DataFrame(docs).sort_values("monto", ascending=False)
    comun.guardar_csv(df[["ruc", "proveedor", "ocid", "contrato_id", "fecha_firma", "entidad", "tipo_entidad",
                          "modalidad", "categoria", "monto", "moneda"]],
                      "elasticsearch", "p4_ejemplo_contratos_ruc")


def p7(es):
    print("P7: búsqueda de texto en ítems adjudicados")
    base = [{"term": {"moneda": "PYG"}}, {"exists": {"field": "monto"}}]
    aggs = {"monto": {"sum": {"field": "monto"}},
            "procesos": {"cardinality": {"field": "ocid", "precision_threshold": 40000}},
            "entidades": {"terms": {"field": "entidad", "size": 10,
                                    "order": [{"monto_entidad": "desc"}, {"_key": "asc"}]},
                          "aggs": {"monto_entidad": {"sum": {"field": "monto"}}}}}
    consultas = {
        "texto": {g: {"match": {"descripcion": {"query": " ".join(p), "operator": "or"}}}
                  for g, p in comun.TERMINOS_P7.items()},
        "catalogo": {g: {"prefix": {"catalogo_id": pref}} for g, pref in PREFIJOS_UNSPSC.items()},
    }
    for metodo, por_grupo in consultas.items():
        resumen, top = [], []
        for grupo, condicion in por_grupo.items():
            cuerpo = {"query": {"bool": {"filter": base, "must": [condicion]}}, "size": 0,
                      "track_total_hits": True, "aggs": aggs}
            r = es.search(index=IDX_ITEMS, body=cuerpo, request_cache=False)
            a = r["aggregations"]
            resumen.append({"grupo": grupo, "items": r["hits"]["total"]["value"],
                            "procesos": a["procesos"]["value"], "monto_pyg": a["monto"]["value"],
                            "ms_es_servidor": r["took"]})
            for b in a["entidades"]["buckets"]:
                top.append({"grupo": grupo, "entidad": b["key"], "monto_pyg": b["monto_entidad"]["value"],
                            "items": b["doc_count"]})
        comun.guardar_csv(pd.DataFrame(resumen), "elasticsearch", f"p7_{metodo}_resumen")
        comun.guardar_csv(pd.DataFrame(top), "elasticsearch", f"p7_{metodo}_top_entidades")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:9200")
    parser.add_argument("--sin-carga", action="store_true", help="usar los índices ya cargados")
    args = parser.parse_args()
    es = Elasticsearch(args.url, request_timeout=120)
    print(f"Elasticsearch {es.info()['version']['number']} en {args.url}")

    if not args.sin_carga:
        carga = []
        n, s = cargar(es, IDX_CONTRATOS, MAPEO_CONTRATOS, documentos(
            "contratos", ["ruc", "proveedor", "ocid", "contrato_id", "fecha_firma", "entidad_id", "entidad",
                          "tipo_entidad", "modalidad", "categoria", "anio", "monto", "moneda"], IDX_CONTRATOS))
        carga.append({"indice": IDX_CONTRATOS, "documentos": n, "segundos": round(s, 1)})
        n, s = cargar(es, IDX_ITEMS, MAPEO_ITEMS, documentos(
            "items_adjudicados", ["descripcion", "catalogo", "catalogo_id", "ocid", "entidad", "anio",
                                  "monto", "moneda"], IDX_ITEMS))
        carga.append({"indice": IDX_ITEMS, "documentos": n, "segundos": round(s, 1)})
        comun.guardar_csv(pd.DataFrame(carga), "elasticsearch", "00_carga_indices")
    p4(es)
    p7(es)


if __name__ == "__main__":
    main()
