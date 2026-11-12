-- =====================================================================
-- Apache Hive: tablas externas sobre el Parquet generado por Spark y
-- consultas HiveQL para las preguntas 1, 2, 3, 5 y 6.
--
-- Cada consulta marcada con "-- exportable: <nombre>" se guarda como
-- resultados/<alcance>/hive/<nombre>.csv (ver src/04_hive.py).
-- ${PARQUET} se reemplaza por la ruta de las tablas (HDFS en Colab).
-- Solo se usan funciones disponibles en Hive 3.x y en Spark SQL.
-- =====================================================================

CREATE DATABASE IF NOT EXISTS dncp;
USE dncp;

DROP TABLE IF EXISTS procesos;
CREATE EXTERNAL TABLE procesos (
    ocid STRING, anio INT, fecha_inicio DATE, fecha_fin DATE, dias_convocatoria INT,
    modalidad STRING, metodo STRING, categoria STRING, entidad_id STRING, entidad STRING,
    tipo_entidad STRING, estado_llamado STRING, moneda_estimado STRING, monto_estimado DOUBLE,
    n_oferentes BIGINT, n_adjudicaciones INT, n_contratos INT, monto_adjudicado_pyg DOUBLE,
    n_adjudicaciones_pyg INT)
STORED AS PARQUET LOCATION '${PARQUET}/procesos';

DROP TABLE IF EXISTS adjudicaciones;
CREATE EXTERNAL TABLE adjudicaciones (
    ocid STRING, anio INT, modalidad STRING, categoria STRING, entidad_id STRING, entidad STRING,
    tipo_entidad STRING, adjudicacion_id STRING, estado STRING, valida BOOLEAN, fecha DATE,
    monto DOUBLE, moneda STRING, n_proveedores INT, ruc STRING, proveedor STRING,
    monto_atribuido DOUBLE)
STORED AS PARQUET LOCATION '${PARQUET}/adjudicaciones';

-- ---------------------------------------------------------------------
-- P1. Procesos, adjudicaciones, contratos y monto adjudicado por año y modalidad
-- ---------------------------------------------------------------------
-- exportable: p1_anio_modalidad
SELECT COALESCE(CAST(anio AS STRING), '(sin dato)') AS anio,
       COALESCE(modalidad, '(sin dato)')            AS modalidad,
       COUNT(*)                                      AS procesos,
       SUM(n_adjudicaciones)                         AS adjudicaciones,
       SUM(n_contratos)                              AS contratos,
       SUM(monto_adjudicado_pyg)                     AS monto_adjudicado_pyg
FROM procesos
GROUP BY COALESCE(CAST(anio AS STRING), '(sin dato)'), COALESCE(modalidad, '(sin dato)')
ORDER BY anio, modalidad;

-- ---------------------------------------------------------------------
-- P2. Proporción de llamados con una sola oferta
-- ---------------------------------------------------------------------
-- exportable: p2_unica_oferta_modalidad
SELECT COALESCE(modalidad, '(sin dato)') AS modalidad,
       COUNT(*) AS llamados_con_oferta,
       SUM(CASE WHEN n_oferentes = 1 THEN 1 ELSE 0 END) AS una_sola_oferta,
       ROUND(100 * SUM(CASE WHEN n_oferentes = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_una_oferta
FROM procesos
WHERE n_oferentes >= 1
GROUP BY COALESCE(modalidad, '(sin dato)')
ORDER BY modalidad;

-- exportable: p2_unica_oferta_tipo_entidad
SELECT tipo_entidad,
       COUNT(*) AS llamados_con_oferta,
       SUM(CASE WHEN n_oferentes = 1 THEN 1 ELSE 0 END) AS una_sola_oferta,
       ROUND(100 * SUM(CASE WHEN n_oferentes = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_una_oferta
FROM procesos
WHERE n_oferentes >= 1
GROUP BY tipo_entidad
ORDER BY tipo_entidad;

-- exportable: p2_unica_oferta_anio
SELECT COALESCE(CAST(anio AS STRING), '(sin dato)') AS anio,
       COUNT(*) AS llamados_con_oferta,
       SUM(CASE WHEN n_oferentes = 1 THEN 1 ELSE 0 END) AS una_sola_oferta,
       ROUND(100 * SUM(CASE WHEN n_oferentes = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS pct_una_oferta
FROM procesos
WHERE n_oferentes >= 1
GROUP BY COALESCE(CAST(anio AS STRING), '(sin dato)')
ORDER BY anio;

-- ---------------------------------------------------------------------
-- P3. Desvío porcentual entre monto estimado y adjudicado (procesos en PYG)
-- ---------------------------------------------------------------------
DROP VIEW IF EXISTS desvios;
CREATE VIEW desvios AS
SELECT COALESCE(categoria, '(sin dato)') AS categoria,
       COALESCE(modalidad, '(sin dato)') AS modalidad,
       monto_estimado, monto_adjudicado_pyg,
       100 * (monto_adjudicado_pyg - monto_estimado) / monto_estimado AS desvio_pct,
       -- PERCENTILE de Hive es exacto pero solo acepta enteros: desvío en centésimas de punto
       CAST(ROUND(10000 * (monto_adjudicado_pyg - monto_estimado) / monto_estimado) AS BIGINT)
           AS desvio_centesimas
FROM procesos
WHERE moneda_estimado = 'PYG' AND monto_estimado > 0
  AND n_adjudicaciones_pyg > 0 AND monto_adjudicado_pyg > 0;

-- exportable: p3_desvio_resumen
SELECT COUNT(*) AS procesos,
       PERCENTILE(desvio_centesimas, 0.25) / 100 AS p25,
       PERCENTILE(desvio_centesimas, 0.5) / 100  AS mediana,
       PERCENTILE(desvio_centesimas, 0.75) / 100 AS p75,
       PERCENTILE(desvio_centesimas, 0.9) / 100  AS p90,
       ROUND(100 * AVG(CASE WHEN desvio_pct > 0 THEN 1 ELSE 0 END), 2)   AS pct_sobre_estimado,
       ROUND(100 * AVG(CASE WHEN desvio_pct > 10 THEN 1 ELSE 0 END), 2)  AS pct_sobre_10,
       ROUND(100 * AVG(CASE WHEN desvio_pct < -10 THEN 1 ELSE 0 END), 2) AS pct_bajo_10,
       SUM(monto_estimado)       AS monto_estimado_pyg,
       SUM(monto_adjudicado_pyg) AS monto_adjudicado_pyg
FROM desvios;

-- exportable: p3_desvio_categoria
SELECT categoria,
       COUNT(*) AS procesos,
       PERCENTILE(desvio_centesimas, 0.5) / 100 AS mediana,
       PERCENTILE(desvio_centesimas, 0.9) / 100 AS p90,
       ROUND(100 * AVG(CASE WHEN desvio_pct > 0 THEN 1 ELSE 0 END), 2)  AS pct_sobre_estimado,
       ROUND(100 * AVG(CASE WHEN desvio_pct > 10 THEN 1 ELSE 0 END), 2) AS pct_sobre_10,
       SUM(monto_estimado)       AS monto_estimado_pyg,
       SUM(monto_adjudicado_pyg) AS monto_adjudicado_pyg,
       ROUND(100 * (SUM(monto_adjudicado_pyg) - SUM(monto_estimado)) / SUM(monto_estimado), 2)
           AS desvio_agregado_pct
FROM desvios
GROUP BY categoria
HAVING COUNT(*) >= 30
ORDER BY pct_sobre_10 DESC;

-- ---------------------------------------------------------------------
-- P5. Concentración: participación del top 10 e índice Herfindahl-Hirschman
-- ---------------------------------------------------------------------
DROP VIEW IF EXISTS cuotas_entidad;
CREATE VIEW cuotas_entidad AS
SELECT entidad_id, ruc, monto, n,
       100 * monto / SUM(monto) OVER (PARTITION BY entidad_id) AS share,
       ROW_NUMBER() OVER (PARTITION BY entidad_id ORDER BY monto DESC, ruc) AS rango
FROM (
    SELECT entidad_id, ruc, SUM(monto_atribuido) AS monto, COUNT(*) AS n
    FROM adjudicaciones
    WHERE valida AND moneda = 'PYG' AND monto_atribuido > 0
      AND ruc IS NOT NULL AND entidad_id IS NOT NULL
    GROUP BY entidad_id, ruc
) por_proveedor;

-- Nombre y tipo de cada entidad (el nombre cambia entre años; se toma uno fijo)
DROP VIEW IF EXISTS entidades;
CREATE VIEW entidades AS
SELECT entidad_id, MAX(entidad) AS entidad, MAX(tipo_entidad) AS tipo_entidad
FROM adjudicaciones
WHERE valida AND moneda = 'PYG' AND monto_atribuido > 0
  AND ruc IS NOT NULL AND entidad_id IS NOT NULL
GROUP BY entidad_id;

-- exportable: p5_concentracion_entidad
SELECT c.entidad_id, e.entidad, e.tipo_entidad, c.adjudicaciones, c.proveedores, c.monto_pyg,
       c.share_top1, c.share_top10, c.hhi
FROM (
    SELECT entidad_id,
           SUM(n) AS adjudicaciones,
           COUNT(*) AS proveedores,
           SUM(monto) AS monto_pyg,
           ROUND(SUM(CASE WHEN rango = 1 THEN share END), 4)   AS share_top1,
           ROUND(SUM(CASE WHEN rango <= 10 THEN share END), 4) AS share_top10,
           ROUND(SUM(share * share), 4) AS hhi
    FROM cuotas_entidad
    GROUP BY entidad_id
) c
JOIN entidades e ON c.entidad_id = e.entidad_id
ORDER BY monto_pyg DESC;

-- exportable: p5_entidades_por_nivel_hhi
SELECT nivel_hhi, COUNT(*) AS entidades, SUM(monto_pyg) AS monto_pyg
FROM (
    SELECT CASE WHEN hhi < 1500 THEN '1. no concentrado (<1500)'
                WHEN hhi <= 2500 THEN '2. moderado (1500-2500)'
                ELSE '3. alto (>2500)' END AS nivel_hhi,
           monto_pyg
    FROM (
        SELECT SUM(n) AS adjudicaciones, SUM(monto) AS monto_pyg, SUM(share * share) AS hhi
        FROM cuotas_entidad
        GROUP BY entidad_id
    ) e
    WHERE adjudicaciones >= 20
) c
GROUP BY nivel_hhi
ORDER BY nivel_hhi;

-- ---------------------------------------------------------------------
-- P6. Duración de la convocatoria y cantidad de oferentes
-- ---------------------------------------------------------------------
-- exportable: p6_dias_modalidad
SELECT COALESCE(modalidad, '(sin dato)') AS modalidad,
       COUNT(*) AS procesos,
       ROUND(AVG(dias_convocatoria), 2) AS media,
       PERCENTILE(CAST(dias_convocatoria AS BIGINT), 0.25) AS p25,
       PERCENTILE(CAST(dias_convocatoria AS BIGINT), 0.5)  AS mediana,
       PERCENTILE(CAST(dias_convocatoria AS BIGINT), 0.75) AS p75
FROM procesos
WHERE dias_convocatoria BETWEEN 0 AND 365
GROUP BY COALESCE(modalidad, '(sin dato)')
HAVING COUNT(*) >= 30
ORDER BY procesos DESC;

-- exportable: p6_tramos_dias_oferentes
SELECT tramo_dias,
       COUNT(*) AS llamados,
       ROUND(AVG(n_oferentes), 4) AS oferentes_promedio,
       PERCENTILE(n_oferentes, 0.5) AS oferentes_mediana,
       ROUND(100 * AVG(CASE WHEN n_oferentes = 1 THEN 1 ELSE 0 END), 2) AS pct_una_oferta
FROM (
    SELECT n_oferentes,
           CASE WHEN dias_convocatoria <= 7  THEN '0-7'
                WHEN dias_convocatoria <= 14 THEN '08-14'
                WHEN dias_convocatoria <= 21 THEN '15-21'
                WHEN dias_convocatoria <= 30 THEN '22-30'
                WHEN dias_convocatoria <= 60 THEN '31-60'
                ELSE '61-365' END AS tramo_dias
    FROM procesos
    WHERE dias_convocatoria BETWEEN 0 AND 365 AND n_oferentes >= 1
) t
GROUP BY tramo_dias
ORDER BY tramo_dias;
