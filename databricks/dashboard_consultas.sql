-- Consultas del dashboard del Avance 1 (una por dataset).
-- Si la importación de dashboard_dncp.lvdash.json falla: New > Dashboard > pestaña Data >
-- Create from SQL, pegar cada consulta y agregar el gráfico indicado en el comentario.

-- Dataset: kpi
--   counter: Llamados publicados
--   counter: % de llamados con una sola oferta
--   counter: Monto adjudicado (billones de PYG)
SELECT (SELECT SUM(llamados) FROM workspace.dncp.oro_p1_anio)                                         AS llamados,
       (SELECT ROUND(SUM(monto_billones_pyg), 1) FROM workspace.dncp.oro_p1_anio)                     AS monto_billones,
       (SELECT ROUND(100 * SUM(una_sola_oferta) / SUM(llamados_con_oferta), 1) FROM workspace.dncp.oro_p2_anio) AS pct_una_oferta;

-- Dataset: p1_anio
--   bar: Llamados publicados por año
--   bar: Monto adjudicado por año (billones de PYG)
SELECT anio, llamados, solo_planificacion, adjudicaciones, contratos, monto_billones_pyg
FROM workspace.dncp.oro_p1_anio
WHERE anio BETWEEN '2011' AND '2025'
ORDER BY anio;

-- Dataset: p2_tipo
--   bar: % con una sola oferta por tipo de entidad
SELECT tipo_entidad, pct_una_oferta, llamados_con_oferta FROM workspace.dncp.oro_p2_tipo_entidad ORDER BY pct_una_oferta DESC;

-- Dataset: p2_modalidad
--   bar: % con una sola oferta por modalidad (1.000 llamados o más)
SELECT modalidad, pct_una_oferta, llamados_con_oferta FROM workspace.dncp.oro_p2_modalidad
WHERE llamados_con_oferta >= 1000 ORDER BY pct_una_oferta DESC;

-- Dataset: p3_resumen
--   counter: Desvío mediano (%)
--   counter: % adjudicados por encima del estimado
--   counter: % más de 10% por encima del estimado
SELECT procesos, mediana, pct_sobre_estimado, pct_sobre_10, pct_bajo_10 FROM workspace.dncp.oro_p3_resumen;

-- Dataset: p3_categoria
--   bar: Categorías con más procesos adjudicados más de 10% sobre el estimado
SELECT categoria, pct_sobre_10, mediana, procesos FROM workspace.dncp.oro_p3_categoria
ORDER BY pct_sobre_10 DESC LIMIT 10;

-- Dataset: p4_top
--   table: Los 20 proveedores con mayor monto contratado
SELECT proveedor, ruc, contratos, entidades, monto_miles_millones_pyg
FROM workspace.dncp.oro_p4_top_proveedores ORDER BY monto_miles_millones_pyg DESC LIMIT 20;

-- Dataset: p4_ruc
--   table: Contratos del RUC consultado (widget del notebook 03)
SELECT CAST(fecha_firma AS STRING) AS fecha_firma, entidad, modalidad, monto, moneda, contrato_id
FROM workspace.dncp.oro_p4_contratos_ruc ORDER BY monto DESC;

-- Dataset: p5_niveles
--   bar: Entidades por nivel de concentración (HHI)
SELECT nivel_hhi, entidades, cuota_top10_mediana FROM workspace.dncp.oro_p5_niveles_hhi ORDER BY nivel_hhi;

-- Dataset: p5_entidades
--   table: Concentración en las 15 entidades con mayor monto adjudicado
SELECT entidad, tipo_entidad, adjudicaciones, proveedores, cuota_top1, cuota_top10, hhi
FROM workspace.dncp.oro_p5_entidades WHERE adjudicaciones >= 20
ORDER BY monto_miles_millones_pyg DESC LIMIT 15;

-- Dataset: p6_modalidad
--   bar: Días de convocatoria (mediana) por modalidad
SELECT modalidad, mediana, procesos FROM workspace.dncp.oro_p6_dias_modalidad ORDER BY procesos DESC LIMIT 8;

-- Dataset: p6_tramos
--   bar: % de llamados con una sola oferta según los días de convocatoria
SELECT tramo_dias, pct_una_oferta, oferentes_promedio, llamados FROM workspace.dncp.oro_p6_tramos ORDER BY tramo_dias;

-- Dataset: p7_resumen
--   bar: Monto adjudicado por grupo (billones de PYG)
SELECT grupo, metodo, monto_billones_pyg, items FROM workspace.dncp.oro_p7_resumen;

-- Dataset: p7_entidades
--   table: Entidades que concentran el gasto de cada grupo (catálogo)
SELECT grupo, puesto, entidad, monto_billones_pyg, cuota_pct FROM workspace.dncp.oro_p7_top_entidades ORDER BY grupo, puesto;

-- Dataset: rend_opt
--   table: Optimizaciones: tiempo antes y después de cambiar una sola cosa
SELECT optimizacion, tiempo_antes_s, tiempo_despues_s, mejora_pct, plan_antes, plan_despues
FROM workspace.dncp.oro_rendimiento_optimizaciones ORDER BY optimizacion;

-- Dataset: rend_consultas
--   table: Tiempo de cada consulta (dos ejecuciones) y resumen del plan físico
SELECT consulta, tiempo_1_s, tiempo_2_s, filas_resultado, resumen_plan
FROM workspace.dncp.oro_rendimiento_consultas WHERE etapa = 'consulta';

