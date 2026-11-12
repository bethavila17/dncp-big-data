#!/bin/bash
# Ejecuta un script del pipeline filtrando el ruido de logs de Spark/Java
cd "$(dirname "$0")"
PYTHONIOENCODING=utf-8 python "$@" 2>&1 | grep -vE "WARN|Picked up|^\s*$|CORRECTO|ha sido terminado|^\s+at |SLF4J" | cut -c1-400
