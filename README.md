# Electro Buk Analytics

ETL de sincronizacion entre la plataforma Buk (Gestion Humana + Asistencia) y una base de datos PostgreSQL en Supabase, con corrida programada via GitHub Actions.

## Objetivo

Consolidar en un unico data warehouse los datos operativos de personas de Electroingenieria S.A.S. para alimentar dashboards de Power BI y analisis de indicadores de gestion humana.

## Arquitectura

- **Fuente**: Buk Core (empleados, areas) + Buk Asistencia (marcas, inasistencias, recintos)
- **Orquestacion**: GitHub Actions con corrida diaria programada
- **Storage**: Supabase (PostgreSQL 17)
- **Visualizacion**: Power BI Service

## Tablas

- `empleados` — nomina actual (refresh completo)
- `recintos` — catalogo de sedes (refresh completo)
- `asistencias_marcas` — historial de marcaciones (upsert por id)
- `inasistencias` — ausencias (upsert por llave compuesta)
- `sync_log` — bitacora de corridas del ETL

## Uso local
python -m venv .venv
..venv\Scripts\Activate.ps1
pip install -r requirements.txt
python main_v2.py
## Autor

Juan Montoya — Analista de Calidad y Transformacion Digital  
Electroingenieria S.A.S. — 2026