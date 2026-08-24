"""
config.py
─────────
Parámetros del ETL Buk Analytics.
Editar aquí cambia el comportamiento sin tocar la lógica.
"""
# ─── Ventanas de sincronización ──────────────────────────────
# BACKFILL: cuando la BD está vacía, trae historia larga
# INCREMENTAL: corridas normales del Task Scheduler / GitHub Actions
DIAS_BACKFILL    = 55
DIAS_INCREMENTAL = 5
# ─── HTTP / Reintentos ───────────────────────────────────────
TIMEOUT_HTTP     = 30        # segundos
MAX_REINTENTOS   = 3         # cuántas veces reintenta si Buk falla
BACKOFF_INICIAL  = 2         # segundos entre reintentos (crece exponencial: 2, 4, 8)
# ─── Paginación ──────────────────────────────────────────────
PAGE_SIZE        = 100
MAX_PAGES        = 500       # tope de seguridad
# ─── Ventanas de tiempo para chunks ──────────────────────────
DIAS_POR_VENTANA = 30        # divide el rango en pedazos de 30 días
                             # (marcas consolidadas, inasistencias: 1 fila/día, livianas)

# Ventana CORTA solo para marcas granulares (obtenerRegistroAsistencia).
# POR QUÉ: ese endpoint devuelve ~22 páginas por día hábil de todo el recinto
# (25 registros/página, ignora page_size). Una ventana de 30 días ≈ 490 páginas,
# rozando MAX_PAGES=500 → pérdida silenciosa de días completos.
# Con 5 días ≈ 108 páginas/ventana: imposible topar el cap.
DIAS_POR_VENTANA_DETALLE = 5
# ─── Logging ─────────────────────────────────────────────────
CARPETA_LOGS     = "logs"    # dónde se guardan los archivos de log
