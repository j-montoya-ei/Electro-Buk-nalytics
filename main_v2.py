"""
main_v2.py
──────────
ETL Buk Core + Buk Asistencia → Supabase (PostgreSQL).

Diferencias vs main.py (v1 - SQLite):
  1. Escribe en PostgreSQL en la nube (portable, sin depender del PC)
  2. UPSERT nativo con ON CONFLICT
  3. Logging profesional a archivo + consola
  4. Reintentos con backoff exponencial ante errores 5xx de Buk
  5. Bitácora de corridas en tabla sync_log (observabilidad)
  6. Configuración externalizada en config.py y jerarquia.py
"""
import os
import sys
import time
import logging
import requests
import pandas as pd
import psycopg2
import calendar

from psycopg2.extras import execute_values
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# Módulos propios
import config
from jerarquia import jerarquia, homologar_sede, interpretar_horario

# ═══════════════════════════════════════════════════════════════════════
# 1. CONFIGURACIÓN INICIAL
# ═══════════════════════════════════════════════════════════════════════
load_dotenv()

# Rutas (portables, no dependen del PC)
BASE_DIR = Path(__file__).parent
LOGS_DIR = BASE_DIR / config.CARPETA_LOGS
LOGS_DIR.mkdir(exist_ok=True)


# ─── Logging: consola + archivo ─────────────────────────────────────────
def setup_logging():
    """Configura logging a archivo y a consola con timestamps."""
    log_file = LOGS_DIR / f"etl_{datetime.now().strftime('%Y-%m-%d_%H%M%S')}.log"

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%H:%M:%S"
    )

    # Handler consola
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)

    # Handler archivo
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()
    root.addHandler(console)
    root.addHandler(file_handler)

    return log_file


LOG_FILE = setup_logging()
log = logging.getLogger(__name__)


# ─── Credenciales ───────────────────────────────────────────────────────
def env(key):
    v = os.getenv(key)
    return v.strip() if v else None


TOKEN_CORE  = env("BUK_TOKEN")
URL_CORE    = env("BASE_URL")
HDR_CORE    = {"Accept": "application/json", "Auth-Token": TOKEN_CORE}

TOKEN_ASIST = env("BUK_ASISTENCIA_TOKEN")
URL_ASIST   = env("URL_ASISTENCIA")
HDR_ASIST   = {"Accept": "application/json", "token": TOKEN_ASIST}

DB_URL      = env("SUPABASE_DB_URL")

# Validación temprana
if not all([TOKEN_CORE, URL_CORE, TOKEN_ASIST, URL_ASIST, DB_URL]):
    log.error("❌ Falta alguna variable en el .env. Revisar BUK_TOKEN, BASE_URL, "
              "BUK_ASISTENCIA_TOKEN, URL_ASISTENCIA, SUPABASE_DB_URL")
    sys.exit(1)


# ═══════════════════════════════════════════════════════════════════════
# 2. HELPERS HTTP CON REINTENTOS
# ═══════════════════════════════════════════════════════════════════════
def http_get_con_retry(url, headers, params=None):
    """GET con reintentos y backoff exponencial ante errores 5xx.
    Retorna Response o None si agotó los reintentos.
    """
    for intento in range(1, config.MAX_REINTENTOS + 1):
        try:
            r = requests.get(url, headers=headers, params=params or {},
                             timeout=config.TIMEOUT_HTTP)

            # 2xx = éxito
            if 200 <= r.status_code < 300:
                return r

            # 429 (rate limit) y 408 (timeout) SÍ se reintentan (respetando Retry-After)
            if r.status_code in (429, 408):
                retry_after = r.headers.get("Retry-After")
                espera_forzada = int(retry_after) if retry_after and retry_after.isdigit() else None
                log.warning(f"   ⚠️  {r.status_code} en {url} "
                            f"(rate limit/timeout, intento {intento}/{config.MAX_REINTENTOS})")
                if intento < config.MAX_REINTENTOS:
                    espera = espera_forzada or config.BACKOFF_INICIAL * (2 ** (intento - 1))
                    log.info(f"   ⏳ Esperando {espera}s (rate limit)...")
                    time.sleep(espera)
                continue

            # Resto de 4xx = error del cliente, no tiene sentido reintentar
            if 400 <= r.status_code < 500:
                log.warning(f"   ⚠️  {r.status_code} en {url} (error del cliente, no reintenta)")
                log.warning(f"      Respuesta: {r.text[:200]}")
                return r

            # 5xx = error del servidor, reintentar
            log.warning(f"   ⚠️  {r.status_code} en {url} (intento {intento}/{config.MAX_REINTENTOS})")

        except requests.exceptions.RequestException as e:
            log.warning(f"   ⚠️  Excepción HTTP: {e} (intento {intento}/{config.MAX_REINTENTOS})")

        # Backoff exponencial: 2s, 4s, 8s...
        if intento < config.MAX_REINTENTOS:
            espera = config.BACKOFF_INICIAL * (2 ** (intento - 1))
            log.info(f"   ⏳ Esperando {espera}s antes de reintentar...")
            time.sleep(espera)

    log.error(f"   ❌ Agotados los {config.MAX_REINTENTOS} reintentos para {url}")
    return None


def get_core(endpoint):
    """Consume un endpoint de Buk Core con paginación completa.

    Retorna (DataFrame|None, completo: bool).
    completo=False ⇒ la descarga se cortó por un error de red/API y los datos
    están INCOMPLETOS. En ese caso NO es seguro hacer refresh_completo (TRUNCATE),
    porque borraría filas que sí existen pero no se alcanzaron a bajar.
    """
    datos, pag, completo = [], 1, True
    while pag <= config.MAX_PAGES:
        r = http_get_con_retry(
            f"{URL_CORE}/{endpoint}?per_page={config.PAGE_SIZE}&page={pag}",
            headers=HDR_CORE
        )
        # Corte por ERROR → descarga incompleta
        if r is None or r.status_code != 200:
            log.error(f"   ❌ {endpoint}: página {pag} falló → descarga INCOMPLETA")
            completo = False
            break
        chunk = r.json().get("data", [])
        # ÚNICO corte por FIN de datos → página vacía = ya no hay más.
        # NO asumimos el tamaño de página: Buk puede devolver 25 aunque pidas 100.
        if not chunk:
            break
        datos.extend(chunk)
        log.info(f"   ✅ {endpoint}: página {pag} → {len(chunk)} filas")
        pag += 1
    df = pd.DataFrame(datos) if datos else None
    return df, completo


def get_asist(endpoint, params=None):
    """Consume un endpoint de Buk Asistencia (una sola página)."""
    r = http_get_con_retry(f"{URL_ASIST}/{endpoint}", headers=HDR_ASIST, params=params)
    if r is None or r.status_code != 200:
        return None
    body = r.json()
    if isinstance(body, list):
        return pd.DataFrame(body)
    for key in ("data", "items", "asistencias", "registros"):
        if key in body and body[key]:
            return pd.DataFrame(body[key])
    return pd.DataFrame([body]) if body else None


def get_asist_paginated(endpoint, params=None):
    """Paginación completa para endpoints de Asistencia.

    Corta cuando llega una página vacía. Válido para endpoints con pocos
    registros por ventana (marcas consolidadas, inasistencias, horas extras).
    Para endpoints con MUCHAS páginas y bloque `pagination.totalPages`, usar
    get_asist_paginated_v2 (corta por totalPages, no espera la página vacía).
    """
    base = dict(params or {})
    partes = []
    for pag in range(1, config.MAX_PAGES + 1):
        p = {**base, "page": pag, "page_size": config.PAGE_SIZE}
        df = get_asist(endpoint, p)
        # ÚNICO corte: página vacía. Igual que get_core, sin asumir tamaño de página.
        if df is None or df.empty:
            break
        partes.append(df)
        log.info(f"      📄 página {pag}: {len(df)} filas")
    return pd.concat(partes, ignore_index=True) if partes else None


def get_asist_paginated_v2(endpoint, params=None):
    """Paginación para endpoints de Asistencia que exponen `pagination.totalPages`
    (ej. obtenerRegistroAsistencia).

    Retorna (DataFrame|None, completo: bool)  ← MISMO CONTRATO QUE get_core.
    completo=False ⇒ la ventana se cortó por un error de red/API (reintentos
    agotados) o por el tope MAX_PAGES. Los datos de ESA ventana están
    INCOMPLETOS. El caller debe registrarlo y NO tratar la corrida como limpia.

    POR QUÉ EXISTE: este endpoint IGNORA el page_size solicitado y entrega 25
    filas por página. Con datos de todo el recinto son cientos de páginas por
    ventana; el paginador clásico (get_asist_paginated), que solo corta al llegar
    a una página vacía, tardaría demasiado y además se toparía con MAX_PAGES
    perdiendo datos. Aquí cortamos usando el `totalPages` que envía Buk.

    FALLBACK: si la respuesta NO trae `pagination.totalPages`, se comporta como el
    paginador clásico (corta al llegar a página vacía, con tope MAX_PAGES → que
    en ese caso marca la ventana como INCOMPLETA).
    """
    base = dict(params or {})
    partes = []
    pag = 1
    total_pages = None
    completo = True

    while True:
        p = {**base, "page": pag, "page_size": config.PAGE_SIZE}
        r = http_get_con_retry(f"{URL_ASIST}/{endpoint}", headers=HDR_ASIST, params=p)

        # ─── Corte por ERROR ───
        # Antes esto hacía un `break` silencioso: devolvía lo parcial como si
        # hubiera terminado bien. Ahora se marca INCOMPLETO para no disfrazar un
        # hueco de éxito. El caller decide (aquí: sube lo que haya y avisa fuerte).
        if r is None or r.status_code != 200:
            log.error(f"   ❌ {endpoint}: página {pag} falló tras reintentos "
                      f"→ ventana INCOMPLETA")
            completo = False
            break

        body = r.json()

        # Separar registros de metadatos de paginación
        if isinstance(body, dict):
            paginacion = body.get("pagination") or {}
            registros = None
            for key in ("data", "items", "asistencias", "registros"):
                if body.get(key):
                    registros = body[key]
                    break
            registros = registros or []
        elif isinstance(body, list):
            paginacion, registros = {}, body
        else:
            paginacion, registros = {}, []

        # Capturar totalPages la primera vez que aparezca
        if total_pages is None:
            tp = paginacion.get("totalPages")
            if isinstance(tp, int) and tp > 0:
                total_pages = tp

        if registros:
            partes.append(pd.DataFrame(registros))
            log.info(f"      📄 página {pag}/{total_pages or '?'}: {len(registros)} filas")

        # ─── Criterio de corte ───
        if total_pages is not None:
            # Confiamos en el conteo de Buk. Sin tope MAX_PAGES aquí, para no
            # perder datos si hay más de MAX_PAGES páginas legítimas.
            if pag >= total_pages:
                break
        else:
            # Sin totalPages → comportamiento clásico: parar en página vacía.
            if not registros:
                break
            if pag >= config.MAX_PAGES:
                # Tope alcanzado SIN saber cuántas páginas faltan → INCOMPLETO.
                log.warning(f"   ⚠️  {endpoint}: tope MAX_PAGES ({config.MAX_PAGES}) "
                            f"sin totalPages → ventana INCOMPLETA")
                completo = False
                break

        pag += 1

    df = pd.concat(partes, ignore_index=True) if partes else None
    return df, completo


def chunks_fechas(desde, hasta, dias=None):
    """Divide un rango (desde, hasta) en ventanas de N días."""
    dias = dias or config.DIAS_POR_VENTANA
    cur = desde
    while cur <= hasta:
        fin = min(cur + timedelta(days=dias - 1), hasta)
        yield cur.strftime("%d-%m-%Y"), fin.strftime("%d-%m-%Y")
        cur = fin + timedelta(days=1)


# ═══════════════════════════════════════════════════════════════════════
# 3. HELPERS DE BASE DE DATOS (PostgreSQL / Supabase)
# ═══════════════════════════════════════════════════════════════════════
def conectar():
    """Abre una conexión a Supabase."""
    return psycopg2.connect(DB_URL)


def tabla_vacia(conn, tabla):
    """True si la tabla no tiene filas."""
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM {tabla};')
        return cur.fetchone()[0] == 0


def upsert_df(conn, df, tabla, pk_cols, cols_actualizar=None):
    """UPSERT de un DataFrame a PostgreSQL usando ON CONFLICT.

    Args:
        conn: conexión abierta a Supabase
        df: DataFrame a insertar
        tabla: nombre de la tabla destino
        pk_cols: tuple con columnas de la llave primaria (ej: ("id",) o ("obra_id","dni","ano","mes","dia"))
        cols_actualizar: columnas a actualizar en caso de conflicto. Si None, actualiza todas menos las PK.

    Returns:
        (filas_insertadas_o_actualizadas, filas_totales_en_df)
    """
    if df is None or df.empty:
        log.warning(f"   ⚠️  {tabla}: sin datos para cargar")
        return 0, 0

    # Filtrar columnas que existen en la tabla real
    cols_df = list(df.columns)

    if cols_actualizar is None:
        cols_actualizar = [c for c in cols_df if c not in pk_cols]

    # Construir la sentencia
    cols_str = ", ".join(f'"{c}"' for c in cols_df)
    pk_str   = ", ".join(f'"{c}"' for c in pk_cols)

    if cols_actualizar:
        set_str = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in cols_actualizar)
        conflict_action = f"DO UPDATE SET {set_str}"
    else:
        conflict_action = "DO NOTHING"

    sql = f"""
        INSERT INTO {tabla} ({cols_str})
        VALUES %s
        ON CONFLICT ({pk_str}) {conflict_action};
    """

    # Convertir DataFrame a lista de tuplas, convirtiendo NaN a None
    rows = [tuple(None if pd.isna(v) else v for v in row) for row in df.to_numpy()]

    with conn.cursor() as cur:
        # Contar antes
        cur.execute(f'SELECT COUNT(*) FROM {tabla};')
        antes = cur.fetchone()[0]

        # Ejecutar UPSERT
        execute_values(cur, sql, rows, page_size=500)

        # Contar después
        cur.execute(f'SELECT COUNT(*) FROM {tabla};')
        despues = cur.fetchone()[0]

    conn.commit()

    nuevas       = despues - antes
    actualizadas = len(df) - nuevas
    log.info(f"   ✅ {tabla:<26} → {despues} totales ({nuevas} nuevas, {actualizadas} actualizadas)")
    return nuevas, actualizadas


def refresh_completo(conn, df, tabla):
    """Reemplaza el contenido completo de una tabla (para empleados/recintos)."""
    if df is None or df.empty:
        log.warning(f"   ⚠️  {tabla}: sin datos")
        return 0

    cols_str = ", ".join(f'"{c}"' for c in df.columns)
    rows = [tuple(None if pd.isna(v) else v for v in row) for row in df.to_numpy()]

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {tabla};")
        sql = f"INSERT INTO {tabla} ({cols_str}) VALUES %s;"
        execute_values(cur, sql, rows, page_size=500)

    conn.commit()
    log.info(f"   ✅ {tabla:<26} → {len(df)} filas (refresh completo)")
    return len(df)


# ═══════════════════════════════════════════════════════════════════════
# 4. TRANSFORMACIONES (limpieza + enriquecimiento)
# ═══════════════════════════════════════════════════════════════════════
def transformar_empleados(df_emp, df_area):
    """Aplica limpieza + reglas de negocio a los empleados.
 
    Caracterización (#8): además de los campos base, extrae 4 atributos
    verificados en la respuesta cruda de Buk Core `employees`:
      · escolaridad   ← custom_attributes NIVEL EMPLEADO · llave "Escolaridad"
      · estrato       ← custom_attributes NIVEL EMPLEADO · llave "Estrato Socioeconómico"
      · tipo_contrato ← current_job · llave "type_of_contract"
      · clasificacion ← current_job.custom_attributes · llave "Clasificación"
 
    OJO: hay DOS objetos 'custom_attributes' distintos (uno a nivel empleado y
    otro dentro de current_job). escolaridad/estrato salen del PRIMERO;
    clasificacion del SEGUNDO. Mezclarlos traería null en silencio.
    """
    if df_emp is None or df_area is None:
        log.error("   ❌ No hay datos de empleados o áreas para transformar")
        return None
 
    # Normaliza texto: recorta espacios y convierte vacío/None a None, para que
    # "no diligenciado" sea NULL y no un string vacío que ensucia las distribuciones.
    def _txt(v):
        if v is None:
            return None
        s = str(v).strip()
        return s or None
 
    nombres_map = dict(zip(df_area["id"], df_area["name"]))
 
    df = df_emp[["document_number", "first_name", "surname", "second_surname",
                 "current_job", "status", "birthday", "active_since", "gender"]].copy()
 
    # custom_attributes de NIVEL EMPLEADO (distinto del de current_job).
    # Defensivo: si el campo faltara en una corrida, escolaridad/estrato quedan
    # None en vez de tumbar TODO el ETL (mismo criterio que el resto del archivo).
    df["_ca_emp"] = df_emp["custom_attributes"] if "custom_attributes" in df_emp.columns else None
 
    df["Sub_Area"] = df["current_job"].apply(
        lambda x: nombres_map.get(x.get("area_id")) if isinstance(x, dict) else "N/A"
    )
    df[["Area", "Division"]] = df["Sub_Area"].apply(lambda x: pd.Series(jerarquia(x)))
    df["Cargo"] = df["current_job"].apply(
        lambda x: x.get("role", {}).get("name") if isinstance(x, dict) else "N/A"
    )
    df["Sede"] = df["current_job"].apply(
        lambda x: homologar_sede(x.get("custom_attributes", {}).get("Sede")) if isinstance(x, dict) else "N/A"
    )
    df["Horario"] = df["current_job"].apply(interpretar_horario)
 
    # ─── Caracterización (#8): 4 atributos nuevos ────────────────────────
    # Tipo de contrato → current_job.type_of_contract
    df["Tipo_Contrato"] = df["current_job"].apply(
        lambda x: _txt(x.get("type_of_contract")) if isinstance(x, dict) else None
    )
    # Clasificación (Administrativo/Operativo) → current_job.custom_attributes
    df["Clasificacion"] = df["current_job"].apply(
        lambda x: _txt((x.get("custom_attributes") or {}).get("Clasificación"))
        if isinstance(x, dict) else None
    )
    # Escolaridad → custom_attributes NIVEL EMPLEADO
    df["Escolaridad"] = df["_ca_emp"].apply(
        lambda x: _txt(x.get("Escolaridad")) if isinstance(x, dict) else None
    )
    # Estrato → custom_attributes NIVEL EMPLEADO (llega como texto, p.ej. "3")
    df["Estrato"] = df["_ca_emp"].apply(
        lambda x: _txt(x.get("Estrato Socioeconómico")) if isinstance(x, dict) else None
    )
 
    df["Nombre_Completo"] = (
        df["first_name"].fillna("") + " " +
        df["surname"].fillna("") + " " +
        df["second_surname"].fillna("")
    ).str.upper().str.strip().str.replace(r"\s+", " ", regex=True)
    df["Genero"] = df["gender"].map({"M": "Masculino", "F": "Femenino"}).fillna("N/A")
 
    # Fechas → tipo date (PostgreSQL)
    df["Fecha_Nacimiento"] = pd.to_datetime(df["birthday"],     errors="coerce").dt.date
    df["Fecha_Ingreso"]    = pd.to_datetime(df["active_since"], errors="coerce").dt.date
 
    # Edad
    hoy = datetime.now().date()
    df["Edad"] = df["Fecha_Nacimiento"].apply(
        lambda b: (hoy - b).days // 365 if pd.notna(b) else None
    )
 
    df_final = df[[
        "document_number", "Nombre_Completo", "Genero", "Fecha_Nacimiento", "Edad",
        "Cargo", "Sede", "Division", "Area", "Sub_Area", "status", "Fecha_Ingreso", "Horario",
        "Escolaridad", "Estrato", "Tipo_Contrato", "Clasificacion"
    ]].copy()
 
    # Renombrar a snake_case (para que coincida con la tabla en Postgres)
    df_final.columns = [
        "documento", "nombre_completo", "genero", "fecha_nacimiento", "edad",
        "cargo", "sede", "division", "area", "sub_area", "estado", "fecha_ingreso", "horario",
        "escolaridad", "estrato", "tipo_contrato", "clasificacion"
    ]
 
    # Deduplicar por documento (por si viene el mismo dos veces)
    df_final = df_final.drop_duplicates(subset=["documento"]).reset_index(drop=True)
    return df_final


def transformar_recintos(df_recintos):
    """Normaliza el DataFrame de recintos al esquema de Postgres."""
    if df_recintos is None or df_recintos.empty:
        return None

    df = df_recintos.copy()
    # La API a veces trae obraId (camelCase) → normalizamos a obra_id
    if "obraId" in df.columns and "obra_id" not in df.columns:
        df = df.rename(columns={"obraId": "obra_id"})

    # Quedarnos solo con las columnas del esquema
    cols_esperadas = ["obra_id", "nombre", "direccion", "comuna", "region", "pais"]
    cols_presentes = [c for c in cols_esperadas if c in df.columns]
    df = df[cols_presentes].copy()

    # Convertir obra_id a texto (nuestra tabla usa TEXT como PK)
    df["obra_id"] = df["obra_id"].astype(str)
    return df


def transformar_marcas(df_marcas):
    """Limpia y enriquece las marcas de asistencia."""
    if df_marcas is None or df_marcas.empty:
        return None

    df = df_marcas.copy()

    # Nombre completo (combinar 3 campos)
    cols_nombre = [c for c in ("nombre", "apellido_paterno", "apellido_materno") if c in df.columns]
    if cols_nombre:
        df["nombre_completo"] = (
            df[cols_nombre].fillna("").astype(str).agg(" ".join, axis=1)
            .str.replace(r"\s+", " ", regex=True).str.strip().str.upper()
        )
        df = df.drop(columns=cols_nombre)

    # Entrada: usar entrada_format que ya viene en hora local
    if "entrada_format" in df.columns:
        ts_e = pd.to_datetime(df["entrada_format"], format="%Y/%m/%d %H:%M:%S", errors="coerce")
        df["fecha_entrada"] = ts_e.dt.date
        df["hora_entrada"]  = ts_e.dt.time

    # Salida
    if "salida_format" in df.columns:
        salida_clean = df["salida_format"].replace("-", pd.NA)
        ts_s = pd.to_datetime(salida_clean, format="%Y/%m/%d %H:%M:%S", errors="coerce")
        df["fecha_salida"] = ts_s.dt.date
        df["hora_salida"]  = ts_s.dt.time

    # Convertir campos UTC a timestamp con zona horaria
    for col in ("entrada", "salida"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Booleanos
    for col in ("turno_noche", "art22"):
        if col in df.columns:
            df[col] = df[col].astype(bool)

    # Descartar marcas sin id (la PK es NOT NULL) + dedup por id
    if "id" in df.columns:
        df = df.dropna(subset=["id"])
        df = df.drop_duplicates(subset=["id"]).reset_index(drop=True)
    else:
        # Sin columna id no hay nada válido que insertar
        return None

    # Quedarnos solo con las columnas del esquema Postgres
    cols_esperadas = [
        "id", "trab_id", "rut_trabajador", "id_recinto", "nombre_recinto",
        "codigo_recinto", "rut_empleador", "especialidad", "area", "contrato",
        "supervisor", "entrada", "turno_noche", "salida", "entrada_turno",
        "salida_turno", "dia_entrada", "entrada_format", "salida_format", "art22",
        "turno", "codigo_turno", "nombre_completo", "fecha_entrada", "hora_entrada",
        "fecha_salida", "hora_salida"
    ]
    cols_presentes = [c for c in cols_esperadas if c in df.columns]
    return df[cols_presentes].copy()


def transformar_marcas_detalle(df_det):
    """Normaliza las marcas granulares (obtenerRegistroAsistencia) al esquema
    de la tabla asistencias_marcas_detalle.

    Cada fila del API = una marca (entrada o salida). Buk envía la fecha y la
    hora partidas en enteros (ano/mes/dia + hora/minutos/segundos); aquí se
    reconstruyen a `fecha` (date) y `hora_marca` (time).

    Tabla destino:
      PK  (obra_id, dni, fecha, hora_marca, sentido)
      CHECK sentido IN ('entrada','salida')
    """
    if df_det is None or df_det.empty:
        return None

    df = df_det.copy()

    # DNI puede venir en mayúscula
    if "DNI" in df.columns and "dni" not in df.columns:
        df = df.rename(columns={"DNI": "dni"})
    if "dni" in df.columns:
        df["dni"] = df["dni"].astype(str).str.strip()

    # Necesitamos los 6 componentes para reconstruir el timestamp. Si faltan,
    # no hay marca válida que guardar → se omite (no se inventa nada).
    componentes = ["ano", "mes", "dia", "hora", "minutos", "segundos"]
    if not all(c in df.columns for c in componentes):
        log.warning("   ⚠️  marcas_detalle: faltan componentes de fecha/hora, se omite el lote")
        return None

    # Reconstruir timestamp desde los enteros. errors="coerce" → una fila con
    # componentes inválidos queda NaT y luego se descarta por la PK.
    ts = pd.to_datetime(dict(
        year   = pd.to_numeric(df["ano"],      errors="coerce"),
        month  = pd.to_numeric(df["mes"],      errors="coerce"),
        day    = pd.to_numeric(df["dia"],      errors="coerce"),
        hour   = pd.to_numeric(df["hora"],     errors="coerce"),
        minute = pd.to_numeric(df["minutos"],  errors="coerce"),
        second = pd.to_numeric(df["segundos"], errors="coerce"),
    ), errors="coerce")
    df["fecha"]      = ts.dt.date
    df["hora_marca"] = ts.dt.time

    # sentido: normalizar y quedarnos SOLO con valores válidos. Esto respeta el
    # CHECK de la tabla: si Buk enviara un sentido raro, esa marca se descarta
    # en vez de tumbar toda la carga.
    if "sentido" not in df.columns:
        log.warning("   ⚠️  marcas_detalle: no viene la columna 'sentido', se omite el lote")
        return None
    df["sentido"] = df["sentido"].astype(str).str.strip().str.lower()
    df = df[df["sentido"].isin(("entrada", "salida"))]

    # PK compuesta: descartar filas con PK incompleta + dedup
    pk = ["obra_id", "dni", "fecha", "hora_marca", "sentido"]
    df = df.dropna(subset=pk)
    df = df.drop_duplicates(subset=pk).reset_index(drop=True)

    if df.empty:
        return None

    cols = ["obra_id", "dni", "fecha", "hora_marca", "sentido", "origen", "dispositivo"]
    return df[[c for c in cols if c in df.columns]].copy()


def transformar_inasistencias(df_inasist):
    """Normaliza inasistencias al esquema Postgres (columnas en minúscula)."""
    if df_inasist is None or df_inasist.empty:
        return None

    df = df_inasist.copy()

    # DNI puede venir con distinto case
    if "DNI" in df.columns and "dni" not in df.columns:
        df = df.rename(columns={"DNI": "dni"})

    # Deduplicar por PK compuesta + descartar filas con PK incompleta (NOT NULL)
    pk = ["obra_id", "dni", "ano", "mes", "dia"]
    if all(c in df.columns for c in pk):
        df = df.dropna(subset=pk)
        df = df.drop_duplicates(subset=pk).reset_index(drop=True)

    cols_esperadas = ["obra_id", "dni", "ano", "mes", "dia", "motivo"]
    cols_presentes = [c for c in cols_esperadas if c in df.columns]
    return df[cols_presentes].copy()


# ─── Horas extras ───────────────────────────────────────────────────────
def meses_a_procesar(modo_backfill):
    """Lista de (ano, mes) a refrescar para horas extras.
    - Incremental: mes en curso + mes anterior (capta aprobaciones tardías).
    - Backfill: desde julio-2026 (inicio de datos confiables) hasta hoy.
    """
    hoy = datetime.now()
    ano_actual, mes_actual = hoy.year, hoy.month

    if modo_backfill:
        a, m = 2026, 7  # julio 2026: inicio de datos confiables
        meses = []
        while (a, m) <= (ano_actual, mes_actual):
            meses.append((a, m))
            m += 1
            if m > 12:
                m, a = 1, a + 1
        return meses

    # Incremental: mes anterior + mes actual
    anterior = (ano_actual - 1, 12) if mes_actual == 1 else (ano_actual, mes_actual - 1)
    return [anterior, (ano_actual, mes_actual)]


def get_horas_extras_mes(obra_id, ano, mes):
    """Trae el acumulado de horas extras de un recinto para un MES COMPLETO.
    El endpoint devuelve el acumulado del rango; por eso se pide el mes entero
    (día 1 → último día), nunca ventanas parciales.
    """
    ultimo_dia = calendar.monthrange(ano, mes)[1]
    from_str = f"01-{mes:02d}-{ano}"
    to_str   = f"{ultimo_dia:02d}-{mes:02d}-{ano}"

    log.info(f"      🗓️  Horas extras {from_str} → {to_str} (obra_id={obra_id})")
    params = {"obra_id": obra_id, "from": from_str, "to": to_str}
    df = get_asist_paginated("obtenerHorasExtras", params)

    if df is None or df.empty:
        return None

    df = df.copy()
    df["ano"] = ano
    df["mes"] = mes
    return df


def transformar_horas_extras(df_he):
    """Normaliza las horas extras al esquema Postgres (tabla horas_extras)."""
    if df_he is None or df_he.empty:
        return None

    df = df_he.copy()
    renombrar = {
        "DNI": "dni",
        "Horas Extras 50%": "he_50",
        "Horas Extras 100%": "he_100",
        "total_horas_extras": "total_he",
    }
    df = df.rename(columns={k: v for k, v in renombrar.items() if k in df.columns})

    if "dni" in df.columns:
        df["dni"] = df["dni"].astype(str).str.strip()

    for col in ("he_50", "he_100", "total_he"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            df[col] = 0

    # PK (dni, ano, mes): descartar filas con PK incompleta + dedup
    pk = ["dni", "ano", "mes"]
    if all(c in df.columns for c in pk):
        df = df.dropna(subset=pk)
        df = df.drop_duplicates(subset=pk).reset_index(drop=True)

    cols = ["obra_id", "dni", "ano", "mes", "he_50", "he_100", "total_he"]
    return df[[c for c in cols if c in df.columns]].copy()


# ═══════════════════════════════════════════════════════════════════════
# 5. sync_log (observabilidad)
# ═══════════════════════════════════════════════════════════════════════
def iniciar_sync_log(conn, modo, dias_ventana):
    """Crea el registro inicial de la corrida y retorna su id."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sync_log (modo, dias_ventana, estado)
            VALUES (%s, %s, 'en_curso')
            RETURNING id;
        """, (modo, dias_ventana))
        sync_id = cur.fetchone()[0]
    conn.commit()
    return sync_id


def cerrar_sync_log(conn, sync_id, resumen, estado="ok", error=None):
    """Actualiza el sync_log al final de la corrida."""
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE sync_log SET
                corrida_fin = NOW(),
                empleados_filas = %s,
                recintos_filas = %s,
                marcas_nuevas = %s,
                marcas_actualizadas = %s,
                inasistencias_nuevas = %s,
                inasistencias_actualizadas = %s,
                estado = %s,
                mensaje_error = %s
            WHERE id = %s;
        """, (
            resumen.get("empleados", 0),
            resumen.get("recintos", 0),
            resumen.get("marcas_nuevas", 0),
            resumen.get("marcas_actualizadas", 0),
            resumen.get("inasist_nuevas", 0),
            resumen.get("inasist_actualizadas", 0),
            estado,
            error,
            sync_id
        ))
    conn.commit()


# ═══════════════════════════════════════════════════════════════════════
# 6. PROCESO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════
def main():
    log.info("=" * 65)
    log.info(f"🚀 Sincronización Buk → Supabase [{datetime.now():%Y-%m-%d %H:%M:%S}]")
    log.info(f"📂 Log: {LOG_FILE}")
    log.info("=" * 65)

    resumen = {}
    sync_id = None

    try:
        # ═══════════════════════════════════════════════════════════════
        # FASE 1 — Conexión CORTA solo para decidir modo + abrir sync_log
        # (Antes la conexión se abría aquí y se mantenía viva durante toda
        #  la extracción de Buk (~38 min), quedaba ociosa y Supabase la
        #  cerraba → 'SSL connection has been closed unexpectedly' al cargar.
        #  Ahora se abre, se usa en segundos, y se CIERRA antes de extraer.)
        # ═══════════════════════════════════════════════════════════════
        conn = conectar()
        try:
            backfill_forzado = os.getenv("BACKFILL") == "1"
            backfill_auto = tabla_vacia(conn, "asistencias_marcas") or tabla_vacia(conn, "inasistencias")
            modo_backfill = backfill_forzado or backfill_auto
            dias = config.DIAS_BACKFILL if modo_backfill else config.DIAS_INCREMENTAL
            modo = "BACKFILL" if modo_backfill else "INCREMENTAL"

            razon = ("forzado por variable de entorno BACKFILL=1" if backfill_forzado
                     else "tabla vacía detectada" if backfill_auto
                     else "corrida normal")
            log.info(f"⚙️  Modo: {modo} ({dias} días, {razon})")

            sync_id = iniciar_sync_log(conn, modo, dias)
            log.info(f"📝 sync_log id: {sync_id}")
        finally:
            conn.close()   # cerramos: no la necesitamos durante la extracción

        # ═══════════════════════════════════════════════════════════════
        # FASE 2 — Extracción de Buk (SIN conexión a Postgres abierta)
        # ═══════════════════════════════════════════════════════════════

        # ─── Buk Core ────────────────────────────────────────────
        log.info("\n📦 Buk Core (empleados y áreas)...")
        df_emp,  emp_completo  = get_core("employees")
        df_area, area_completo = get_core("areas")

        # ─── Buk Asistencia: recintos ────────────────────────────
        log.info("\n📦 Buk Asistencia — recintos...")
        df_recintos_raw = get_asist("informacionRecinto", {"page": 1, "page_size": 100})

        # ─── Buk Asistencia: marcas + inasistencias + marcas detalle ──────────
        fecha_hasta = datetime.now()
        fecha_desde = fecha_hasta - timedelta(days=dias)
        log.info(f"\n📅 Ventana de asistencia: {fecha_desde:%d-%m-%Y} → {fecha_hasta:%d-%m-%Y}")

        marcas_acumuladas, inasist_acumuladas = [], []
        marcas_detalle_acumuladas = []
        detalle_completo_global = True

        if df_recintos_raw is not None and not df_recintos_raw.empty:
            id_col     = next((c for c in ("obra_id", "obraId") if c in df_recintos_raw.columns), None)
            nombre_col = "nombre" if "nombre" in df_recintos_raw.columns else None

            if id_col:
                for _, recinto in df_recintos_raw.iterrows():
                    obra_id = recinto[id_col]
                    nombre  = recinto[nombre_col] if nombre_col else obra_id
                    log.info(f"\n📦 Recinto: {nombre} (id={obra_id})")

                    # ── Loop 1: marcas consolidadas + inasistencias ──────────
                    for d_str, h_str in chunks_fechas(fecha_desde, fecha_hasta):
                        log.info(f"   🗓️  [consolidadas] Ventana {d_str} → {h_str}")
                        params_marcas  = {"obra_id": obra_id, "desde": d_str, "hasta": h_str}
                        params_inasist = {"obra_id": obra_id, "from":  d_str, "to":    h_str}

                        df_m = get_asist_paginated("v2/asistencia-empresa", params_marcas)
                        df_i = get_asist_paginated("obtenerInasistencias", params_inasist)

                        if df_m is not None and not df_m.empty:
                            marcas_acumuladas.append(df_m)
                        if df_i is not None and not df_i.empty:
                            inasist_acumuladas.append(df_i)

                    # ── Loop 2 (AISLADO): marcas granulares ──────────────────
                    for d_str, h_str in chunks_fechas(
                            fecha_desde, fecha_hasta, dias=config.DIAS_POR_VENTANA_DETALLE):
                        log.info(f"   🗓️  [granulares] Ventana {d_str} → {h_str}")
                        params_detalle = {"obra_id": obra_id, "from": d_str, "to": h_str}

                        df_d, det_completo = get_asist_paginated_v2(
                            "obtenerRegistroAsistencia", params_detalle)

                        if df_d is not None and not df_d.empty:
                            marcas_detalle_acumuladas.append(df_d)
                        if not det_completo:
                            detalle_completo_global = False
                            log.warning(f"   ⚠️  [granulares] Ventana {d_str} → {h_str} "
                                        f"INCOMPLETA (recinto {obra_id})")

        df_marcas_raw   = pd.concat(marcas_acumuladas,         ignore_index=True) if marcas_acumuladas         else None
        df_inasist_raw  = pd.concat(inasist_acumuladas,        ignore_index=True) if inasist_acumuladas        else None
        df_detalle_raw  = pd.concat(marcas_detalle_acumuladas, ignore_index=True) if marcas_detalle_acumuladas else None

        # ─── Buk Asistencia: horas extras (por MES completo) ─────
        meses_he = meses_a_procesar(modo_backfill)
        log.info(f"\n📦 Horas extras — meses a procesar: {meses_he}")
        he_acumuladas = []
        if df_recintos_raw is not None and not df_recintos_raw.empty:
            id_col_he = next((c for c in ("obra_id", "obraId") if c in df_recintos_raw.columns), None)
            if id_col_he:
                for _, recinto in df_recintos_raw.iterrows():
                    obra_id = recinto[id_col_he]
                    for ano_he, mes_he in meses_he:
                        df_he = get_horas_extras_mes(obra_id, ano_he, mes_he)
                        if df_he is not None and not df_he.empty:
                            he_acumuladas.append(df_he)
        df_he_raw = pd.concat(he_acumuladas, ignore_index=True) if he_acumuladas else None

        # ─── Transformaciones (tampoco necesitan DB) ─────────────
        log.info("\n🔧 Transformando datos...")
        df_empleados      = transformar_empleados(df_emp, df_area)
        df_recintos       = transformar_recintos(df_recintos_raw)
        df_marcas         = transformar_marcas(df_marcas_raw)
        df_marcas_detalle = transformar_marcas_detalle(df_detalle_raw)
        df_inasist        = transformar_inasistencias(df_inasist_raw)
        df_horas_extras   = transformar_horas_extras(df_he_raw)

        # ═══════════════════════════════════════════════════════════════
        # FASE 3 — Conexión CORTA solo para cargar (dura segundos)
        # ═══════════════════════════════════════════════════════════════
        log.info("\n💾 Cargando a Supabase...")
        conn = conectar()
        try:
            if emp_completo and area_completo:
                resumen["empleados"] = refresh_completo(conn, df_empleados, "empleados")
            else:
                log.warning("   ⛔ Descarga de Buk Core INCOMPLETA → se OMITE el refresh de "
                            "'empleados'. La tabla conserva la última carga válida.")
                resumen["empleados"] = 0

            resumen["recintos"]  = refresh_completo(conn, df_recintos,  "recintos")

            n, a = upsert_df(conn, df_marcas, "asistencias_marcas", pk_cols=("id",))
            resumen["marcas_nuevas"]       = n
            resumen["marcas_actualizadas"] = a

            n, a = upsert_df(conn, df_marcas_detalle, "asistencias_marcas_detalle",
                             pk_cols=("obra_id", "dni", "fecha", "hora_marca", "sentido"))
            resumen["detalle_nuevas"]       = n
            resumen["detalle_actualizadas"] = a

            n, a = upsert_df(conn, df_inasist, "inasistencias",
                             pk_cols=("obra_id", "dni", "ano", "mes", "dia"))
            resumen["inasist_nuevas"]       = n
            resumen["inasist_actualizadas"] = a

            n, a = upsert_df(conn, df_horas_extras, "horas_extras",
                             pk_cols=("dni", "ano", "mes"))
            resumen["he_nuevas"]       = n
            resumen["he_actualizadas"] = a

            estado_final = "ok" if detalle_completo_global else "ok_con_avisos"
            cerrar_sync_log(conn, sync_id, resumen, estado=estado_final)
        finally:
            conn.close()

        log.info("\n" + "=" * 65)
        log.info("✅ PROCESO COMPLETADO OK")
        log.info(f"   Empleados:      {resumen.get('empleados', 0)}")
        log.info(f"   Recintos:       {resumen.get('recintos', 0)}")
        log.info(f"   Marcas:         {resumen.get('marcas_nuevas', 0)} nuevas, "
                 f"{resumen.get('marcas_actualizadas', 0)} actualizadas")
        log.info(f"   Marcas detalle: {resumen.get('detalle_nuevas', 0)} nuevas, "
                 f"{resumen.get('detalle_actualizadas', 0)} actualizadas")
        log.info(f"   Inasistencias:  {resumen.get('inasist_nuevas', 0)} nuevas, "
                 f"{resumen.get('inasist_actualizadas', 0)} actualizadas")
        log.info(f"   Horas extras:   {resumen.get('he_nuevas', 0)} nuevas, "
                 f"{resumen.get('he_actualizadas', 0)} actualizadas")
        if not detalle_completo_global:
            log.warning("   " + "─" * 55)
            log.warning("   ⚠️  ATENCIÓN: al menos UNA ventana de marcas granulares")
            log.warning("       quedó INCOMPLETA (error de red o tope MAX_PAGES).")
            log.warning("       El histórico de asistencias_marcas_detalle puede tener")
            log.warning("       huecos. Vuelve a correr el BACKFILL para rellenarlas")
            log.warning("       (el UPSERT es idempotente, no duplica).")
        log.info("=" * 65)

    except Exception as e:
        log.exception(f"❌ Error en la corrida: {e}")
        # Intento best-effort de marcar el sync_log como error, con conexión NUEVA
        # (la que falló pudo quedar muerta). Si tampoco se puede, se ignora.
        if sync_id:
            try:
                conn_err = conectar()
                try:
                    cerrar_sync_log(conn_err, sync_id, resumen, estado="error", error=str(e))
                finally:
                    conn_err.close()
            except Exception:
                pass
        sys.exit(1)

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
