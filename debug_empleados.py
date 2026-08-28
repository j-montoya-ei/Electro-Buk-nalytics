"""
debug_empleados.py  ──  SCRIPT TEMPORAL DE DIAGNÓSTICO
───────────────────────────────────────────────────────────────────────
Objetivo: ver la forma CRUDA de la respuesta de Buk Core `employees` para
confirmar las llaves EXACTAS de los atributos que pide el módulo de
caracterización (#8) y que hoy el ETL NO extrae:
  - Escolaridad (nivel educativo)
  - Estrato Socioeconómico
  - Tipo de contrato
  - Grupo familiar / cargas (para el pendiente de "número de hijos")
  - Clasificación (Administrativo/Operativo) — bonus para segmentar

NO toca Supabase. Solo lee de Buk e imprime al log de la corrida.

Reutiliza get_core() de main_v2 (la MISMA autenticación y paginación ya
probadas). El guard `if __name__ == "__main__"` de main_v2 evita que el
import dispare el ETL.

Uso: se corre desde el workflow debug_empleados.yml (workflow_dispatch).
La salida sale directo en la consola de la corrida (no hay que bajar artifacts).

⚠️  BORRAR este archivo y debug_empleados.yml después de leer el log.
"""
import json
import main_v2  # trae auth + helpers ya probados; NO corre main() por el guard

log = main_v2.log


def preview(obj, max_len=2000):
    """Serializa a JSON legible; recorta por seguridad para no llenar el log."""
    try:
        s = json.dumps(obj, indent=2, ensure_ascii=False, default=str)
    except Exception:
        s = str(obj)
    if len(s) > max_len:
        s = s[:max_len] + f"\n... [recortado a {max_len} caracteres]"
    return s


def main():
    log.info("=" * 65)
    log.info("🔍 DEBUG employees — inspección de respuesta cruda de Buk Core")
    log.info("=" * 65)

    df_emp, completo = main_v2.get_core("employees")

    if df_emp is None or df_emp.empty:
        log.error("❌ No llegaron empleados (df vacío). Revisar BUK_TOKEN / BASE_URL.")
        return

    log.info(f"✅ Empleados recibidos: {len(df_emp)} | descarga completa: {completo}")

    # ── (1) Llaves de nivel colaborador (top-level) ──────────────────────
    log.info("\n── (1) LLAVES DE NIVEL COLABORADOR ──")
    log.info(preview(list(df_emp.columns)))

    # Elegir el primer empleado que tenga current_job como dict (defensivo)
    idx = 0
    for i in range(len(df_emp)):
        if isinstance(df_emp.iloc[i].get("current_job"), dict):
            idx = i
            break
    emp0 = df_emp.iloc[idx].to_dict()

    # ── (2) current_job completo (aquí viven los custom_attributes) ──────
    cj = emp0.get("current_job")
    log.info("\n── (2) current_job DEL PRIMER EMPLEADO ──")
    log.info(preview(cj, max_len=4000))

    # ── (3) custom_attributes por separado: LAS LLAVES QUE NECESITO ──────
    log.info("\n── (3) current_job.custom_attributes ──")
    ca = cj.get("custom_attributes") if isinstance(cj, dict) else None
    if isinstance(ca, dict):
        log.info("Llaves exactas: " + preview(list(ca.keys())))
        log.info("Contenido:\n" + preview(ca, max_len=3000))
    else:
        log.info(f"custom_attributes no es dict → tipo {type(ca)}: {preview(ca)}")

    # ── (4) Empleado completo (para ubicar tipo de contrato y grupo ──────
    #        familiar, que pueden NO estar dentro de current_job) ─────────
    log.info("\n── (4) PRIMER EMPLEADO COMPLETO (recortado) ──")
    log.info("Nota: puedes tachar nombre/cédula antes de pegarlo; lo que importa "
             "son las LLAVES, no los valores personales.")
    log.info(preview(emp0, max_len=6000))

    log.info("\n" + "=" * 65)
    log.info("🔍 FIN DEBUG. Copia los bloques (1)–(4) y bórrame junto al workflow.")
    log.info("=" * 65)


if __name__ == "__main__":
    main()
