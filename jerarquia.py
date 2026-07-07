"""
jerarquia.py
────────────
Mapa organizacional de Electroingeniería S.A.S.
Estructura: {Sub_Area: (Area, Division)}

Cuando RR.HH. cree/modifique una sub-área, editar SOLO este archivo.
No hay que tocar main_v2.py.
"""

JERARQUIA = {
    # ─── División: Concesiones ──────────────────────────────────────
    'Atención al Usuario':             ('Gestión Alumbrado Público',      'Concesiones'),
    'Iluminación':                     ('Gestión Alumbrado Público',      'Concesiones'),
    'Compras AP':                      ('Gestión Alumbrado Público',      'Concesiones'),
    'SIG':                             ('Gestión Alumbrado Público',      'Concesiones'),
    'Logistica AP':                    ('Gestión Alumbrado Público',      'Concesiones'),
    'Medio Ambiente':                  ('Gestión Alumbrado Público',      'Concesiones'),
    'Operaciones Cesar':               ('AOM',                            'Concesiones'),
    'Operaciones Córdoba':             ('AOM',                            'Concesiones'),
    'Operaciones Palmira':             ('AOM',                            'Concesiones'),
    'Operaciones Valle - Quindio':     ('AOM',                            'Concesiones'),
    'Gerencia AP':                     ('Gerencia AP',                    'Concesiones'),

    # ─── División: Corporativa ──────────────────────────────────────
    'Auditoría':                       ('Gestión Mejora continua',        'Corporativa'),
    'Calidad':                         ('Gestión Mejora continua',        'Corporativa'),
    'Contabilidad':                    ('Gestión Contabilidad',           'Corporativa'),
    'Cumplimiento':                    ('Gestión Cumplimiento',           'Corporativa'),
    'Finanzas':                        ('Gestión Finanzas',               'Corporativa'),
    'Gerencia Administrativa':         ('Gerencia Administrativa',        'Corporativa'),
    'Gerencia General':                ('Gerencia General',               'Corporativa'),
    'Servicios Generales':             ('Gestión Administrativa',         'Corporativa'),
    'Activos Fijos':                   ('Gestión Administrativa',         'Corporativa'),
    'Gestión documental':              ('Gestión Administrativa',         'Corporativa'),
    'Sistemas':                        ('Gestión Administrativa',         'Corporativa'),
    'Mantenimiento':                   ('Gestión Administrativa',         'Corporativa'),
    'Administrativo':                  ('Gestión Administrativa',         'Corporativa'),
    'Gestión Humana':                  ('Gestión Humana',                 'Corporativa'),
    'Seguridad y Salud en el Trabajo': ('Gestión Humana',                 'Corporativa'),
    'Nómina':                          ('Gestión Humana',                 'Corporativa'),
    'Jurídico':                        ('Gestión Jurídico',               'Corporativa'),

    # ─── División: Proyectos ────────────────────────────────────────
    'Proyectos Administrativo':        ('Gestión Proyectos',              'Proyectos'),
    'Proyectos Ejecucción':            ('Gestión Proyectos',              'Proyectos'),
    'Proyectos Planeación':            ('Gestión Proyectos',              'Proyectos'),
    'Proyectos Cierre':                ('Gestión Proyectos',              'Proyectos'),

    # ─── División: Suministros ──────────────────────────────────────
    'Gerencia Suministros y Proyectos':('Gerencia Suministros y Proyectos','Suministros'),
    'Compras Suministros':             ('Gestión Suministros',            'Suministros'),
    'Logística Suministros':           ('Gestión Suministros',            'Suministros'),
    'Cartera':                         ('Gestión Suministros',            'Suministros'),
    'Ventas':                          ('Gestión Suministros',            'Suministros'),
}


def jerarquia(sub_area: str) -> tuple[str, str]:
    """Resuelve (Area, Division) para una sub-área.
    Si no está mapeada, retorna ('Otra Área', 'Otra División').
    """
    return JERARQUIA.get(str(sub_area).strip(), ("Otra Área", "Otra División"))


def homologar_sede(valor: str) -> str:
    """Reglas de negocio para unificar nombres de sede.
    BODEGA / SUMINISTROS / SAJONIA → Tuluá.
    """
    if not valor or not isinstance(valor, str):
        return "No definida"
    v = valor.upper()
    if any(k in v for k in ["BODEGA", "SUMINISTROS", "SAJONIA"]):
        return "Tuluá"
    return valor


def interpretar_horario(current_job: dict) -> str:
    """Convierte los días crudos del contrato en texto legible.
    Ejemplo: {'l','m','w','j','v'} → 'Lunes a Viernes'
    """
    if not isinstance(current_job, dict):
        return "N/A"
    days = current_job.get("days") or []
    if not days:
        return "N/A"
    dias_set = set(days)
    if "s" in dias_set and "d" in dias_set:
        return "Lunes a Domingo"
    elif "s" in dias_set:
        return "Lunes a Sábado"
    elif dias_set >= {"l", "m", "w", "j", "v"}:
        return "Lunes a Viernes"
    else:
        mapa = {"l": "Lun", "m": "Mar", "w": "Mié", "j": "Jue", "v": "Vie", "s": "Sáb", "d": "Dom"}
        return ", ".join(mapa.get(d, d) for d in days)
