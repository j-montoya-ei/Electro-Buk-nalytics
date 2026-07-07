"""
Test de conexión a Supabase.
Solo verifica que:
1. El .env se lee correctamente
2. La conexión a PostgreSQL funciona
3. Podemos leer las tablas creadas
"""
import os
from dotenv import load_dotenv
import psycopg2

# Cargar el .env
load_dotenv()

DB_URL = os.getenv("SUPABASE_DB_URL")

if not DB_URL:
    print("❌ ERROR: No se encontró SUPABASE_DB_URL en el archivo .env")
    exit(1)

# Ocultar la contraseña al mostrar en pantalla (por si haces screenshot)
url_visible = DB_URL.split("@")[1] if "@" in DB_URL else "???"
print(f"🔗 Intentando conectar a: {url_visible}")

try:
    # Abrir conexión
    conn = psycopg2.connect(DB_URL)
    cur = conn.cursor()

    # Prueba 1: versión de PostgreSQL
    cur.execute("SELECT version();")
    version = cur.fetchone()[0]
    print(f"\n✅ Conexión exitosa")
    print(f"📊 Versión PostgreSQL: {version[:60]}...")

    # Prueba 2: listar las tablas de nuestro esquema
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """)
    tablas = [r[0] for r in cur.fetchall()]
    print(f"\n📋 Tablas encontradas ({len(tablas)}):")
    for t in tablas:
        cur.execute(f'SELECT COUNT(*) FROM "{t}";')
        n = cur.fetchone()[0]
        print(f"   - {t}: {n} filas")

    cur.close()
    conn.close()
    print("\n🎯 Todo OK. Ya podemos migrar el ETL.")

except Exception as e:
    print(f"\n❌ ERROR de conexión: {e}")
    print("\n💡 Revisa que:")
    print("   1. La contraseña en el .env sea correcta")
    print("   2. La cadena SUPABASE_DB_URL esté completa")
    print("   3. Tu PC tenga internet")