
import psycopg2

def crear_usuario_y_bd(admin_user="postgres", admin_pass="admin"):
    conn = psycopg2.connect(
        dbname="postgres",
        user=admin_user,
        password=admin_pass,
        host="localhost"
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Verificar si el usuario ya existe
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname='etl_user'")
    if not cur.fetchone():
        cur.execute("CREATE USER etl_user WITH PASSWORD 'etl_pass'")
        print("Usuario 'etl_user' creado.")
    else:
        print("El usuario 'etl_user' ya existe.")

    # Verificar si la base de datos ya existe
    cur.execute("SELECT 1 FROM pg_database WHERE datname='spotify_analysis'")
    if not cur.fetchone():
        cur.execute("CREATE DATABASE spotify_analysis OWNER etl_user")
        print("Base de datos 'spotify_analysis' creada.")
    else:
        print("La base de datos 'spotify_analysis' ya existe.")

    cur.close()
    conn.close()

def conectar_etl():
    return psycopg2.connect(
        dbname="spotify_analysis",
        user="etl_user",
        password="etl_pass",
        host="localhost"
    )

def crear_tabla_grammy_awards(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS grammy_awards (
            id SERIAL PRIMARY KEY,
            ceremony_year INT,
            category TEXT,
            nominee TEXT,
            artist TEXT,
            work TEXT,
            winner BOOLEAN
        );
    """)
    conn.commit()
    cur.close()
    print("Tabla 'grammy_awards' verificada/creada.")

def insertar_datos_grammy_awards(conn, df):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM grammy_awards")
    if cur.fetchone()[0] == 0:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO grammy_awards (ceremony_year, category, nominee, artist, work, winner)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row['ceremony_year'],
                row['category'],
                row['nominee'],
                row['artist'],
                row['work'],
                str(row['winner']).strip().lower() in ['true', '1', 'yes']
            ))
        conn.commit()
        print("Datos insertados en 'grammy_awards'")
    else:
        print("Los datos ya están insertados. No se duplicaron registros.")
    cur.close()
