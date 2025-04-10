
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

def crear_tabla_spotify_data(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS spotify_data (
            id SERIAL PRIMARY KEY,
            artists TEXT,
            album_name TEXT,
            track_name TEXT,
            popularity INTEGER,
            duration_ms INTEGER,
            explicit BOOLEAN,
            danceability FLOAT,
            energy FLOAT,
            key INTEGER,
            loudness FLOAT,
            mode INTEGER,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT,
            time_signature INTEGER,
            track_genre TEXT
        );
    """)
    conn.commit()
    cur.close()
    print("Tabla 'spotify_data' verificada/creada.")


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

def insertar_datos_spotify_data(conn,df):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM spotify_data")
    if cur.fetchone()[0] == 0:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO spotify_data (
                    artists, album_name, track_name, popularity,
                    duration_ms, explicit, danceability, energy, key,
                    loudness, mode, speechiness, acousticness, instrumentalness,
                    liveness, valence, tempo, time_signature, track_genre
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                          %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['artists'],
                row['album_name'],
                row['track_name'],
                int(row['popularity']),
                int(row['duration_ms']),
                bool(row['explicit']),
                float(row['danceability']),
                float(row['energy']),
                int(row['key']),
                float(row['loudness']),
                int(row['mode']),
                float(row['speechiness']),
                float(row['acousticness']),
                float(row['instrumentalness']),
                float(row['liveness']),
                float(row['valence']),
                float(row['tempo']),
                int(row['time_signature']),
                row['track_genre']
            ))
        conn.commit()
        print("Datos insertados en 'spotify_data'.")
    else:
        print("Los datos ya están insertados. No se duplicaron registros.")
    cur.close()