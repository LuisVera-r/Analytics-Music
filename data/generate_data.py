import duckdb
import os
from generators.date_generator import generate_dim_date
from generators.user_generator import generate_dim_user
from generators.music_generator import generate_music_data
from generators.fact_generator import generate_fact_streams

def create_schemas(conn):
    
    tables = ['dim_date', 'dim_artist', 'dim_track', 'dim_user', 'fact_streams']
    print(" Creando tablas...")
    for table in tables:
        path = f'sql/schema/{table}.sql'
        if os.path.exists(path):
            with open(path, 'r') as f:
                conn.execute(f.read())
        else:
            print(f"  Advertencia: No se encontró {path}")

def main():
    
    db_path = 'musicflow.duckdb'
    conn = duckdb.connect(db_path)
    
    try:
        print(" INICIANDO PIPELINE MUSICFLOW ANALYTICS\n" + "="*40)
        
        create_schemas(conn)
        generate_dim_date(conn)
        users = generate_dim_user(conn)
        artists = generate_dim_artist(conn)
        tracks = generate_dim_track(conn, artists)
        generate_fact_streams(conn, users, tracks)
        
        print("="*40 + "\n DATA WAREHOUSE LISTO")
        
    except Exception as e:
        print(f" ERROR CRÍTICO: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
