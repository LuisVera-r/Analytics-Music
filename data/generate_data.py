import sys
import os
from pathlib import Path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import duckdb 
import traceback
from generators.dim_artist_generator import generate_dim_artist
from generators.dim_date_generator import  generate_dim_date
from generators.dim_user_generator import generate_dim_user
from generators.dim_track_generator import generate_dim_track
from generators.fact_generator import generate_fact_streams

def create_schemas(conn):
    
    tables = ['dim_date', 'dim_artist', 'dim_track', 'dim_user', 'fact_streams']
    print(" Creando tablas...")
    for table in tables:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(os.path.dirname(
        BASE_DIR),
        'sql',
        'schema',
        f'{table}.sql'
        )
        if os.path.exists(path):
            with open(path, 'r') as f:
                conn.execute(f.read())
        else:
            print(f"  Advertencia: No se encontró {path}")

def main():
    
    BASE_DIR = Path(__file__).resolve().parent.parent

    db_path = BASE_DIR/'storage'/'musicflow.duckdb'
    
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
        traceback.print_exc()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
