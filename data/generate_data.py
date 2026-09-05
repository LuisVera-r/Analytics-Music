from pathlib import Path
import duckdb 
import traceback
from data.generators.dim_artist_generator import generate_dim_artist
from data.generators.dim_date_generator import  generate_dim_date
from data.generators.dim_user_generator import generate_dim_user
from data.generators.dim_track_generator import generate_dim_track
from data.generators.fact_generator import generate_fact_streams

TABLES = (
     "dim_date",
    "dim_artist",
    "dim_track",
    "dim_user",
    "fact_streams",
    "stg_streams"
)

def create_tables(conn: duckdb.DuckDBPyConnection) -> None: 
        BASE_DIR = Path(__file__).resolve().parent.parent

        print("creando tablas...")

        for table in TABLES:
            path = BASE_DIR / "sql" / "schema" / f"{table}.sql"

            if not path.exists():
                print(f"Advertencia: No se encontró {path}")
                continue

            conn.execute(path.read_text())


def main() -> None:
    
    BASE_DIR = Path(__file__).resolve().parent.parent

    db_path = BASE_DIR/'storage'/'musicflow.duckdb'
    
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with duckdb.connect(db_path) as conn:
            print(" INICIANDO PIPELINE MUSICFLOW ANALYTICS\n" + "="*40)
        
            create_tables(conn)

            generate_dim_date(conn)

            users = generate_dim_user(conn)
            artists = generate_dim_artist(conn)
            tracks = generate_dim_track(conn, artists)

            generate_fact_streams(
                conn, 
                users, 
                tracks
            )
            
            print("="*40 + "\n DATA WAREHOUSE LISTO")
        
    except Exception as e:
        print(f"Pipeline falló: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
