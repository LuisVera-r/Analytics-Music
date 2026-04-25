import duckdb
import random
from faker import Faker
from datetime import datetime, timedelta, date

fake = Faker()
random.seed(42)  

# ── 1. CREACIÓN DE ESQUEMAS ──────────────────────────────────
def create_schemas(conn):
    """Lee los archivos SQL y crea las tablas en DuckDB."""
    tablas = ['dim_date', 'dim_artist', 'dim_track', 'dim_user', 'fact_streams']
    
    print("⏳ Creando esquemas desde SQL...")
    for tabla in tablas:
        # Asegúrate de que las rutas a los .sql sean correctas desde donde ejecutas el script
        with open(f'sql/schema/{tabla}.sql', 'r') as f:
            conn.execute(f.read())
        print(f"  ✓ Tabla {tabla} creada.")

# ── 2. GENERACIÓN DE DATOS EN DIMENSIONES ─────────────────────────────
def generate_dim_date(conn):
    """Genera e inserta la dimensión de fechas."""
    dates = []
    start = date(2022, 1, 1)
    for i in range(365 * 4):
        d = start + timedelta(days=i)
        dates.append((
            int(d.strftime('%Y%m%d')), d, d.day, d.month, d.strftime('%B'),
            (d.month - 1) // 3 + 1, d.year, d.isocalendar()[1],
            d.strftime('%A'), d.weekday() >= 5, False
        ))
    
    conn.executemany("INSERT INTO dim_date VALUES (?,?,?,?,?,?,?,?,?,?,?)", dates)
    print(f"✅ dim_date: {len(dates)} registros insertados.")

def generate_dim_artist(conn):
    """Genera e inserta artistas. Devuelve la lista para usarla en tracks."""
    genres = ['Pop', 'Rock', 'Hip-Hop', 'Electronic', 'Jazz', 'Classical', 'Reggaeton', 'R&B', 'Latin', 'Metal', 'Indie']
    countries = ['MX', 'US', 'CO', 'AR', 'BR', 'ES', 'UK', 'KR', 'NG', 'CA']
    
    artists = [(i, f'ART-{i:04d}', fake.name(), random.choice(countries), random.choice(genres)) 
               for i in range(1, 1001)]
    
    conn.executemany("INSERT INTO dim_artist VALUES (?,?,?,?,?)", artists)
    print(f"✅ dim_artist: {len(artists)} registros insertados.")
    return artists

def generate_dim_track(conn, artists):
    """Genera e inserta tracks basándose en los artistas generados."""
    tracks = []
    for i in range(1, 10001):
        artist = random.choice(artists)
        tracks.append((
            i, f'TRK-{i:05d}', fake.catch_phrase(), artist[0], artist[4],
            random.randint(120, 360), fake.bs().title(),
            fake.date_between(start_date=date(2022, 1, 1), end_date=date(2025, 12, 31))
        ))
    
    conn.executemany("INSERT INTO dim_track VALUES (?,?,?,?,?,?,?,?)", tracks)
    print(f"✅ dim_track: {len(tracks)} registros insertados.")
    return tracks

def generate_dim_user(conn):
    """Genera e inserta usuarios aplicando lógica SCD-2."""
    regions = ['CDMX', 'GDL', 'MTY', 'NYC', 'LAX', 'Miami', 'Bogotá', 'Lima', 'Madrid', 'Buenos Aires', 'Liverpool', 'Manchester']
    countries = ['MX', 'US', 'CO', 'AR', 'BR', 'ES', 'UK'] # Añadido para que no de error
    
    users = []
    sk = 1
    # ... (Aquí va tu bloque FOR idéntico de 5000 usuarios y los 300 cambios SCD-2) ...
    # Por brevedad en la respuesta, asume que tu lógica está aquí pegada.
    
    # conn.executemany("INSERT INTO dim_user VALUES (?,?,?,?,?,?,?,?,?,?,?)", users)
    print(f"✅ dim_user: {len(users)} registros insertados (incluye histórico).")
    return users

# ── 3. GENERACIÓN DE HECHOS ──────────────────────────────────
def generate_fact_streams(conn, users, tracks):
    """Genera e inserta los streams en lotes."""
    devices = ['mobile', 'desktop', 'tablet', 'smart_tv']
    batch = []
    limite_global = date(2025, 12, 31)
    
    print("⏳ Generando 1,000,000 de streams en lotes...")
    for i in range(1, 1_000_001):
        # ... (Tu lógica de generación de streams va aquí) ...
        
        # Insertar en lotes de 50,000 para no saturar la memoria RAM
        if i % 50_000 == 0:
            # conn.executemany("INSERT INTO fact_streams VALUES (?,?,?,?,?,?,?,?)", batch)
            batch = []
            print(f"  → {i:,} streams insertados...")
            
    # Insertar el remanente si quedó algo en el batch
    if batch:
        # conn.executemany("INSERT INTO fact_streams VALUES (?,?,?,?,?,?,?,?)", batch)
        pass
        
    print("✅ fact_streams: Carga completada.")

# ── 4. FUNCIÓN PRINCIPAL (ORQUESTADOR) ───────────────────────
def main():
    print("🚀 Iniciando pipeline de datos MusicAnalytics...")
    
    # 1. Crear conexión
    conn = duckdb.connect('musicflow.duckdb')
    
    try:
        # 2. Ejecutar DDLs
        create_schemas(conn)
        
        # 3. Cargar Dimensiones independientes
        generate_dim_date(conn)
        artists = generate_dim_artist(conn)
        users = generate_dim_user(conn)
        
        # 4. Cargar Dimensiones dependientes (Tracks necesita Artistas)
        tracks = generate_dim_track(conn, artists)
        
        # 5. Cargar Tabla de Hechos (Necesita Usuarios y Tracks)
        generate_fact_streams(conn, users, tracks)
        
        print("\n🎉 ¡Data Warehouse generado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error durante la carga: {e}")
    finally:
        # Siempre cerrar la conexión, incluso si hay error
        conn.close()

if __name__ == "__main__":
    main()
