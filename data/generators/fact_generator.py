import random
from datetime import date, datetime

from src.config import BATCH_SIZE

from faker import Faker

fake = Faker()

STREAMS_PER_WEEK = 50_000
TOTAL_STREAMS = 250_000
LIMIT_DATE = date(2025, 12, 31)

DEVICES = (
    "mobile",
    "desktop",
    "tablet",
    "smart_tv",
)

def generate_fact_streams(conn, users, tracks):
    batch = []
     
    for stream_sk in range(1, TOTAL_STREAMS + 1):
        user = random.choice(users)

        valid_from = user[8]
        valid_to = user[9]
  
         # Evita generar fechas fuera del rango de dim_date
        fecha_maxima = min(valid_to, LIMIT_DATE)
        valid_from = min(valid_from, fecha_maxima)
  
        stream_date = fake.date_between(
            start_date=valid_from,
            end_date=fecha_maxima
        )
  
        # Convertimos la fecha (2023-05-10) al formato numérico date_sk (20230510)
        stream_date_sk = int(stream_date.strftime('%Y%m%d'))
  
        track = random.choice(tracks)
        duration = track[5]
  
        batch.append((
            stream_sk,                                 # stream_sk
            user[0],                           # user_sk
            track[0],                          # track_sk
            stream_date_sk,                    # date_sk
            random.randint(30, duration),      # listened_seconds
            1,                                 # play_count
            random.random() < 0.25,            # 25% skipped
            random.choice(DEVICES)
        ))
  
        # Insertar en lotes de 50,000 para no saturar memoria
        if stream_sk % BATCH_SIZE == 0:
            conn.executemany(
                """
                INSERT INTO fact_streams (
                    stream_sk,
                    user_sk,
                    track_sk,
                    date_sk,
                    listened_seconds,
                    play_count,
                    was_skipped,
                    device_type
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch,
            )
            print(f"→ {stream_sk:,} streams insertados...")
            batch.clear()
  
        # Insertar los registros restantes
    if batch:
        conn.executemany(
            """
            INSERT INTO fact_streams (
                stream_sk,
                user_sk,
                track_sk,
                date_sk,
                listened_seconds,
                play_count,
                was_skipped,
                device_type
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch,
        )

    print(f"fact_streams: {TOTAL_STREAMS:,} streams cargados")

def generate_incremental_streams(users, tracks, start_date, end_date, last_stream_sk, num_streams):
    batch = []

    # Airflow entrega Pendulum DateTime.
    # Lo convertimos a datetime si es necesario.
    if not isinstance(start_date, datetime):
        start_date = datetime.fromisoformat(str(start_date))

    if not isinstance(end_date, datetime):
        end_date = datetime.fromisoformat(str(end_date))

    # La ejecución representa UNA hora.
    # Como el DW solo almacena date_sk,
    # usamos el día correspondiente al intervalo.
    stream_date = start_date.date()
    stream_date_sk = int(stream_date.strftime("%Y%m%d"))

    for stream_sk in range(
        last_stream_sk + 1,
        last_stream_sk + num_streams + 1,
    ):

        user = random.choice(users)
        track = random.choice(tracks)

        batch.append(
            {
                "stream_sk": stream_sk,
                "user_sk": user[0],
                "track_sk": track[0],
                "date_sk": stream_date_sk,
                "listened_seconds": random.randint(30, track[5]),
                "play_count": 1,
                "was_skipped": random.random() < 0.25,
                "device_type": random.choice(DEVICES),
            }
        )

    return batch