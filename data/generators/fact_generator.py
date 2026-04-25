import random
from faker import Faker
from datetime import date

fake = Faker()
def generate_fact_streams(conn, users, tracks):
  devices = ['mobile', 'desktop', 'tablet', 'smart_tv']
  batch = []
  limite_global = date(2025, 12, 31)
  
  for i in range(1, 1_000_001):
      user = random.choice(users)
      user_sk = user[0]
      valid_from = user[8]
      valid_to = user[9]
  
      # Topamos la fecha máxima al 2025 para no pasarnos de nuestra dim_date
      fecha_maxima = min(valid_to, limite_global)
  
      # Si por alguna rareza de los datos basura valid_from se pasa, lo ajustamos
      valid_from = min(valid_from, fecha_maxima)
  
      stream_date = fake.date_between(start_date=valid_from, end_date=fecha_maxima)
  
      # Convertimos la fecha (2023-05-10) al formato numérico date_sk (20230510)
      stream_date_sk = int(stream_date.strftime('%Y%m%d'))
  
      track = random.choice(tracks)
      duration = track[5]
  
      batch.append((
          i,                                 # stream_sk
          user[0],                           # user_sk
          track[0],                          # track_sk
          stream_date_sk,                    # date_sk
          random.randint(30, duration),      # listened_seconds
          1,                                 # play_count
          random.random() < 0.25,            # 25% skipped
          random.choice(devices)
      ))
  
      # Insertar en lotes de 50,000 para no saturar memoria
      if i % 50_000 == 0:
          conn.executemany(
              "INSERT INTO fact_streams VALUES (?,?,?,?,?,?,?,?)", batch)
          batch = []
          print(f"  → {i:,} streams insertados...")
  
  if batch:
      conn.executemany(
          "INSERT INTO fact_streams VALUES (?,?,?,?,?,?,?,?)", batch)

print(" fact_streams: 1,000,000 streams cargados")
