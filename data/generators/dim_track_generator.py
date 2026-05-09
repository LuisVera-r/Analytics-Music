import random
from faker import Faker
from datetime import date
  
fake = Faker()
def generate_dim_track(conn,artists):
  tracks = []
  for i in range(1, 10001):
      artist = random.choice(artists)
      tracks.append((
          i, f'TRK-{i:05d}', fake.catch_phrase(),
          artist[0],           # artist_sk
          artist[4],           # mismo género del artista
          random.randint(120, 360),
          fake.bs().title(),
          fake.date_between(start_date=date(2022, 1, 1), end_date=date(2025, 12, 31))
      ))
  conn.executemany(
      "INSERT INTO dim_track VALUES (?,?,?,?,?,?,?,?)", tracks)
  
  print(f" dim_track: {len(tracks)} tracks cargados")
  return tracks
  
