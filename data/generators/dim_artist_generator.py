import random
from faker import Faker

def generate_dim_artist(conn):
  genres = ['Pop', 'Rock', 'Hip-Hop', 'Electronic', 'Jazz',
          'Classical', 'Reggaeton', 'R&B', 'Latin', 'Metal','Indie']
  countries = ['MX', 'US', 'CO', 'AR', 'BR', 'ES', 'UK', 'KR', 'NG', 'CA']

  artists = [(i, f'ART-{i:04d}', fake.name(),
            random.choice(countries), random.choice(genres))
             for i in range(1, 1001)]

  conn.executemany(
    "INSERT INTO dim_artist VALUES (?,?,?,?,?)", artists)

  print(f" dim_artist: {len(artists)} artistas cargados")
  return artists
  

