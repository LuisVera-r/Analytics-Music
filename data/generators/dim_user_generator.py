import random
from faker import Faker
from datetime import date, timedelta

fake = Faker()
def generate_dim_user(conn):
  regions = ['CDMX', 'GDL', 'MTY', 'NYC', 'LAX', 'Miami',
           'Bogotá', 'Lima', 'Madrid', 'Buenos Aires','Liverpool','Manchester']
  
  # Simulacion de 5,000 usuarios
  users = []
  sk = 1
  for i in range(1, 5001):
      signup = fake.date_between(start_date=date(2022, 1, 1), end_date=date(2025, 12, 31))
      users.append((
          sk, f'USR-{i:04d}', fake.name(),
          random.choice(countries), random.choice(regions),
          random.randint(15, 55),
          random.choice(['M', 'F', 'NB']),
          signup,
          signup,          # valid_from = signup_date
          date(9999,12,31),
          True
      ))
      sk += 1
  
  # Simula 300 cambios de región (SCD-2 en acción)
  for _ in range(300):
      index = random.randint(0, 4999)
      old_user = users[index]
  
      fecha_version_vieja = old_user[8]
      fecha_minima = fecha_version_vieja + timedelta(days=1)
      if fecha_minima > date(2025, 12, 31):
          continue
      change_date = fake.date_between(start_date=fecha_minima, end_date=date(2025, 12, 31))
  
      old_user_list = list(old_user)
      old_user_list[9] = change_date
      old_user_list[10] = False
      users[index] = tuple(old_user_list) # Guardamos el cambio en la lista principal
  
  
      user_id = old_user[1]
  
      # Nueva versión del usuario con región diferente
      new_region = random.choice([r for r in regions if r != old_user[4]])
      users.append((
          sk, user_id, old_user[2],
          old_user[3], new_region, old_user[5],
          old_user[6], old_user[7],
          change_date, date(9999,12,31), True
      ))
      sk += 1
  
  conn.executemany("""
  INSERT INTO dim_user VALUES (?,?,?,?,?,?,?,?,?,?,?)
  """, users)
  
  print(f" dim_user: {len(users)} registros ({sk-5001} cambios SCD-2)")
  return users
