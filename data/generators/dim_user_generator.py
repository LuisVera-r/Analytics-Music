import random
from datetime import date, timedelta

from faker import Faker

fake = Faker()

regions = ('CDMX', 'GDL', 'MTY', 'NYC', 'LAX', 'Miami',
           'Bogotá', 'Lima', 'Madrid', 'Buenos Aires','Liverpool','Manchester'
)

countries = (
        'MX', 'US', 'CO', 'AR',
        'BR', 'ES', 'UK', 'KR',
        'NG', 'CA'
)

NUM_USERS = 5000
NUM_CHANGES = 300

START_DATE = date(2022, 1, 1)
END_DATE = date(2025, 12, 31)
MAX_DATE = date(9999, 12, 31)

def generate_dim_user(conn):
    users = []

    for user_sk in range(1, NUM_USERS + 1):
        signup = fake.date_between(
            start_date=START_DATE,
            end_date=END_DATE
        )

        users.append((
            user_sk,
            f"USR-{user_sk:04d}",
            fake.name(),
            random.choice(countries),
            random.choice(regions),
            random.randint(15, 55),
            random.choice(("M", "F", "NB")),
            signup,
            signup,
            MAX_DATE,
            True
        ))
        
    next_user_sk = NUM_USERS + 1
    changes_created = 0

  
  # Simula 300 cambios de región (SCD-2 en acción)
    for _ in range(NUM_CHANGES):
        index = random.randrange(NUM_USERS)
        old_user = users[index]

        min_change_date = old_user[8] + timedelta(days=1)

        if min_change_date > END_DATE:
            continue

        change_date = fake.date_between(
            start_date=min_change_date,
            end_date=END_DATE
        )

        # Cerrar vigencia del registro anterior
        old_user = list(old_user)
        old_user[9] = change_date
        old_user[10] = False
        users[index] = tuple(old_user)

  
      # Nueva versión del usuario con región diferente
        new_region = random.choice([r for r in regions if r != old_user[4]])
        users.append((
            next_user_sk, old_user[1], old_user[2],
            old_user[3], new_region, old_user[5],
            old_user[6], old_user[7],
            change_date, MAX_DATE, True
        ))
        next_user_sk += 1
        changes_created += 1
  
    conn.executemany("""
        INSERT INTO dim_user (
        user_sk,
        user_id,
        name,
        country,
        region,
        age,
        gender,
        signup_date,
        valid_from,
        valid_to,
        is_current) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, users)
        
    print(
    f"dim_user: {len(users)} registros "
    f"({changes_created} cambios SCD Tipo 2)"
    )
    return users
