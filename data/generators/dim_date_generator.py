from datetime import datetime, timedelta, date

def generate_dim_user(conn):
  
  dates = []
  start = date(2022, 1, 1)
  for i in range(365 * 4):  # 4 años de fechas
      d = start + timedelta(days=i)
      dates.append({
          'date_sk'    : int(d.strftime('%Y%m%d')), #Convierte la fecha a un número sin guiones
          'full_date'  : d,
          'day'        : d.day,
          'month'      : d.month,
          'month_name' : d.strftime('%B'),  #Saca el nombre del mes en texto
          'quarter'    : (d.month - 1) // 3 + 1,
          'year'       : d.year,
          'week'       : d.isocalendar()[1], #calculando en qué semana del año cae la fecha.
          'day_of_week': d.strftime('%A'),  #obtiene el nombre del día.
          'is_weekend' : d.weekday() >= 5,
          'is_holiday' : False
      })
  
  conn.executemany("""
  INSERT INTO dim_date VALUES (
      ?, ?, ?, ?, ?,
      ?, ?, ?, ?, ?, ?
  )
  """, [list(d.values()) for d in dates]) #List Comprehension
  
  print(f" dim_date: {len(dates)} fechas cargadas")
  
