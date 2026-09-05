from datetime import timedelta, date

from src.config import get_duckdb_conn



def extend_dim_date(conn):

    start = date(2026, 1, 1)
    end = date(2026, 1, 8)

    dates = []

    current_date = start

    while current_date <= end:

        d = current_date

        dates.append({
            "date_sk": int(d.strftime("%Y%m%d")),
            "full_date": d,
            "day": d.day,
            "month": d.month,
            "month_name": d.strftime("%B"),
            "quarter": (d.month - 1) // 3 + 1,
            "year": d.year,
            "week": d.isocalendar()[1],
            "day_of_week": d.strftime("%A"),
            "is_weekend": d.weekday() >= 5,
            "is_holiday": False,
        })

        current_date += timedelta(days=1)

    conn.executemany(
        """
        INSERT INTO dim_date (
            date_sk,
            full_date,
            day,
            month,
            month_name,
            quarter,
            year,
            week,
            day_of_week,
            is_weekend,
            is_holiday
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (date_sk) DO NOTHING
        """,
        [
            [
                d["date_sk"],
                d["full_date"],
                d["day"],
                d["month"],
                d["month_name"],
                d["quarter"],
                d["year"],
                d["week"],
                d["day_of_week"],
                d["is_weekend"],
                d["is_holiday"],
            ]
            for d in dates
        ],
    )

    print(f"dim_date: {len(dates)} fechas agregadas")

if __name__ == "__main__":
    print(">>> INICIANDO EXTENSION DE DIM_DATE")

    conn = get_duckdb_conn()

    try:
        extend_dim_date(conn)
    finally:
        conn.close()

    print(">>> PROCESO TERMINADO")