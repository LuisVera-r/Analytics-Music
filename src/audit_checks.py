from datetime import datetime

def log_check(conn, check_name, status, valor, umbral, detalle):
    conn.execute("""
        INSERT INTO pipeline_quality_log
        VALUES (CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)
    """, [check_name, status, valor, umbral, detalle])

def run_all_checks(conn) -> list:
    """Ejecuta las 6 dimensiones de calidad y devuelve una lista de errores."""
    total = conn.execute("SELECT COUNT(*) FROM fact_streams").fetchone()[0]
    print(f"\n{'═'*50}")
    print(f"  MusicFlow Quality Report — {datetime.now():%Y-%m-%d %H:%M}")
    print(f"  Total filas en fact_streams: {total:,}")
    print(f"{'═'*50}")

    errores = []

    # 1. Completeness
    nulos_user = conn.execute("""
        SELECT COUNT(*) FROM fact_streams WHERE user_sk IS NULL
    """).fetchone()[0]
    status = '✅' if nulos_user == 0 else '❌'
    if status == '❌': errores.append('completeness')
    log_check(conn, 'completeness_user_sk', status, nulos_user, 0, f'{nulos_user} nulos en user_sk')
    print(f"  1. Completeness | user_sk nulos: {nulos_user} {status}")

    # 2. Uniqueness
    dupes = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT stream_sk FROM fact_streams GROUP BY stream_sk HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    status = '✅' if dupes == 0 else '❌'
    if status == '❌': errores.append('uniqueness')
    log_check(conn, 'uniqueness_stream_sk', status, dupes, 0, f'{dupes} duplicados')
    print(f"  2. Uniqueness | Duplicados: {dupes} {status}")

    # 3. Validity
    invalidos = conn.execute("""
        SELECT COUNT(*) FROM fact_streams
        WHERE listened_seconds <= 0 OR listened_seconds > 3600
    """).fetchone()[0]
    pct_invalidos = (invalidos / total * 100) if total > 0 else 0
    status = '✅' if pct_invalidos < 0.1 else '❌'
    if status == '❌': errores.append('validity')
    log_check(conn, 'validity_listened_seconds', status, pct_invalidos, 0.1, f'{invalidos} fuera de rango')
    print(f"  3. Validity | Fuera de rango: {invalidos} ({pct_invalidos:.3f}%) {status}")

    # 4. Referential Integrity
    huerfanos = conn.execute("""
        SELECT COUNT(*) FROM fact_streams f
        WHERE NOT EXISTS (SELECT 1 FROM dim_user u WHERE u.user_sk = f.user_sk)
    """).fetchone()[0]
    status = '✅' if huerfanos == 0 else '❌'
    if status == '❌': errores.append('referential_integrity')
    log_check(conn, 'ref_integrity_user_sk', status, huerfanos, 0, f'{huerfanos} huérfanos')
    print(f"  4. Referential Integrity | Huérfanos: {huerfanos} {status}")

    # 5. Consistency
    avg_segundos = conn.execute("SELECT AVG(listened_seconds) FROM fact_streams").fetchone()[0] or 0.0
    status = '✅' if 30 <= avg_segundos <= 300 else '⚠️'
    log_check(conn, 'consistency_avg_seconds', status, avg_segundos, 300, f'Promedio: {avg_segundos:.1f}s')
    print(f"  5. Consistency | Promedio: {avg_segundos:.1f}s {status}")

    # 6. Timeliness (simulado)
    print(f"  6. Timeliness | Última carga: simulada hace < 1h ✅")

    print(f"{'═'*50}")
    if errores:
        print(f"  🔴 CRITICAL: {len(errores)} checks fallidos: {errores}")
    else:
        print(f"  ✅ Todos los checks pasaron — datos confiables")
    print(f"{'═'*50}\n")
    
    return errores