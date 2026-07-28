from datetime import datetime

def evaluar_slo_freshness(conn, errores: list, hora_limite: int = 8) -> dict:
    """Evalúa si el SLO de frescura se cumplió hoy."""
    hora_actual = datetime.now().hour
    cumplido = hora_actual < hora_limite or len(errores) == 0

    conn.execute("""
        INSERT INTO slo_tracker VALUES (CURRENT_DATE, ?, 99.5, ?, ?)
    """, ['freshness_8am', cumplido, hora_actual])

    return {
        'slo': 'fact_streams actualizado antes de las 8am',
        'target': '99.5%',
        'cumplido_hoy': '✅' if cumplido else '❌',
    }

def calcular_error_budget(conn) -> dict:
    """Calcula el Error Budget restante basado en el historial de slo_tracker."""
    total_dias_result = conn.execute("SELECT COUNT(DISTINCT fecha) FROM slo_tracker").fetchone()
    dias_fallidos_result = conn.execute("SELECT COUNT(*) FROM slo_tracker WHERE NOT cumplido").fetchone()
    
    total_dias = total_dias_result[0] if total_dias_result else 0
    dias_fallidos = dias_fallidos_result[0] if dias_fallidos_result else 0

    if total_dias == 0:
        return {'error': 'Sin datos históricos aún'}

    tasa_exito = ((total_dias - dias_fallidos) / total_dias) * 100
    budget_total = total_dias * 0.005
    budget_restante = budget_total - dias_fallidos

    return {
        'dias_medidos': total_dias,
        'tasa_exito': f'{tasa_exito:.2f}%',
        'budget_total': f'{budget_total:.1f} días',
        'budget_gastado': f'{dias_fallidos} días',
        'budget_restante': f'{budget_restante:.1f} días',
        'status': '✅ Saludable' if budget_restante > 0 else '🔴 Agotado'
    }