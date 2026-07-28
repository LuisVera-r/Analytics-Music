-- Tabla de histórico de quality runs
CREATE TABLE IF NOT EXISTS pipeline_quality_log (
    run_at          TIMESTAMPTZ,
    check_name      VARCHAR,
    status          VARCHAR,
    valor_medido    DOUBLE,
    umbral          DOUBLE,
    detalle         VARCHAR
);

-- Tabla de SLO histórico
CREATE TABLE IF NOT EXISTS slo_tracker (
    fecha           DATE,
    slo_name        VARCHAR,
    target_pct      DOUBLE,
    cumplido        BOOLEAN,
    valor_real      DOUBLE
);