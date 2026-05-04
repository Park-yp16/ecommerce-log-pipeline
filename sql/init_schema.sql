-- pipeline_db 초기 스키마

CREATE TABLE IF NOT EXISTS public.session_stats (
    session_id          TEXT        NOT NULL,
    user_id             TEXT        NOT NULL,
    device              TEXT,
    event_count         INTEGER,
    purchase_count      INTEGER,
    total_revenue       NUMERIC(12, 2),
    session_start       TIMESTAMP,
    session_end         TIMESTAMP,
    session_duration_min NUMERIC(8, 2),
    converted           BOOLEAN,
    etl_date            DATE        NOT NULL,
    loaded_at           TIMESTAMP   NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, etl_date)
);

CREATE INDEX IF NOT EXISTS idx_session_stats_etl_date ON public.session_stats (etl_date);
CREATE INDEX IF NOT EXISTS idx_session_stats_user_id  ON public.session_stats (user_id);

-- 일별 요약 뷰
CREATE OR REPLACE VIEW public.daily_summary AS
SELECT
    etl_date,
    COUNT(DISTINCT session_id)              AS total_sessions,
    COUNT(DISTINCT user_id)                 AS unique_users,
    SUM(event_count)                        AS total_events,
    SUM(purchase_count)                     AS total_purchases,
    ROUND(SUM(total_revenue)::numeric, 0)   AS total_revenue,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE converted) / NULLIF(COUNT(*), 0), 2
    )                                       AS conversion_rate_pct
FROM public.session_stats
GROUP BY etl_date
ORDER BY etl_date DESC;
