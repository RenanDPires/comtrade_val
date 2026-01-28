WITH win AS (
  SELECT 
    '2025-08-01 00:00:00'::timestamptz AS t_from,
    '2025-08-25 23:59:59'::timestamptz AS t_to
),

-- (A) Oscilografias (scans) no intervalo
scan_win AS (
  SELECT
    f.sub_id,
    f.ied_id,
    f.stem,
    f.created_at,
    f.integrity_ok,
    COALESCE(jsonb_array_length(f.missing), 0)   AS n_missing,
    COALESCE(jsonb_array_length(f.zero_kb), 0)   AS n_zero_kb
  FROM osci.fato_scan_pkg f
  JOIN win w 
    ON f.created_at >= w.t_from 
   AND f.created_at <= w.t_to
),

-- (B) Canais mapeados como "falha de comutação"
dig_fail_candidates AS (
  SELECT d.ied_id, d.idx1, d.id_hint, d.description
  FROM osci.dim_ied_digital d
  WHERE (d.id_hint ILIKE '%comut%' OR d.description ILIKE '%comut%')
     OR (d.id_hint ILIKE '%falha%' OR d.description ILIKE '%falha%')
     OR (d.id_hint ILIKE '%switch%fail%' OR d.description ILIKE '%switch%fail%')
),

-- (C) Eventos digitais no intervalo (apenas os de falha de comutação)
dig_win AS (
  SELECT
    a.sub_id,
    a.ied_id,
    a.stem,
    a.channel_index,
    a.triggered,
    a.first_rise_dt
  FROM osci.fato_analysis_digital a
  JOIN win w 
    ON a.first_rise_dt IS NOT NULL
   AND a.first_rise_dt >= w.t_from
   AND a.first_rise_dt <= w.t_to
  JOIN dig_fail_candidates c
    ON c.ied_id = a.ied_id
   AND c.idx1   = a.channel_index
),

-- (D) Agregações por subestação
agg_scan AS (
  SELECT
    i.sub_id,
    COUNT(*) AS total_osc,
    SUM(CASE
          WHEN s.integrity_ok = FALSE THEN 1
          WHEN s.n_missing > 0 OR s.n_zero_kb > 0 THEN 1
          ELSE 0
        END) AS err_count
  FROM scan_win s
  JOIN osci.dim_ied i 
    ON i.ied_id = s.ied_id
  GROUP BY i.sub_id
),

agg_dig AS (
  SELECT
    sub_id,
    COUNT(DISTINCT (ied_id || '|' || stem)) FILTER (WHERE triggered) AS switch_fail_count
  FROM dig_win
  GROUP BY sub_id
)

-- (E) Resultado final
SELECT
  d.sub_id,
  d.subestacao_nome,
  d.municipio,
  d.uf,
  d.lat,
  d.lon,
  COALESCE(s.total_osc, 0)         AS total_osc,
  COALESCE(s.err_count, 0)         AS err_count,
  COALESCE(g.switch_fail_count, 0) AS switch_fail_count,
  CASE WHEN COALESCE(s.total_osc,0) > 0
       THEN ROUND(s.err_count::numeric / s.total_osc, 4)
       ELSE 0 END                  AS err_rate,
  CASE WHEN COALESCE(s.total_osc,0) > 0
       THEN ROUND(COALESCE(g.switch_fail_count,0)::numeric / s.total_osc, 4)
       ELSE 0 END                  AS switch_fail_rate,
  d.ieds
FROM osci.v_osci_subestacao_dim d
LEFT JOIN agg_scan s ON s.sub_id = d.sub_id
LEFT JOIN agg_dig  g ON g.sub_id = d.sub_id
-- WHERE d.lat IS NOT NULL AND d.lon IS NOT NULL
ORDER BY d.subestacao_nome;
