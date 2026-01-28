SET search_path TO osci, public;

-- Lista de IEDs por subestação (para tooltip)
CREATE OR REPLACE VIEW v_osci_subestacao_dim AS
SELECT
  s.sub_id,
  COALESCE(NULLIF(s.nome, ''), s.sub_id) AS subestacao_nome,
  s.municipio,
  s.uf,
  s.lat,
  s.lon,
  STRING_AGG(i.ied_id, ', ' ORDER BY i.ied_id) AS ieds
FROM dim_subestacao s
LEFT JOIN dim_ied i
  ON i.sub_id = s.sub_id
GROUP BY s.sub_id, s.nome, s.municipio, s.uf, s.lat, s.lon;
