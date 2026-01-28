-- ==== SCHEMA =========================================================
CREATE SCHEMA IF NOT EXISTS osci;
SET search_path TO osci, public;

-- ==== CONFIGURAÇÃO (catálogo do JSON) ================================

-- Subestações (inclui base_dir do JSON e campos de localização futuros)
CREATE TABLE IF NOT EXISTS dim_subestacao (
  sub_id     TEXT PRIMARY KEY,            -- ex: 'SE_RJTRIO', 'SE_PAXNGX'
  nome       TEXT NOT NULL DEFAULT '',    -- pode ficar igual ao id por ora
  base_dir   TEXT NOT NULL,               -- do JSON
  municipio  TEXT,
  uf         TEXT,
  lat        DOUBLE PRECISION,            -- WGS84 (opcional, para futuro)
  lon        DOUBLE PRECISION,            -- WGS84 (opcional, para futuro)
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- IEDs (cada IED pertence a uma subestação)
CREATE TABLE IF NOT EXISTS dim_ied (
  ied_id     TEXT PRIMARY KEY,            -- ex: 'RJTRIO_PL1_UPD1'
  sub_id     TEXT NOT NULL REFERENCES dim_subestacao(sub_id),
  descricao  TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS k_ied_sub ON dim_ied(sub_id);

-- Extensões esperadas por IED ('.cfg', '.dat', '.hdr'…)
CREATE TABLE IF NOT EXISTS dim_ied_expected_file (
  ied_id     TEXT NOT NULL REFERENCES dim_ied(ied_id),
  ext        TEXT NOT NULL,               -- sempre com ponto: '.cfg'
  PRIMARY KEY (ied_id, ext)
);

-- Canais digitais (índice 1‑based do JSON, id_hint e descrição)
CREATE TABLE IF NOT EXISTS dim_ied_digital (
  ied_id     TEXT NOT NULL REFERENCES dim_ied(ied_id),
  idx1       INT  NOT NULL,               -- 1-based
  id_hint    TEXT,
  description TEXT,
  PRIMARY KEY (ied_id, idx1)
);

-- (Opcional, mas recomendável) snapshot do JSON bruto para versionamento
CREATE TABLE IF NOT EXISTS dim_config_snapshot (
  cfg_id        BIGSERIAL PRIMARY KEY,
  version_tag   TEXT NOT NULL,            -- ex: '2025-08-24_v1'
  payload_json  JSONB NOT NULL,           -- cópia do config.json
  effective_from TIMESTAMPTZ NOT NULL DEFAULT now(),
  effective_to   TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS k_cfg_from ON dim_config_snapshot (effective_from);
CREATE INDEX IF NOT EXISTS k_cfg_to   ON dim_config_snapshot (effective_to);

-- ==== RESULTADOS (compatíveis com seu fluxo atual) ===================

-- Scan por oscilografia (um pacote por stem)
CREATE TABLE IF NOT EXISTS fato_scan_pkg (
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),  -- carimbo da ingestão
  sub_id              TEXT NOT NULL REFERENCES dim_subestacao(sub_id),
  ied_id              TEXT NOT NULL REFERENCES dim_ied(ied_id),
  directory           TEXT NOT NULL,
  stem                TEXT NOT NULL,
  expected            JSONB,                                -- lista de extensões esperadas
  present             JSONB,                                -- lista de presentes
  missing             JSONB,                                -- lista de faltantes
  zero_kb             JSONB,                                -- lista de 0 KB
  size_cfg            BIGINT,
  size_dat            BIGINT,
  size_hdr            BIGINT,
  size_inf            BIGINT,
  max_arrival_skew_s  DOUBLE PRECISION,
  received_ts_epoch   DOUBLE PRECISION,                     -- epoch (float) do seu código
  integrity_ok        BOOLEAN NOT NULL,
  PRIMARY KEY (sub_id, ied_id, stem)
);
CREATE INDEX IF NOT EXISTS k_scan_created  ON fato_scan_pkg (created_at DESC);
CREATE INDEX IF NOT EXISTS k_scan_received ON fato_scan_pkg (received_ts_epoch);

-- Resultados por canal digital (uma linha por canal/osc)
CREATE TABLE IF NOT EXISTS fato_analysis_digital (
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),     -- carimbo da ingestão
  sub_id           TEXT NOT NULL REFERENCES dim_subestacao(sub_id),
  ied_id           TEXT NOT NULL REFERENCES dim_ied(ied_id),
  stem             TEXT NOT NULL,
  channel_index    INT  NOT NULL,                           -- 1‑based
  channel_name     TEXT,
  id_hint          TEXT,
  description      TEXT,
  triggered        BOOLEAN NOT NULL,
  first_rise_dt    TIMESTAMPTZ,
  n_rises          INT  NOT NULL DEFAULT 0,
  PRIMARY KEY (sub_id, ied_id, stem, channel_index)
);
CREATE INDEX IF NOT EXISTS k_analysis_first_rise ON fato_analysis_digital (first_rise_dt DESC);
