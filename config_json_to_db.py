#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Popula/atualiza as tabelas de catálogo no PostgreSQL (schema osci) a partir do config.json.

Cria/atualiza:
- osci.dim_subestacao (inclui lat/lon/municipio/uf a partir de sub.location)
- osci.dim_ied
- osci.dim_ied_expected_file
- osci.dim_ied_digital
- (opcional) osci.dim_config_snapshot  [snapshot versionado do JSON]

Comportamento:
- Somente adiciona itens novos.
- Atualiza campos existentes de forma NÃO destrutiva (usa COALESCE: só aplica quando vier valor no JSON).
"""

# ========= EDITE AQUI =========
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "oscilografias_v0"
DB_USER = "postgres"
DB_PASS = "admin"
CONFIG_PATH = r"D:\Projetos\comtrade_val\config_with_locations.json"
VERSION_TAG = "manual_load_v1"  # "" para NÃO criar snapshot
# ==============================

import json
import re
from typing import Dict, Any, List, Optional, Tuple

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------
# Engine (sem credenciais na URL p/ evitar problemas com encoding)
# ---------------------------------------------------------------------
engine: Engine = create_engine(
    "postgresql+psycopg2://",
    connect_args=dict(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        passfile="NUL",  # Windows: não lê %APPDATA%\postgresql\pgpass.conf
    ),
    pool_pre_ping=True,
    future=True,
)

# (opcional) sanity check inicial
with engine.connect() as c:
    db, = c.execute(text("select current_database()")).one()
    print("Conectado em:", db)
    n, = c.execute(text("""
        select count(*)
          from information_schema.tables
         where table_schema='osci'
           and table_name in ('dim_config_snapshot','dim_subestacao','dim_ied',
                              'dim_ied_expected_file','dim_ied_digital')
    """)).one()
    print("Tabelas de config no schema osci:", n)

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def norm_ext(ext: str) -> str:
    """Garante que a extensão tenha ponto e esteja em minúsculas."""
    ext = (ext or "").strip()
    if not ext:
        return ""
    e = ext.lower()
    return e if e.startswith(".") else f".{e}"

def to_float_or_none(v) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        f = float(v)
        # valida faixa geográfica básica
        return f
    except (TypeError, ValueError):
        return None

def parse_municipio_uf_from_locname(name: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extrai "Municipio, UF" se existir entre parênteses no final do name.
    Ex.: "Sub Terminal Rio (Paracambi, RJ)" -> ("Paracambi", "RJ")
         "Sub Xingu (Anapu, PA)"            -> ("Anapu", "PA")
    Retorna (None, None) se não conseguir extrair.
    """
    if not name:
        return None, None
    m = re.search(r"\(([^()]+)\)\s*$", name.strip())
    if not m:
        return None, None
    inner = m.group(1)  # "Paracambi, RJ"
    parts = [p.strip() for p in inner.split(",")]
    if len(parts) == 2:
        municipio, uf = parts[0] or None, parts[1] or None
        # normaliza UF para 2 letras maiúsculas se parecer UF
        if uf and re.fullmatch(r"[A-Za-z]{2}", uf):
            uf = uf.upper()
        return municipio, uf
    return None, None

# ---------------------------------------------------------------------
# Ingestão do JSON nas tabelas de catálogo
# ---------------------------------------------------------------------
def upsert_catalog_from_config(cfg: Dict[str, Any], engine: Engine, version_tag: Optional[str]) -> Optional[int]:
    """
    Popula/atualiza:
      - osci.dim_subestacao
      - osci.dim_ied
      - osci.dim_ied_expected_file
      - osci.dim_ied_digital
      - (opcional) osci.dim_config_snapshot
    Retorna cfg_id do snapshot se criado.
    """
    snapshot_id: Optional[int] = None

    with engine.begin() as conn:
        # 1) Snapshot versionado (opcional)
        if version_tag:
            conn.execute(text("""
                UPDATE osci.dim_config_snapshot
                   SET effective_to = now()
                 WHERE effective_to IS NULL
            """))
            snapshot_id = conn.execute(
                text("""
                    INSERT INTO osci.dim_config_snapshot (version_tag, payload_json, effective_from)
                    VALUES (:vtag, CAST(:payload AS JSONB), now())
                    RETURNING cfg_id
                """),
                {"vtag": version_tag, "payload": json.dumps(cfg, ensure_ascii=False)},
            ).scalar_one()

        # 2) Subestações / IEDs / extensões / canais
        subs: List[Dict[str, Any]] = cfg.get("substations", []) or []
        for sub in subs:
            sub_id: str = sub["id"]
            base_dir: str = sub.get("base_dir") or ""
            # nome: não vem no JSON atual — mantemos = sub_id
            nome: str = sub.get("name") or sub_id

            # localização
            loc: Dict[str, Any] = sub.get("location") or {}
            lat = to_float_or_none(loc.get("lat"))
            lon = to_float_or_none(loc.get("lon"))
            municipio, uf = parse_municipio_uf_from_locname(loc.get("name"))

            # INSERT/UPSERT da subestação (não destrutivo)
            conn.execute(text("""
                INSERT INTO osci.dim_subestacao (sub_id, nome, base_dir, municipio, uf, lat, lon, updated_at)
                VALUES (:sid, :nome, :bdir, :mun, :uf, :lat, :lon, now())
                ON CONFLICT (sub_id) DO UPDATE SET
                  nome       = COALESCE(EXCLUDED.nome,      osci.dim_subestacao.nome),
                  base_dir   = COALESCE(EXCLUDED.base_dir,  osci.dim_subestacao.base_dir),
                  municipio  = COALESCE(EXCLUDED.municipio, osci.dim_subestacao.municipio),
                  uf         = COALESCE(EXCLUDED.uf,        osci.dim_subestacao.uf),
                  lat        = COALESCE(EXCLUDED.lat,       osci.dim_subestacao.lat),
                  lon        = COALESCE(EXCLUDED.lon,       osci.dim_subestacao.lon),
                  updated_at = now()
            """), {
                "sid": sub_id,
                "nome": nome,
                "bdir": base_dir,
                "mun": municipio,
                "uf": uf,
                "lat": lat,
                "lon": lon,
            })

            # IEDs
            for ied in (sub.get("ieds") or []):
                ied_id: str = ied["id"]
                conn.execute(text("""
                    INSERT INTO osci.dim_ied (ied_id, sub_id, descricao, updated_at)
                    VALUES (:iid, :sid, :desc, now())
                    ON CONFLICT (ied_id) DO UPDATE SET
                      sub_id    = COALESCE(EXCLUDED.sub_id,   osci.dim_ied.sub_id),
                      descricao = COALESCE(EXCLUDED.descricao,osci.dim_ied.descricao),
                      updated_at= now()
                """), {"iid": ied_id, "sid": sub_id, "desc": ied.get("description")})

                # Extensões esperadas (dedupe simples)
                for ext in (ied.get("expected_files") or []):
                    e = norm_ext(ext)
                    if not e:
                        continue
                    conn.execute(text("""
                        INSERT INTO osci.dim_ied_expected_file (ied_id, ext)
                        VALUES (:iid, :ext)
                        ON CONFLICT (ied_id, ext) DO NOTHING
                    """), {"iid": ied_id, "ext": e})

                # Canais digitais
                digitals = ((ied.get("channels") or {}).get("digitals") or [])
                for d in digitals:
                    if "index" not in d:
                        continue
                    conn.execute(text("""
                        INSERT INTO osci.dim_ied_digital (ied_id, idx1, id_hint, description)
                        VALUES (:iid, :idx1, :hint, :descr)
                        ON CONFLICT (ied_id, idx1) DO UPDATE SET
                          id_hint     = COALESCE(EXCLUDED.id_hint,     osci.dim_ied_digital.id_hint),
                          description = COALESCE(EXCLUDED.description, osci.dim_ied_digital.description)
                    """), {
                        "iid":  ied_id,
                        "idx1": int(d["index"]),
                        "hint": (d.get("id_hint") or None),
                        "descr": (d.get("description") or None),
                    })

    return snapshot_id

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    vtag = VERSION_TAG.strip() or None
    cfg_id = upsert_catalog_from_config(cfg, engine, version_tag=vtag)

    print("[OK] Catálogo atualizado no schema osci.")
    if cfg_id:
        print(f"[OK] Snapshot salvo: cfg_id={cfg_id}, tag='{vtag}'")

if __name__ == "__main__":
    main()
