#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Iterable, Any
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json

import numpy as np
import pandas as pd
from comtrade import Comtrade

# =============================================================================
# Configuração do ambiente
# =============================================================================

# ======= EDITE AQUI: conexão com o PostgreSQL =======
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "oscilografias_v0"
DB_USER = "postgres"
DB_PASS = "admin"
# =====================================================

# Salvar CSVs também? (útil para conferência / legado)
WRITE_CSV = False

HERE = Path(__file__).resolve().parent
SCAN_CSV_TPL = "scan_{sub}.csv"
ANALYSIS_CSV_TPL = "analysis_{sub}.csv"

VALID_EXTS = {".cfg", ".dat", ".hdr", ".inf"}
TZ = ZoneInfo("America/Sao_Paulo")

# =============================================================================
# Conexão PG
# =============================================================================
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def get_pg_engine() -> Engine:
    # Evita .pgpass e problemas de encoding no Windows
    return create_engine(
        "postgresql+psycopg2://",
        connect_args=dict(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            passfile="NUL",
        ),
        pool_pre_ping=True,
        future=True,
    )


# =============================================================================
# Helpers (pré-ingestão)
# =============================================================================

def norm_ext(ext: str) -> str:
    e = ext.lower()
    return e if e.startswith(".") else f".{e}"


def stem_of(p: Path) -> str:
    return p.with_suffix("").name


def list_files_se(base_dir: Path) -> Iterable[Path]:
    pats = ["*.cfg", "*.CFG", "*.dat", "*.DAT", "*.hdr", "*.HDR", "*.inf", "*.INF"]
    for pat in pats:
        yield from base_dir.glob(pat)


# =============================================================================
# Extrair datetime base do STEM 
# =============================================================================

_STEM_TS_RE = re.compile(r"-(\d{8})-(\d{6,9})-OSC", re.IGNORECASE)


def stem_base_datetime(stem: str) -> datetime | None:
    m = _STEM_TS_RE.search(stem)
    if not m:
        return None
    ymd, hmsx = m.group(1), m.group(2)
    year = int(ymd[0:4])
    month = int(ymd[4:6])
    day = int(ymd[6:8])
    hour = int(hmsx[0:2])
    minute = int(hmsx[2:4])
    second = int(hmsx[4:6])
    ms = int(hmsx[6:9]) if len(hmsx) >= 9 else 0
    try:
        return datetime(year, month, day, hour, minute, second, ms * 1000, tzinfo=TZ)
    except Exception:
        return None


# =============================================================================
# Estruturas
# =============================================================================

@dataclass
class FileInfo:
    size: int
    mtime: float


@dataclass
class Package:
    substation: str
    ied: str
    directory: str
    stem: str
    expected: List[str]
    present: List[str]
    missing: List[str]
    zero_kb: List[str]
    max_arrival_skew_s: float
    received_ts: float
    sizes: Dict[str, int | None]

    @property
    def integrity_ok(self) -> bool:
        return (len(self.missing) == 0) and (len(self.zero_kb) == 0)

    def to_row(self) -> Dict:
        row = {
            "substation": self.substation,
            "ied": self.ied,
            "directory": self.directory,
            "stem": self.stem,
            "expected": ",".join(self.expected),
            "present": ",".join(sorted(self.present)),
            "missing": ",".join(sorted(self.missing)),
            "zero_kb": ",".join(sorted(self.zero_kb)),
            "max_arrival_skew_s": self.max_arrival_skew_s,
            "received_ts": self.received_ts,
            "integrity_ok": self.integrity_ok,
        }
        for e in sorted(VALID_EXTS):
            row[f"size_{e.lstrip('.')}"] = self.sizes.get(e)
        return row


# =============================================================================
# Pré-ingestão
# =============================================================================

def scan_substation(sub_cfg: dict) -> pd.DataFrame:
    sub_id = sub_cfg["id"]
    base_dir = Path(sub_cfg["base_dir"])

    packages: List[Package] = []

    for ied in sub_cfg.get("ieds", []):
        ied_id = ied["id"]
        expected = [norm_ext(e) for e in ied.get("expected_files", [])]

        files = [p for p in list_files_se(base_dir) if ied_id in p.name]

        groups: Dict[tuple, List[Path]] = {}
        for p in files:
            key = (str(p.parent), stem_of(p))
            groups.setdefault(key, []).append(p)

        for (dir_path, st), paths in groups.items():
            infos: Dict[str, FileInfo] = {}
            for p in paths:
                ext = norm_ext(p.suffix)
                if ext not in VALID_EXTS:
                    continue
                st_ = p.stat()
                infos[ext] = FileInfo(size=st_.st_size, mtime=st_.st_mtime)

            present_exts = sorted(infos.keys())
            missing = [e for e in expected if e not in infos]
            zero_kb = [e for e in expected if e in infos and infos[e].size == 0]

            mtimes = [fi.mtime for fi in infos.values()]
            skew = (max(mtimes) - min(mtimes)) if mtimes else 0.0
            received = max(mtimes) if mtimes else 0.0

            sizes = {e: (infos[e].size if e in infos else None) for e in VALID_EXTS}

            packages.append(
                Package(
                    substation=sub_id,
                    ied=ied_id,
                    directory=dir_path,
                    stem=st,
                    expected=expected,
                    present=present_exts,
                    missing=missing,
                    zero_kb=zero_kb,
                    max_arrival_skew_s=skew,
                    received_ts=received,
                    sizes=sizes,
                )
            )

    # print(packages)

    # Se não encontrou nenhum pacote, retorna DF vazio
    if not packages:
        return pd.DataFrame(
            columns=[
                "substation",
                "ied",
                "directory",
                "stem",
                "expected",
                "present",
                "missing",
                "zero_kb",
                "max_arrival_skew_s",
                "received_ts",
                "integrity_ok",
                *[f"size_{e.lstrip('.')}" for e in sorted(VALID_EXTS)],
            ]
        )

    df = pd.DataFrame([p.to_row() for p in packages]).sort_values(
        ["integrity_ok", "ied", "stem"], ascending=[False, True, True]
    )
    size_cols = [c for c in df.columns if c.startswith("size_")]
    ordered = [c for c in df.columns if not c.startswith("size_")] + size_cols
    return df[ordered]


# =============================================================================
# Análise COMTRADE
# =============================================================================

def _load_comtrade_digitals(stem_path: Path) -> tuple[pd.DataFrame, Dict, List[str]]:
    cfg = stem_path.with_suffix(".cfg")
    if not cfg.exists():
        cfg = stem_path.with_suffix(".CFG")
    dat = stem_path.with_suffix(".dat")
    if not dat.exists():
        dat = stem_path.with_suffix(".DAT")
    if not cfg.exists() or not dat.exists():
        raise FileNotFoundError(f"CFG/DAT ausentes para {stem_path}")

    rec = Comtrade()
    rec.load(str(cfg), str(dat))

    if getattr(rec, "time", None) is not None and len(rec.time):
        t = np.asarray(rec.time, dtype=float)
    else:
        fs = getattr(rec, "fs", None) or getattr(rec, "frequency", None)
        if fs is None and hasattr(rec, "cfg"):
            sr = getattr(rec.cfg, "sample_rates", None)
            if sr:
                try:
                    fs = float(sr[0][0])
                except Exception:
                    fs = None
        n = len(rec.status[0]) if getattr(rec, "status_count", 0) else 0
        t = np.arange(n, dtype=float) / float(fs) if (fs and n) else np.arange(n, dtype=float)

    nD = int(getattr(rec, "status_count", 0) or 0)
    ids = getattr(rec, "status_channel_ids", None) or getattr(rec, "status_ids", None)
    names = getattr(rec, "status_channel_names", None) or getattr(rec, "status_names", None)
    labels = getattr(rec, "status_channel_labels", None)

    def _get(lst, i):
        try:
            v = lst[i]
            if isinstance(v, str) and v.strip() == "":
                return None
            return v
        except Exception:
            return None

    chosen_names: List[str] = []
    cols = {}
    for i in range(nD):
        raw_id = _get(ids, i) if ids is not None else None
        raw_name = _get(names, i) if names is not None else None
        raw_lab = _get(labels, i) if labels is not None else None
        chosen = raw_id or raw_name or raw_lab or f"DI{i+1}"
        chosen_names.append(str(chosen))
        cols[str(chosen)] = np.asarray(rec.status[i], dtype=int)

    df = pd.DataFrame(cols, index=pd.Index(t, name="t"))
    meta = {"cfg": str(cfg), "dat": str(dat), "status_count": nD}
    return df, meta, chosen_names


def _ensure_binary(s: pd.Series) -> pd.Series:
    if s.dtype.kind == "b":
        return s.astype(int)
    if s.dtype.kind == "f":
        return (s >= 0.5).astype(int)
    return (s.astype(int) != 0).astype(int)


def _rising_edges(s: pd.Series) -> List[float]:
    s = _ensure_binary(s)
    if s.empty:
        return []
    v = s.values
    t = s.index.values
    edges: List[float] = []
    prev = v[0]
    for i in range(1, len(v)):
        if prev == 0 and v[i] == 1:
            edges.append(float(t[i]))
        prev = v[i]
    return edges


def analyze_integral_packages(sub_cfg: dict, df_scan: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict] = []
    sub_id = sub_cfg["id"]

    digitals_by_ied: Dict[str, List[Dict]] = {}
    for ied in sub_cfg.get("ieds", []):
        dlist = ied.get("channels", {}).get("digitals", [])
        digitals_by_ied[ied["id"]] = [d for d in dlist if "index" in d]

    df_valid = df_scan[(df_scan["substation"] == sub_id) & (df_scan["integrity_ok"] == True)]

    for _, r in df_valid.iterrows():
        ied_id = r["ied"]
        stem = r["stem"]
        stem_path = Path(r["directory"]) / stem

        try:
            df_dig, meta, chosen_names = _load_comtrade_digitals(stem_path)
        except Exception as e:
            rows.append(
                {
                    "substation": sub_id,
                    "ied": ied_id,
                    "stem": stem,
                    "status": "load_error",
                    "error": str(e),
                }
            )
            continue

        n_channels = len(chosen_names)
        base_dt = stem_base_datetime(stem)

        for ch in digitals_by_ied.get(ied_id, []):
            idx = int(ch["index"])
            id_hint = ch.get("id_hint")
            desc = ch.get("description")

            if not (1 <= idx <= n_channels):
                rows.append(
                    {
                        "substation": sub_id,
                        "ied": ied_id,
                        "stem": stem,
                        "channel_index": idx,
                        "channel_name": None,
                        "id_hint": id_hint,
                        "description": desc,
                        "triggered": False,
                        "first_rise_s": None,
                        "first_rise_dt": None,
                        "n_rises": 0,
                        "note": "index_out_of_range",
                    }
                )
                continue

            col_name = chosen_names[idx - 1]

            s = df_dig[col_name]
            rises = _rising_edges(s)

            first_s = (rises[0] if rises else None)
            first_dt = (base_dt + timedelta(seconds=first_s)) if (base_dt and first_s is not None) else None

            rows.append(
                {
                    "substation": sub_id,
                    "ied": ied_id,
                    "osc": stem,
                    "channel_index": int(idx),
                    "channel_name": col_name,
                    "id_hint": id_hint,
                    "description": desc,
                    "triggered": bool(rises),
                    "first_rise_dt": first_dt.isoformat() if first_dt else None,
                    "n_rises": len(rises),
                }
            )

    return pd.DataFrame(rows)


# =============================================================================
# Camada de persistência (PostgreSQL)
# =============================================================================

def write_scan_df_to_pg(df_scan: pd.DataFrame, sub_id: str, cfg_id: int | None, engine: Engine):
    if df_scan is None or df_scan.empty:
        return

    def _as_json_list(v: Any) -> str:
        if isinstance(v, str):
            arr = [x for x in v.split(",") if x]
            return json.dumps(arr, ensure_ascii=False)
        if v is None:
            return "[]"
        return json.dumps(v, ensure_ascii=False)

    def _int_or_none(v):
        # None ou NaN -> None; números -> int
        if v is None:
            return None
        try:
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                return None
        except Exception:
            pass
        try:
            return int(v)
        except Exception:
            return None

    with engine.begin() as conn:
        sql = text("""
          INSERT INTO osci.analysis_osc_integras
            (sub_id, ied_id, directory, stem,
             expected, present, missing, zero_kb,
             size_cfg, size_dat, size_hdr, size_inf,
             max_arrival_skew_s, received_ts_epoch, integrity_ok, cfg_id)
          VALUES
            (:sub_id, :ied_id, :directory, :stem,
             CAST(:expected AS JSONB), CAST(:present AS JSONB),
             CAST(:missing AS JSONB), CAST(:zero_kb AS JSONB),
             :size_cfg, :size_dat, :size_hdr, :size_inf,
             :skew, :rcv, :ok, :cfg_id)
          ON CONFLICT (sub_id, ied_id, stem) DO UPDATE SET
             directory = EXCLUDED.directory,
             expected  = EXCLUDED.expected,
             present   = EXCLUDED.present,
             missing   = EXCLUDED.missing,
             zero_kb   = EXCLUDED.zero_kb,
             size_cfg  = EXCLUDED.size_cfg,
             size_dat  = EXCLUDED.size_dat,
             size_hdr  = EXCLUDED.size_hdr,
             size_inf  = EXCLUDED.size_inf,
             max_arrival_skew_s = EXCLUDED.max_arrival_skew_s,
             received_ts_epoch  = EXCLUDED.received_ts_epoch,
             integrity_ok = EXCLUDED.integrity_ok,
             cfg_id      = EXCLUDED.cfg_id
        """)

        for _, r in df_scan.iterrows():
            conn.execute(
                sql,
                {
                    "sub_id": sub_id,
                    "ied_id": r["ied"],
                    "directory": r["directory"],
                    "stem": r["stem"],
                    "expected": _as_json_list(r.get("expected")),
                    "present": _as_json_list(r.get("present")),
                    "missing": _as_json_list(r.get("missing")),
                    "zero_kb": _as_json_list(r.get("zero_kb")),
                    "size_cfg": _int_or_none(r.get("size_cfg")),
                    "size_dat": _int_or_none(r.get("size_dat")),
                    "size_hdr": _int_or_none(r.get("size_hdr")),
                    "size_inf": _int_or_none(r.get("size_inf")),
                    "skew": float(r.get("max_arrival_skew_s", 0) or 0),
                    "rcv": float(r.get("received_ts", 0) or 0),
                    "ok": bool(r.get("integrity_ok", False)),
                    "cfg_id": cfg_id,
                },
            )


def write_analysis_df_to_pg(df_analysis: pd.DataFrame, sub_id: str, engine: Engine, cfg_id: int | None = None):
    if df_analysis is None or df_analysis.empty:
        return

    # Normaliza e filtra linhas com channel_index válido (inteiro)
    df = df_analysis.copy()

    # Garante a coluna e numérica
    if "channel_index" not in df.columns:
        return

    df["channel_index_num"] = pd.to_numeric(df["channel_index"], errors="coerce")
    valid_mask = df["channel_index_num"].notna()  # remove NaN (ex.: load_error)
    df = df[valid_mask].copy()
    if df.empty:
        return

    # Cast controlado para int
    df["channel_index_num"] = df["channel_index_num"].astype(int)

    # Normaliza n_rises
    if "n_rises" not in df.columns:
        df["n_rises"] = 0
    df["n_rises_num"] = pd.to_numeric(df["n_rises"], errors="coerce").fillna(0).astype(int)

    # Normaliza first_rise_dt (None se vazio/NaN)
    def _norm_frdt(v):
        if v is None:
            return None
        if isinstance(v, float) and np.isnan(v):
            return None
        s = str(v).strip()
        return s or None

    with engine.begin() as conn:
        sql = text("""
          INSERT INTO osci.analysis_digital_triggers
            (sub_id, ied_id, stem, channel_index, channel_name, id_hint, description,
             triggered, first_rise_dt, n_rises, cfg_id)
          VALUES
            (:sub_id, :ied, :stem, :idx, :name, :hint, :desc,
             :trig, :frdt, :nr, :cfg_id)
          ON CONFLICT (sub_id, ied_id, stem, channel_index) DO UPDATE SET
             channel_name   = EXCLUDED.channel_name,
             id_hint        = EXCLUDED.id_hint,
             description    = EXCLUDED.description,
             triggered      = EXCLUDED.triggered,
             first_rise_dt  = EXCLUDED.first_rise_dt,
             n_rises        = EXCLUDED.n_rises,
             cfg_id         = EXCLUDED.cfg_id
        """)
        for _, r in df.iterrows():
            frdt = _norm_frdt(r.get("first_rise_dt"))
            conn.execute(
                sql,
                {
                    "sub_id": sub_id,
                    "ied": r.get("ied"),
                    "stem": r.get("osc"),
                    "idx": int(r["channel_index_num"]),
                    "name": r.get("channel_name"),
                    "hint": r.get("id_hint"),
                    "desc": r.get("description"),
                    "trig": bool(r.get("triggered", False)),
                    "frdt": frdt,  # string ISO ou None (PG aceita)
                    "nr": int(r["n_rises_num"]),
                    "cfg_id": cfg_id,
                },
            )


# =============================================================================
# Regras para pular/reprocessar (checa “já processada e íntegra”)
# =============================================================================

def _same_or_none(a, b) -> bool:
    return (a is None and b is None) or (a == b)


def _float_close(a: float | None, b: float | None, tol: float = 1e-3) -> bool:
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) <= tol
    except Exception:
        return False


def was_already_processed_and_integral(
    engine: Engine,
    sub_id: str,
    ied_id: str,
    stem: str,
) -> bool:
    """
    True se:
      - existe pelo menos 1 registro em analysis_digital_triggers
        para (sub_id, ied_id, stem)
    """
    with engine.connect() as c:
        row = c.execute(text("""
            SELECT 1
              FROM osci.analysis_digital_triggers
             WHERE sub_id = :sub
               AND ied_id = :ied
               AND stem   = :stem
             LIMIT 1
        """), {
            "sub": sub_id,
            "ied": ied_id,
            "stem": stem
        }).first()

    return row is not None


def purge_digital_triggers_for_stem(engine: Engine, sub_id: str, ied_id: str, stem: str):
    """Remove triggers antigos antes de reprocessar, evitando sobras."""
    with engine.begin() as conn:
        conn.execute(
            text("""
            DELETE FROM osci.analysis_digital_triggers
             WHERE sub_id = :sub
               AND ied_id = :ied
               AND stem   = :stem
        """),
            {"sub": sub_id, "ied": ied_id, "stem": stem},
        )


# =============================================================================
# Carregar a configuração do BANCO (monta o “sub_cfg” compatível com o JSON)
# =============================================================================

def load_runtime_config_from_db(engine: Engine) -> List[dict]:
    """
    Retorna uma lista de sub_cfg no formato:
    {
      "id": "SE_XXX",
      "base_dir": "C:/.../WaveForms",
      "ieds": [
         {"id": "IED1", "expected_files": [".cfg",".dat",".hdr"],
          "channels": {"digitals": [{"index":1,"id_hint":"...","description":"..."}]}}
      ]
    }
    """
    with engine.connect() as c:
        # subestações com base_dir
        subs = c.execute(
            text("""
            SELECT sub_id, COALESCE(base_dir,'') AS base_dir
              FROM osci.def_subestacao_ied
             ORDER BY sub_id
        """)
        ).mappings().all()

        # IEDs
        ieds = c.execute(
            text("""
            SELECT ied_id, sub_id, COALESCE(descricao,'') AS descricao
              FROM osci.def_ied
        """)
        ).mappings().all()

        # expected files
        exts = c.execute(
            text("""
            SELECT ied_id, ext
              FROM osci.def_expected_files
        """)
        ).mappings().all()

        # digitals
        digs = c.execute(
            text("""
            SELECT ied_id, idx1, id_hint, description
              FROM osci.def_digital_channels
             ORDER BY ied_id, idx1
        """)
        ).mappings().all()

    # indexações rápidas
    exts_by_ied: Dict[str, List[str]] = {}
    for r in exts:
        exts_by_ied.setdefault(r["ied_id"], []).append(r["ext"])

    digs_by_ied: Dict[str, List[Dict]] = {}
    for r in digs:
        digs_by_ied.setdefault(r["ied_id"], []).append(
            {"index": int(r["idx1"]), "id_hint": r["id_hint"], "description": r["description"]}
        )

    ieds_by_sub: Dict[str, List[Dict]] = {}
    for r in ieds:
        ied_id = r["ied_id"]
        sub_id = r["sub_id"]
        ieds_by_sub.setdefault(sub_id, []).append(
            {
                "id": ied_id,
                "expected_files": exts_by_ied.get(ied_id, []),
                "channels": {"digitals": digs_by_ied.get(ied_id, [])},
            }
        )

    # monta sub_cfg
    sub_cfgs: List[Dict] = []
    for s in subs:
        sub_id = s["sub_id"]
        sub_cfgs.append(
            {"id": sub_id, "base_dir": s["base_dir"], "ieds": ieds_by_sub.get(sub_id, [])}
        )

    return sub_cfgs


def get_active_cfg_id(engine: Engine) -> int | None:
    """Retorna o cfg_id ativo (dim_config_snapshot) se existir; senão None."""
    try:
        with engine.connect() as c:
            row = c.execute(
                text("""
                SELECT cfg_id
                  FROM osci.dim_config_snapshot
                 WHERE effective_to IS NULL
              ORDER BY effective_from DESC
                 LIMIT 1
            """)
            ).first()
            return int(row[0]) if row else None
    except Exception:
        return None


# =============================================================================
# Orquestração – usando o banco como fonte de verdade
# =============================================================================

def main():
    print(f'Inicio em: {datetime.now(tz=TZ).isoformat()}\n')
    engine = get_pg_engine()

    # carrega configuração runtime do banco
    subs_cfg = load_runtime_config_from_db(engine)
    if not subs_cfg:
        raise RuntimeError("Nenhuma subestação encontrada em osci.def_subestacao_ied / osci.def_ied.")

    cfg_id = get_active_cfg_id(engine)  # pode ser None se você não usa snapshot

    for sub in subs_cfg:
        sub_id = sub["id"]

        # 1) Pré-ingestão
        df_scan = scan_substation(sub)

        if WRITE_CSV:
            (HERE / SCAN_CSV_TPL.format(sub=sub_id)).write_text(
                df_scan.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
            )

        # 1b) grava no Postgres (atualiza/insere o estado do scan)
        write_scan_df_to_pg(df_scan, sub_id, cfg_id, engine)

        if df_scan is None or df_scan.empty:
            continue

        # 2) Decide quais oscilografias devem ser analisadas (processar/reprocessar)
        df_scan2 = df_scan.copy()

        # garante colunas usadas na comparação (caso o scan mude no futuro)
        for col in ("size_cfg", "size_dat", "size_hdr", "size_inf"):
            if col not in df_scan2.columns:
                df_scan2[col] = None

        needs_flags: List[bool] = []
        for _, r in df_scan2.iterrows():
            ied_id = r.get("ied")
            stem = r.get("stem")

            # Se não está íntegra agora, não analisa.
            if not bool(r.get("integrity_ok", False)):
                needs_flags.append(False)
                continue

            scan_row = {
                "size_cfg": r.get("size_cfg"),
                "size_dat": r.get("size_dat"),
                "size_hdr": r.get("size_hdr"),
                "size_inf": r.get("size_inf"),
                "received_ts": r.get("received_ts"),
            }

            already_ok = was_already_processed_and_integral(
                engine=engine,
                sub_id=sub_id,
                ied_id=ied_id,
                stem=stem,
            )


            # Se já foi processada e continua igual/íntegra, não reprocessa.
            needs_flags.append(not already_ok)

        df_scan2["needs_processing"] = needs_flags

        df_to_analyze = df_scan2[
            (df_scan2["integrity_ok"] == True) & (df_scan2["needs_processing"] == True)
        ].copy()

        if df_to_analyze.empty:
            continue

        # limpeza preventiva (reprocessamento)
        for _, r in df_to_analyze.iterrows():
            purge_digital_triggers_for_stem(engine, sub_id, r["ied"], r["stem"])

        # 3) Análise de digitais (somente os que precisam)
        df_analysis = analyze_integral_packages(sub, df_to_analyze)

        if WRITE_CSV:
            (HERE / ANALYSIS_CSV_TPL.format(sub=sub_id)).write_text(
                df_analysis.to_csv(index=False, encoding="utf-8"), encoding="utf-8"
            )

        # 3b) grava no Postgres
        write_analysis_df_to_pg(df_analysis, sub_id, engine, cfg_id=cfg_id)
        print(f'Fim em: {datetime.now(tz=TZ).isoformat()}\n')


if __name__ == "__main__":
    main()
