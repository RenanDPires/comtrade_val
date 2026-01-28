    from __future__ import annotations
    from dataclasses import dataclass
    from pathlib import Path
    from typing import Dict, List, Iterable
    import json
    import re
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    import numpy as np
    import pandas as pd
    from comtrade import Comtrade

    # =====================================================================
    # Configurações básicas
    # =====================================================================

    HERE = Path(__file__).resolve().parent
    CONFIG_PATH = HERE / "config.json"
    SCAN_CSV_TPL = "scan_{sub}.csv"
    ANALYSIS_CSV_TPL = "analysis_{sub}.csv"

    VALID_EXTS = {".cfg", ".dat", ".hdr", ".inf"}
    TZ = ZoneInfo("America/Sao_Paulo")

    # =====================================================================
    # Helpers (pré-ingestão)
    # =====================================================================

    def norm_ext(ext: str) -> str:
        e = ext.lower()
        return e if e.startswith(".") else f".{e}"

    def stem_of(p: Path) -> str:
        return p.with_suffix("").name

    def list_files_se(base_dir: Path) -> Iterable[Path]:
        pats = ["*.cfg","*.CFG","*.dat","*.DAT","*.hdr","*.HDR","*.inf","*.INF"]
        for pat in pats:
            yield from base_dir.glob(pat)

    # =====================================================================
    # Extrair datetime base do STEM
    # =====================================================================

    _STEM_TS_RE = re.compile(r"-(\d{8})-(\d{6,9})-OSC", re.IGNORECASE)
    # casa ...-YYYYMMDD-HHMMSS[fff]-OSC   (fff opcional de milissegundos)

    def stem_base_datetime(stem: str) -> datetime | None:
        """
        Extrai datetime base do stem no fuso America/Sao_Paulo.
        Ex.: ...-20240901-154208090-OSC  -> 2024-09-01 15:42:08.090 -03:00
            ...-20240901-154208-OSC     -> 2024-09-01 15:42:08.000 -03:00
        """
        m = _STEM_TS_RE.search(stem)
        if not m:
            return None
        ymd, hmsx = m.group(1), m.group(2)
        year = int(ymd[0:4]); month = int(ymd[4:6]); day = int(ymd[6:8])
        hour = int(hmsx[0:2]); minute = int(hmsx[2:4]); second = int(hmsx[4:6])
        # milissegundos se houver 9 dígitos; se 6, assume 0
        ms = int(hmsx[6:9]) if len(hmsx) >= 9 else 0
        try:
            return datetime(year, month, day, hour, minute, second, ms * 1000, tzinfo=TZ)
        except Exception:
            return None

    # =====================================================================
    # Estruturas
    # =====================================================================

    @dataclass
    class FileInfo:
        size: int
        mtime: float

    @dataclass
    class Package:
        substation: str
        ied: str                 # id do JSON
        directory: str
        stem: str
        expected: List[str]      # extensões com ponto
        present: List[str]
        missing: List[str]
        zero_kb: List[str]
        max_arrival_skew_s: float
        received_ts: float
        sizes: Dict[str, int | None]

        @property
        def integrity_ok(self) -> bool:
            # íntegra somente se não houver faltantes nem 0KB entre os esperados
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

    # =====================================================================
    # Pré‑ingestão
    # =====================================================================

    def scan_substation(sub_cfg: dict) -> pd.DataFrame:
        sub_id = sub_cfg["id"]
        base_dir = Path(sub_cfg["base_dir"])

        packages: List[Package] = []

        for ied in sub_cfg.get("ieds", []):
            ied_id = ied["id"]
            expected = [norm_ext(e) for e in ied.get("expected_files", [])]

            # só arquivos deste IED (id no nome)
            files = [p for p in list_files_se(base_dir) if ied_id in p.name]

            # agrupar por (pasta, stem)
            groups: Dict[tuple, List[Path]] = {}
            for p in files:
                key = (str(p.parent), stem_of(p))
                groups.setdefault(key, []).append(p)

            # montar pacotes
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

                packages.append(Package(
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
                    sizes=sizes
                ))

        df = pd.DataFrame([p.to_row() for p in packages]).sort_values(
            ["integrity_ok", "ied", "stem"], ascending=[False, True, True]
        )
        size_cols = [c for c in df.columns if c.startswith("size_")]
        ordered = [c for c in df.columns if not c.startswith("size_")] + size_cols
        return df[ordered]

    # =====================================================================
    # Análise de digitais (COMTRADE) por INDEX do JSON
    # =====================================================================

    def _load_comtrade_digitals(stem_path: Path) -> tuple[pd.DataFrame, Dict, List[str]]:
        """
        Carrega CFG/DAT e retorna:
        - df_digitais: DataFrame dos canais digitais (0/1), indexado em segundos.
        - meta: dict simples com caminhos e contagem.
        - chosen_names: lista de nomes (strings) por índice 1-based (ordem do arquivo).
        """
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

        # tempo (segundos) – tolerante
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

        # nomes conforme versão
        ids    = getattr(rec, "status_channel_ids", None)    or getattr(rec, "status_ids", None)
        names  = getattr(rec, "status_channel_names", None)  or getattr(rec, "status_names", None)
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
            raw_id   = _get(ids, i)    if ids    is not None else None
            raw_name = _get(names, i)  if names  is not None else None
            raw_lab  = _get(labels, i) if labels is not None else None
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
        """
        Para cada pacote íntegro, analisa os digitais declarados no JSON do respectivo IED,
        localizando por INDEX 1-based. 'id_hint' é usado apenas como conferência.
        - 'first_rise_dt' é a data/hora absoluta (America/Sao_Paulo) baseada no timestamp do stem.
        """
        rows: List[Dict] = []
        sub_id = sub_cfg["id"]

        # indexa lista de canais digitais por IED (index + id_hint + description)
        digitals_by_ied: Dict[str, List[Dict]] = {}
        for ied in sub_cfg.get("ieds", []):
            dlist = ied.get("channels", {}).get("digitals", [])
            digitals_by_ied[ied["id"]] = [d for d in dlist if "index" in d]

        # somente pacotes íntegros desta SE
        df_valid = df_scan[(df_scan["substation"] == sub_id) & (df_scan["integrity_ok"] == True)]

        for _, r in df_valid.iterrows():
            ied_id = r["ied"]
            stem = r["stem"]
            stem_path = Path(r["directory"]) / stem

            try:
                df_dig, meta, chosen_names = _load_comtrade_digitals(stem_path)
            except Exception as e:
                rows.append({
                    "substation": sub_id, "ied": ied_id, "stem": stem,
                    "status": "load_error", "error": str(e)
                })
                continue

            n_channels = len(chosen_names)
            base_dt = stem_base_datetime(stem)  # datetime com fuso; pode ser None

            for ch in digitals_by_ied.get(ied_id, []):
                idx = int(ch["index"])             # 1-based no JSON
                id_hint = ch.get("id_hint")
                desc = ch.get("description")

                if not (1 <= idx <= n_channels):
                    rows.append({
                        "substation": sub_id, "ied": ied_id, "stem": stem,
                        "channel_index": idx, "channel_name": None,
                        "id_hint": id_hint, "description": desc,
                        "triggered": False, "first_rise_s": None, "first_rise_dt": None,
                        "n_rises": 0, "note": "index_out_of_range"
                    })
                    continue

                col_name = chosen_names[idx - 1]

                # Checagem opcional do id_hint: compara prefixo antes de '|'
                note = ""
                if id_hint:
                    cfg_prefix = (col_name.split('|', 1)[0] or "").strip()
                    if id_hint.strip().upper() not in cfg_prefix.upper():
                        note = f"id_hint_mismatch(cfg='{cfg_prefix}', hint='{id_hint}')"

                s = df_dig[col_name]
                rises = _rising_edges(s)

                first_s = (rises[0] if rises else None)
                first_dt = (base_dt + timedelta(seconds=first_s)) if (base_dt and first_s is not None) else None
                if base_dt is None:
                    note = (note + ";" if note else "") + "no_base_datetime_from_stem"

                rows.append({
                    "substation": sub_id,
                    "ied": ied_id,
                    "osc": stem,
                    "channel_index": int(idx),
                    "channel_name": col_name,   # como a lib expôs
                    "id_hint": id_hint,
                    "description": desc,
                    "triggered": bool(rises),
                    #"first_rise_s": first_s,            # mantemos em segundos (útil p/ cálculo)
                    "first_rise_dt": first_dt.isoformat() if first_dt else None,
                    "n_rises": len(rises),
                })

        return pd.DataFrame(rows)

    # =====================================================================
    # Orquestração
    # =====================================================================

    def main():
        if not CONFIG_PATH.exists():
            raise FileNotFoundError(f"config.json não encontrado em {CONFIG_PATH}")
        cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

        engine = get_engine()
        cfg_id = upsert_catalog_from_config(cfg, engine, version_tag=datetime.now().strftime("%Y%m%d_v1"))

        for sub in cfg.get("substations", []):
            # 1) Pré‑ingestão
            df_scan = scan_substation(sub)
            scan_csv = HERE / SCAN_CSV_TPL.format(sub=sub["id"])
            df_scan.to_csv(scan_csv, index=False, encoding="utf-8")

            # 1b) grava no MySQL
            write_scan_df_to_mysql(df_scan, sub["id"], cfg_id, engine)

            # 2) Análise de digitais
            df_analysis = analyze_integral_packages(sub, df_scan)
            out_csv = HERE / ANALYSIS_CSV_TPL.format(sub=sub["id"])
            df_analysis.to_csv(out_csv, index=False, encoding="utf-8")

            # 2b) grava no MySQL
            write_analysis_df_to_mysql(df_analysis, sub["id"], engine, cfg_id=cfg_id)



    # ==== MySQL helpers ====
    import os
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine

    MYSQL_URL = os.getenv("MYSQL_URL", "mysql+mysqldb://root:root@localhost:3306/osci")

    def get_engine() -> Engine:
        return create_engine(
            MYSQL_URL,
            pool_pre_ping=True,
            pool_recycle=3600,
            future=True,
        )

    def upsert_catalog_from_config(cfg: dict, engine: Engine, version_tag: str):
        """
        Lê cfg['substations'][*] e popula:
        - dim_subestacao
        - dim_ied
        - dim_ied_expected_file
        - dim_ied_digital
        - dim_config_snapshot (snapshot do JSON)
        """
        from datetime import datetime, timezone
        with engine.begin() as conn:
            # 1) snapshot (fecha o vigente se houver)
            now = datetime.now(tz=None).replace(microsecond=0)
            conn.execute(text("""
            UPDATE dim_config_snapshot
                SET effective_to = :now
            WHERE effective_to IS NULL
            """), {"now": now})
            r = conn.execute(text("""
            INSERT INTO dim_config_snapshot (version_tag, payload_json, effective_from)
            VALUES (:vtag, CAST(:payload AS JSON), :now)
            """), {"vtag": version_tag, "payload": json.dumps(cfg, ensure_ascii=False), "now": now})
            cfg_id = r.lastrowid

            for sub in cfg.get("substations", []):
                sub_id = sub["id"]
                loc = (sub.get("location") or {})
                conn.execute(text("""
                INSERT INTO dim_subestacao (sub_id, nome, municipio, uf, lat, lon, ativo)
                VALUES (:sid, :nome, :mun, :uf, :lat, :lon, 1)
                ON DUPLICATE KEY UPDATE
                    nome = VALUES(nome),
                    municipio = VALUES(municipio),
                    uf = VALUES(uf),
                    lat = VALUES(lat),
                    lon = VALUES(lon),
                    ativo = 1
                """), {
                    "sid": sub_id,
                    "nome": loc.get("name") or sub_id,
                    "mun": sub.get("municipio"),
                    "uf":  sub.get("uf"),
                    "lat": loc.get("lat"),
                    "lon": loc.get("lon"),
                })

                for ied in sub.get("ieds", []):
                    conn.execute(text("""
                    INSERT INTO dim_ied (ied_id, sub_id, base_dir, descricao)
                    VALUES (:iid, :sid, :bdir, :desc)
                    ON DUPLICATE KEY UPDATE
                        sub_id = VALUES(sub_id),
                        base_dir = VALUES(base_dir),
                        descricao = VALUES(descricao)
                    """), {
                        "iid": ied["id"], "sid": sub_id,
                        "bdir": ied.get("base_dir"),
                        "desc": ied.get("description"),
                    })

                    # expected files
                    expected = [e.lower() if e.startswith(".") else ("." + e.lower())
                                for e in ied.get("expected_files", [])]
                    for ext in expected:
                        conn.execute(text("""
                        INSERT IGNORE INTO dim_ied_expected_file (ied_id, ext)
                        VALUES (:iid, :ext)
                        """), {"iid": ied["id"], "ext": ext})

                    # digitals
                    for d in (ied.get("channels", {}).get("digitals", []) or []):
                        if "index" not in d: 
                            continue
                        conn.execute(text("""
                        INSERT INTO dim_ied_digital (ied_id, idx1, id_hint, description)
                        VALUES (:iid, :idx1, :hint, :desc)
                        ON DUPLICATE KEY UPDATE
                            id_hint = VALUES(id_hint),
                            description = VALUES(description)
                        """), {
                            "iid": ied["id"],
                            "idx1": int(d["index"]),
                            "hint": d.get("id_hint"),
                            "desc": d.get("description"),
                        })
        return cfg_id

    def write_scan_df_to_mysql(df_scan: pd.DataFrame, sub_id: str, cfg_id: int, engine: Engine):
        """
        Grava df do scan_substation em osci.fato_scan_pkg.
        Mantém UNIQUE (sub_id, ied_id, stem) com upsert.
        """
        def parse_sizes(row):
            return {
                "size_cfg": row.get("size_cfg"),
                "size_dat": row.get("size_dat"),
                "size_hdr": row.get("size_hdr"),
                "size_inf": row.get("size_inf"),
            }

        with engine.begin() as conn:
            sql = text("""
            INSERT INTO fato_scan_pkg
                (sub_id, ied_id, directory, stem,
                expected, present, missing, zero_kb,
                size_cfg, size_dat, size_hdr, size_inf,
                max_arrival_skew_s, received_ts, integrity_ok, cfg_id)
            VALUES
                (:sub_id, :ied_id, :directory, :stem,
                CAST(:expected AS JSON), CAST(:present AS JSON), CAST(:missing AS JSON), CAST(:zero_kb AS JSON),
                :size_cfg, :size_dat, :size_hdr, :size_inf,
                :skew, :rcv, :ok, :cfg_id)
            ON DUPLICATE KEY UPDATE
                directory = VALUES(directory),
                expected = VALUES(expected),
                present  = VALUES(present),
                missing  = VALUES(missing),
                zero_kb  = VALUES(zero_kb),
                size_cfg = VALUES(size_cfg),
                size_dat = VALUES(size_dat),
                size_hdr = VALUES(size_hdr),
                size_inf = VALUES(size_inf),
                max_arrival_skew_s = VALUES(max_arrival_skew_s),
                received_ts = VALUES(received_ts),
                integrity_ok = VALUES(integrity_ok),
                cfg_id = VALUES(cfg_id)
            """)
            for _, r in df_scan.iterrows():
                sizes = parse_sizes(r)
                conn.execute(sql, {
                    "sub_id": sub_id,
                    "ied_id": r["ied"],
                    "directory": r["directory"],
                    "stem": r["stem"],
                    "expected": json.dumps((r["expected"] or "").split(",")) if isinstance(r["expected"], str) else json.dumps(r["expected"]),
                    "present":  json.dumps((r["present"]  or "").split(",")) if isinstance(r["present"],  str) else json.dumps(r["present"]),
                    "missing":  json.dumps((r["missing"]  or "").split(",")) if isinstance(r["missing"],  str) else json.dumps(r["missing"]),
                    "zero_kb":  json.dumps((r["zero_kb"]  or "").split(",")) if isinstance(r["zero_kb"],  str) else json.dumps(r["zero_kb"]),
                    "size_cfg": sizes["size_cfg"], "size_dat": sizes["size_dat"],
                    "size_hdr": sizes["size_hdr"], "size_inf": sizes["size_inf"],
                    "skew": float(r.get("max_arrival_skew_s", 0) or 0),
                    "rcv":  float(r.get("received_ts", 0) or 0),
                    "ok":   int(bool(r.get("integrity_ok", False))),
                    "cfg_id": cfg_id
                })

    def write_analysis_df_to_mysql(df_analysis: pd.DataFrame, sub_id: str, engine: Engine, cfg_id: int | None = None):
        """
        Grava df do analyze_integral_packages em osci.fato_analysis_digital.
        Unique (sub_id, ied_id, stem, channel_index) com upsert.
        """
        with engine.begin() as conn:
            sql = text("""
            INSERT INTO fato_analysis_digital
                (sub_id, ied_id, stem, channel_index, channel_name, id_hint, description,
                triggered, first_rise_dt, n_rises, cfg_id)
            VALUES
                (:sub_id, :ied_id, :stem, :idx, :name, :hint, :desc,
                :trig, :frdt, :nr, :cfg_id)
            ON DUPLICATE KEY UPDATE
                channel_name = VALUES(channel_name),
                id_hint = VALUES(id_hint),
                description = VALUES(description),
                triggered = VALUES(triggered),
                first_rise_dt = VALUES(first_rise_dt),
                n_rises = VALUES(n_rises),
                cfg_id = VALUES(cfg_id)
            """)
            for _, r in df_analysis.iterrows():
                frdt = r.get("first_rise_dt")
                # r["first_rise_dt"] veio como string isoformat() no seu código
                conn.execute(sql, {
                    "sub_id": sub_id,
                    "ied_id": r["ied"],
                    "stem": r["osc"],
                    "idx":  int(r["channel_index"]),
                    "name": r.get("channel_name"),
                    "hint": r.get("id_hint"),
                    "desc": r.get("description"),
                    "trig": int(bool(r.get("triggered", False))),
                    "frdt": None if not frdt else frdt.replace("Z","").replace("T"," "),
                    "nr":   int(r.get("n_rises", 0)),
                    "cfg_id": cfg_id
                })

    if __name__ == "__main__":
        main()