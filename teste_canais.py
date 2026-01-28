# teste_canais.py
from pathlib import Path
from typing import List, Dict, Any
import numpy as np
import pandas as pd
from comtrade import Comtrade

# >>> ajuste para o seu stem (sem extensão)
STEM = Path(r"C:\Users\renan\OneDrive\Área de Trabalho\WaveForms_1\RJTRIO_PL1_UPD1-20250722-005809457-OSC")

def _find_case_insensitive(stem: Path, exts: List[str]) -> Path | None:
    for e in exts:
        p = stem.with_suffix(e)
        if p.exists():
            return p
    return None

def _get_attr(obj: Any, names: List[str], default=None):
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return default

def _safe_list(x, n):
    if x is None:
        return [None] * n
    try:
        return list(x)
    except Exception:
        return [None] * n

def load_comtrade(stem: Path):
    cfg = _find_case_insensitive(stem, [".cfg", ".CFG"])
    dat = _find_case_insensitive(stem, [".dat", ".DAT"])
    hdr = _find_case_insensitive(stem, [".hdr", ".HDR"])
    if not cfg or not dat:
        raise FileNotFoundError(f"Não achei CFG/DAT para {stem}")

    rec = Comtrade()
    rec.load(str(cfg), str(dat))  # pode falhar se o DAT estiver truncado

    # Tempo: prioriza vetor time; se não houver, tenta estimar por fs/contagem
    t = _get_attr(rec, ["time"], None)
    if t is not None and len(t):
        t = np.asarray(t, dtype=float)
    else:
        # tenta achar fs em rec.cfg ou rec
        fs = _get_attr(rec, ["fs", "frequency"], None)
        if fs is None and hasattr(rec, "cfg"):
            fs = _get_attr(rec.cfg, ["fs", "frequency"], None)
            # algumas versões guardam amostragens como (rate, samples) em sample_rates
            sr = _get_attr(rec.cfg, ["sample_rates"], None)
            if fs is None and sr:
                try:
                    fs = float(sr[0][0])
                except Exception:
                    fs = None
        # tamanho pelo número de digitais (ou analógicos)
        n = 0
        if _get_attr(rec, ["status"], None):
            n = len(rec.status[0])
        elif _get_attr(rec, ["analog"], None):
            n = len(rec.analog[0])
        if fs and n:
            t = np.arange(n, dtype=float) / float(fs)
        else:
            t = np.arange(n, dtype=float)

    analog_count = int(_get_attr(rec, ["analog_count"], 0) or 0)
    status_count = int(_get_attr(rec, ["status_count"], 0) or 0)

    # nomes analógicos (tenta várias variantes / e no cfg também)
    a_ids   = _get_attr(rec, ["analog_channel_ids", "analog_ids"], None)
    a_names = _get_attr(rec, ["analog_channel_names", "analog_names"], None)
    if (a_ids is None or not len(a_ids)) and hasattr(rec, "cfg"):
        a_ids   = _get_attr(rec.cfg, ["analog_channel_ids", "analog_ids"], None)
        a_names = _get_attr(rec.cfg, ["analog_channel_names", "analog_names"], None)
    a_units = _get_attr(rec, ["analog_units"], None) or (_get_attr(rec.cfg, ["analog_units"], None) if hasattr(rec, "cfg") else None)
    a_ids   = _safe_list(a_ids, analog_count)
    a_names = _safe_list(a_names, analog_count)
    a_units = _safe_list(a_units, analog_count)

    # nomes digitais (tenta variantes / e no cfg)
    d_ids   = _get_attr(rec, ["status_channel_ids", "status_ids"], None)
    d_names = _get_attr(rec, ["status_channel_names", "status_names"], None)
    d_labs  = _get_attr(rec, ["status_channel_labels"], None)
    if ((d_ids is None or not len(d_ids)) or (d_names is None or not len(d_names))) and hasattr(rec, "cfg"):
        d_ids   = d_ids   or _get_attr(rec.cfg, ["status_channel_ids", "status_ids"], None)
        d_names = d_names or _get_attr(rec.cfg, ["status_channel_names", "status_names"], None)
        d_labs  = d_labs  or _get_attr(rec.cfg, ["status_channel_labels"], None)
    d_ids   = _safe_list(d_ids, status_count)
    d_names = _safe_list(d_names, status_count)
    d_labs  = _safe_list(d_labs, status_count)

    # monta dataframes de inventário
    dfA = pd.DataFrame([{
        "index": i+1,
        "id": a_ids[i],
        "name": a_names[i],
        "unit": a_units[i]
    } for i in range(analog_count)])

    dfD = pd.DataFrame([{
        "index": i+1,
        "id": d_ids[i],
        "name": d_names[i],
        "label": d_labs[i]
    } for i in range(status_count)])

    meta = {
        "cfg": str(cfg),
        "dat": str(dat),
        "hdr": str(hdr) if hdr else None,
        "station_name": _get_attr(rec, ["station_name"], None),
        "rec_dev_id": _get_attr(rec, ["rec_dev_id"], None),
        "analog_count": analog_count,
        "status_count": status_count,
        "t_start": float(t[0]) if len(t) else None,
        "t_end": float(t[-1]) if len(t) else None,
        "duration_s": float(t[-1] - t[0]) if len(t) else 0.0,
    }
    return meta, dfA, dfD

def main():
    meta, dfA, dfD = load_comtrade(STEM)
    print("=== META ===")
    for k,v in meta.items():
        print(f"{k}: {v}")

    print("\n=== Canais ANALÓGICOS ===")
    print(dfA.to_string(index=False) if not dfA.empty else "(nenhum)")

    print("\n=== Canais DIGITAIS ===")
    print(dfD.to_string(index=False) if not dfD.empty else "(nenhum)")

    # salva CSVs ao lado do arquivo
    out_dir = STEM.parent
    dfA.to_csv(out_dir / f"{STEM.name}__analogs.csv", index=False, encoding="utf-8")
    dfD.to_csv(out_dir / f"{STEM.name}__digitals.csv", index=False, encoding="utf-8")
    print(f"\n[i] CSVs salvos em: {out_dir}")

if __name__ == "__main__":
    main()
