from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
from collections import defaultdict
from pathlib import Path
import subprocess
import shutil

# ======== CONFIGURE AQUI ========
PG_USER = "postgres"
PG_PASSWORD = "admin"
PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "oscilografias_v0"

# Use ["public"] para travar; use ["*"] para auto-descobrir (exclui schemas do sistema)
SCHEMAS = ["*"]

OUTPUT_MMD = Path("schema_erd.mmd")
EXPORT_PNG = True
OUTPUT_PNG = Path("schema_erd.png")
# ================================

url = URL.create(
    "postgresql+psycopg2",
    username=PG_USER,
    password=PG_PASSWORD,
    host=PG_HOST,
    port=PG_PORT,
    database=PG_DB,
)
engine = create_engine(url, future=True)
insp = inspect(engine)

def mermaid_escape(name: str) -> str:
    ok = all(c.isalnum() or c == "_" for c in name)
    return name if ok else f'"{name}"'

def table_id(schema: str, table: str) -> str:
    return mermaid_escape(f"{schema}.{table}")

# --------- Descoberta de schemas ----------
if SCHEMAS == ["*"]:
    all_schemas = [s for s in insp.get_schema_names()
                   if s not in ("pg_catalog", "information_schema")]
    SCHEMAS = sorted(all_schemas)

# --------- Coletas ----------
columns = defaultdict(list)   # (schema, name) -> [(col, type, nullable, default, ispk)]
pks = defaultdict(list)       # (schema, name) -> [col]
fks = []                      # (src_schema, src_tbl, [src_cols], tgt_schema, tgt_tbl, [tgt_cols], fk_name)

found_objects = []            # para diagnóstico

# pega materialized views via catálogo
def get_matviews(schema: str):
    with engine.connect() as conn:
        rows = conn.execute(
            text("""
                SELECT matviewname
                FROM pg_catalog.pg_matviews
                WHERE schemaname = :schema
                ORDER BY matviewname
            """), {"schema": schema}
        ).fetchall()
    return [r[0] for r in rows]

for schema in SCHEMAS:
    tables = insp.get_table_names(schema=schema) or []
    views = insp.get_view_names(schema=schema) or []
    matviews = get_matviews(schema) or []

    for kind, names in (("table", tables), ("view", views), ("matview", matviews)):
        for name in names:
            full = (schema, name)
            found_objects.append((schema, kind, name))

            # colunas (funciona para tabelas e views)
            for col in insp.get_columns(name, schema=schema):
                columns[full].append((
                    col.get("name"),
                    str(col.get("type")),
                    bool(col.get("nullable", True)),
                    col.get("default"),
                    False
                ))

            # PK apenas para tabelas (views normalmente não têm PK real)
            if kind == "table":
                pk_info = insp.get_pk_constraint(name, schema=schema) or {}
                for c in (pk_info.get("constrained_columns") or []):
                    pks[full].append(c)
                # marca PKs
                if pks[full]:
                    for i, (cname, typ, nullable, default, _ispk) in enumerate(columns[full]):
                        if cname in pks[full]:
                            columns[full][i] = (cname, typ, nullable, default, True)

            # FKs só em tabelas (em geral)
            if kind == "table":
                for fk in insp.get_foreign_keys(name, schema=schema):
                    src_cols = fk.get("constrained_columns", []) or []
                    tgt_schema = fk.get("referred_schema") or schema
                    tgt_table = fk.get("referred_table")
                    tgt_cols = fk.get("referred_columns", []) or []
                    fks.append((schema, name, src_cols, tgt_schema, tgt_table, tgt_cols, fk.get("name", "")))

# --------- Gera Mermaid ---------
lines = ["erDiagram"]

if not columns:
    # Ajuda visual direta no .mmd para indicar que não há objetos
    lines.append('  %% Nenhuma tabela/view encontrada nos schemas selecionados.')
else:
    for (schema, name), cols in sorted(columns.items()):
        tid = table_id(schema, name)
        lines.append(f"  {tid} {{")
        for cname, typ, nullable, default, ispk in cols:
            mm_type = "text"
            badges = []
            if ispk: badges.append("PK")
            if default not in (None, "null"): badges.append("DEF")
            if not nullable: badges.append("NN")
            tag = " ".join(badges)
            col_name = mermaid_escape(cname or "")
            lines.append(f"    {mm_type} {col_name}" + (f" {tag}" if tag else ""))
        lines.append("  }")

    for (s_s, s_t, s_cols, t_s, t_t, t_cols, fkname) in fks:
        src = table_id(s_s, s_t)
        tgt = table_id(t_s, t_t)
        label = fkname or ",".join(s_cols) or "fk"
        lines.append(f"  {src} }}o--|| {tgt} : {mermaid_escape(label)}")

OUTPUT_MMD.write_text("\n".join(lines), encoding="utf-8")
print(f"Arquivo Mermaid salvo em: {OUTPUT_MMD.resolve()}")

# Resumo de diagnóstico
print("\n=== Diagnóstico ===")
if SCHEMAS:
    print("Schemas considerados:", ", ".join(SCHEMAS))
else:
    print("Nenhum schema considerado.")
if found_objects:
    by_schema = defaultdict(list)
    for s, kind, name in found_objects:
        by_schema[s].append((kind, name))
    for s, items in by_schema.items():
        kinds = {"table":0,"view":0,"matview":0}
        for k,_ in items: kinds[k]+=1
        print(f"- {s}: {kinds['table']} tabelas, {kinds['view']} views, {kinds['matview']} matviews")
else:
    print("Nenhuma tabela/view/materialized view encontrada.")

# --------- Exporta PNG (robusto) ---------
def run_mermaid_export(input_mmd: Path, output_png: Path) -> bool:
    candidates = ["mmdc", "mmdc.cmd",
                  str(Path("node_modules")/".bin"/"mmdc"),
                  str(Path("node_modules")/".bin"/"mmdc.cmd")]
    for exe in candidates:
        found = shutil.which(exe)
        if found:
            try:
                subprocess.run([found, "-i", str(input_mmd), "-o", str(output_png)], check=True)
                return True
            except subprocess.CalledProcessError:
                pass
    for npx in ("npx", "npx.cmd"):
        found = shutil.which(npx)
        if found:
            try:
                subprocess.run([found, "-y", "@mermaid-js/mermaid-cli",
                                "-i", str(input_mmd), "-o", str(output_png)], check=True)
                return True
            except subprocess.CalledProcessError:
                pass
    return False

if EXPORT_PNG:
    if run_mermaid_export(OUTPUT_MMD, OUTPUT_PNG):
        print(f"PNG exportado em: {OUTPUT_PNG.resolve()}")
    else:
        print(
            "Aviso: não consegui executar o Mermaid CLI.\n"
            f'Rode manualmente: npx -y @mermaid-js/mermaid-cli -i "{OUTPUT_MMD}" -o "{OUTPUT_PNG}"\n'
            "Ou defina EXPORT_PNG=False para gerar apenas o .mmd."
        )
