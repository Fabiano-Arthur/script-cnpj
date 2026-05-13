"""
Carrega TODAS as competências de CNPJ em PostgreSQL, num loop.

Lê os arquivos baixados pelo sync_months.py (estrutura
<base>/YYYY-MM/dados.tar.gz) e os carrega num único banco com a coluna
`competencia` distinguindo cada mês.

- Tabelas principais (empresas, estabelecimentos, socios, simples)
  acumulam, com a coluna `competencia` como primeira coluna.
- Tabelas auxiliares (cnaes, municipios, etc.) são carregadas apenas
  uma vez — são iguais entre competências.
- Idempotente: mantém uma tabela `competencias_carregadas` e pula
  competências já processadas. Use --reload para reprocessar.

Uso:
    python bulk_load_postgres.py                       # carrega tudo
    python bulk_load_postgres.py --output-dir /dados
    python bulk_load_postgres.py --only 2024-01 2024-02
    python bulk_load_postgres.py --reload --only 2024-01

Credenciais via --dsn ou variáveis PG* (PGHOST, PGUSER, ...).
"""

import argparse
import csv
import logging
import os
import re
import sys
import tarfile
import tempfile
import zipfile
from pathlib import Path

import psycopg
from psycopg import sql

from cnpj_pipeline import (
    LOOKUP_TABLES,
    MAIN_TABLES,
    _detect_format,
    find_csvs,
    resolve_output_dir,
)

MONTH_RE = re.compile(r"^\d{4}-\d{2}$")
DOWNLOAD_FILENAME = "dados.tar.gz"
MARKER_NAME = ".downloading"
TRACKING_TABLE = "competencias_carregadas"

INDEXES = [
    ("empresas", "idx_empresas_comp_cnpj", ["competencia", "cnpj_basico"]),
    ("empresas", "idx_empresas_cnpj", ["cnpj_basico"]),
    ("estabelecimentos", "idx_estab_comp_cnpj", ["competencia", "cnpj_basico"]),
    ("estabelecimentos", "idx_estab_cnpj", ["cnpj_basico"]),
    ("estabelecimentos", "idx_estab_uf", ["uf"]),
    ("socios", "idx_socios_comp_cnpj", ["competencia", "cnpj_basico"]),
    ("socios", "idx_socios_cnpj", ["cnpj_basico"]),
    ("simples", "idx_simples_comp_cnpj", ["competencia", "cnpj_basico"]),
]

log = logging.getLogger("cnpj-bulk")


# ==============================
# DESCOBERTA DOS ARQUIVOS
# ==============================

def list_available_months(base: Path, only: list[str] | None) -> list[tuple[str, Path]]:
    found = []
    for entry in sorted(base.iterdir()):
        if not entry.is_dir() or not MONTH_RE.match(entry.name):
            continue
        if only and entry.name not in only:
            continue
        if (entry / MARKER_NAME).exists():
            log.warning("%s: download incompleto (.downloading), pulando", entry.name)
            continue
        archive = entry / DOWNLOAD_FILENAME
        if not archive.exists():
            log.warning("%s: %s não encontrado, pulando", entry.name, DOWNLOAD_FILENAME)
            continue
        found.append((entry.name, archive))
    return found


# ==============================
# SCHEMA
# ==============================

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {}.{} (
                competencia TEXT PRIMARY KEY,
                carregada_em TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """).format(sql.Identifier(schema), sql.Identifier(TRACKING_TABLE)))

        # Tabelas principais: primeira coluna = competencia.
        for name, cfg in MAIN_TABLES.items():
            cols = [sql.SQL("{} TEXT NOT NULL").format(sql.Identifier("competencia"))]
            cols += [sql.SQL("{} TEXT").format(sql.Identifier(c)) for c, _ in cfg["columns"]]
            cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(schema),
                sql.Identifier(name),
                sql.SQL(", ").join(cols),
            ))

        # Tabelas auxiliares: sem competencia.
        for name, cfg in LOOKUP_TABLES.items():
            cols = [sql.SQL("{} TEXT").format(sql.Identifier(c)) for c, _ in cfg["columns"]]
            cur.execute(sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({})").format(
                sql.Identifier(schema),
                sql.Identifier(name),
                sql.SQL(", ").join(cols),
            ))
    conn.commit()


def already_loaded(conn: psycopg.Connection, schema: str, competencia: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT 1 FROM {}.{} WHERE competencia = %s").format(
                sql.Identifier(schema), sql.Identifier(TRACKING_TABLE)
            ),
            (competencia,),
        )
        return cur.fetchone() is not None


def lookup_is_empty(conn: psycopg.Connection, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT 1 FROM {}.{} LIMIT 1").format(
            sql.Identifier(schema), sql.Identifier(table)
        ))
        return cur.fetchone() is None


# ==============================
# EXTRAÇÃO
# ==============================

def extract_archive(archive: Path, dest: Path) -> None:
    """Extrai dados.tar.gz para dest. Detecta automaticamente se o arquivo
    é tar.gz ou zip (o Nextcloud da Receita varia entre os dois formatos)."""
    log.info("  extraindo %s", archive.name)
    fmt = _detect_format(archive)
    if fmt == "gzip":
        with tarfile.open(archive, "r:gz") as tar:
            try:
                tar.extractall(path=dest, filter="data")
            except TypeError:
                tar.extractall(path=dest)
    elif fmt == "zip":
        with zipfile.ZipFile(archive, "r") as z:
            z.extractall(dest)
    else:
        raise RuntimeError(
            f"Formato desconhecido em {archive.name}. "
            "Pode estar corrompido — apague a pasta e rode sync_months.py de novo."
        )

    for zf in list(dest.rglob("*.zip")):
        with zipfile.ZipFile(zf, "r") as z:
            z.extractall(zf.parent)
        zf.unlink()


# ==============================
# COPY
# ==============================

def copy_csv(
    conn: psycopg.Connection,
    csv_path: Path,
    schema: str,
    table: str,
    base_cols: list[str],
    competencia: str | None,
) -> int:
    columns = (["competencia"] + base_cols) if competencia else base_cols
    n_base = len(base_cols)
    col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(schema, table), col_idents
    )

    rows = 0
    with open(csv_path, "r", encoding="latin1", newline="") as fh, conn.cursor() as cur:
        reader = csv.reader(fh, delimiter=";", quotechar='"')
        with cur.copy(copy_sql) as cp:
            for row in reader:
                if len(row) < n_base:
                    row = row + [""] * (n_base - len(row))
                elif len(row) > n_base:
                    row = row[:n_base]
                if competencia:
                    cp.write_row([competencia, *row])
                else:
                    cp.write_row(row)
                rows += 1
    return rows


def load_main_tables(
    conn: psycopg.Connection,
    schema: str,
    csv_dir: Path,
    competencia: str,
) -> None:
    for name, cfg in MAIN_TABLES.items():
        files = find_csvs(csv_dir, cfg["prefix"])
        if not files:
            log.warning("  sem CSVs para %s (prefixo %s)", name, cfg["prefix"])
            continue
        base_cols = [c for c, _ in cfg["columns"]]
        total = 0
        for f in files:
            total += copy_csv(conn, f, schema, name, base_cols, competencia)
        log.info("  %s: %d linhas", name, total)


def maybe_load_lookups(conn: psycopg.Connection, schema: str, csv_dir: Path) -> None:
    """Carrega as tabelas auxiliares apenas se ainda estiverem vazias."""
    for name, cfg in LOOKUP_TABLES.items():
        if not lookup_is_empty(conn, schema, name):
            continue
        files = find_csvs(csv_dir, cfg["prefix"])
        if not files:
            continue
        base_cols = [c for c, _ in cfg["columns"]]
        total = 0
        for f in files:
            total += copy_csv(conn, f, schema, name, base_cols, competencia=None)
        log.info("  %s (auxiliar): %d linhas", name, total)


# ==============================
# DELETE PARA --reload
# ==============================

def delete_competencia(conn: psycopg.Connection, schema: str, competencia: str) -> None:
    log.info("  apagando dados antigos de %s", competencia)
    with conn.cursor() as cur:
        for name in MAIN_TABLES:
            cur.execute(
                sql.SQL("DELETE FROM {}.{} WHERE competencia = %s").format(
                    sql.Identifier(schema), sql.Identifier(name)
                ),
                (competencia,),
            )
        cur.execute(
            sql.SQL("DELETE FROM {}.{} WHERE competencia = %s").format(
                sql.Identifier(schema), sql.Identifier(TRACKING_TABLE)
            ),
            (competencia,),
        )
    conn.commit()


# ==============================
# ÍNDICES E ANALYZE
# ==============================

def create_indexes(conn: psycopg.Connection, schema: str) -> None:
    log.info("Criando índices (IF NOT EXISTS)")
    with conn.cursor() as cur:
        for table, idx, cols in INDEXES:
            cur.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(idx),
                sql.Identifier(schema, table),
                sql.SQL(", ").join(sql.Identifier(c) for c in cols),
            ))
    conn.commit()


def analyze_all(conn: psycopg.Connection, schema: str) -> None:
    log.info("ANALYZE")
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for name in list(MAIN_TABLES) + list(LOOKUP_TABLES) + [TRACKING_TABLE]:
                cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(schema, name)))
    finally:
        conn.autocommit = False


# ==============================
# CLI
# ==============================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dsn", help="DSN do Postgres (postgresql://...). Default: variáveis PG*.")
    p.add_argument("--schema", default="cnpj", help="Schema de destino (default: cnpj).")
    p.add_argument("--output-dir", type=Path, help="Pasta base com YYYY-MM/dados.tar.gz.")
    p.add_argument("--no-prompt", action="store_true")
    p.add_argument("--only", nargs="+", help="Carregar só estas competências (YYYY-MM).")
    p.add_argument(
        "--reload",
        action="store_true",
        help="Recarrega competências mesmo se já marcadas como carregadas.",
    )
    p.add_argument(
        "--skip-indexes",
        action="store_true",
        help="Não criar índices ao final (útil pra cargas em massa antes de criar índices manualmente).",
    )
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    base = resolve_output_dir(args)
    months = list_available_months(base, args.only)
    if not months:
        log.error("Nenhuma competência válida encontrada em %s", base)
        return 2

    log.info("Encontradas %d competência(s): %s", len(months), ", ".join(m for m, _ in months))

    dsn = args.dsn or os.environ.get("DATABASE_URL")
    log.info("Conectando ao Postgres (%s)", "DSN explícito" if dsn else "variáveis PG*")

    try:
        with psycopg.connect(dsn) if dsn else psycopg.connect() as conn:
            ensure_schema(conn, args.schema)

            processed = 0
            skipped = 0
            for comp, archive in months:
                if args.reload:
                    delete_competencia(conn, args.schema, comp)
                elif already_loaded(conn, args.schema, comp):
                    log.info("%s: já carregada, pulando", comp)
                    skipped += 1
                    continue

                log.info("=== %s ===", comp)
                with tempfile.TemporaryDirectory(prefix=f"cnpj_{comp}_") as tmp:
                    tmp_path = Path(tmp)
                    try:
                        extract_archive(archive, tmp_path)
                        maybe_load_lookups(conn, args.schema, tmp_path)
                        load_main_tables(conn, args.schema, tmp_path, comp)
                        with conn.cursor() as cur:
                            cur.execute(
                                sql.SQL("INSERT INTO {}.{} (competencia) VALUES (%s)").format(
                                    sql.Identifier(args.schema),
                                    sql.Identifier(TRACKING_TABLE),
                                ),
                                (comp,),
                            )
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise
                processed += 1
                log.info("%s: OK", comp)

            if not args.skip_indexes:
                create_indexes(conn, args.schema)
            analyze_all(conn, args.schema)

            log.info("---")
            log.info("Processadas: %d | Puladas: %d", processed, skipped)
    except psycopg.Error as e:
        log.error("Erro do Postgres: %s", e)
        return 1
    except KeyboardInterrupt:
        log.warning("Interrompido. A competência em andamento foi revertida; "
                    "as anteriores estão preservadas.")
        return 130

    return 0


if __name__ == "__main__":
    sys.exit(main())
