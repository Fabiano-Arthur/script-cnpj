"""
Pipeline ETL dos dados públicos de CNPJ para PostgreSQL.

Reaproveita o download e a extração do cnpj_pipeline.py, mas carrega
via COPY FROM STDIN (~10x mais rápido que INSERT em lote) num banco
Postgres existente.

Credenciais:
    Passe --dsn 'postgresql://user:pass@host:5432/db'
    ou use as variáveis padrão libpq: PGHOST, PGPORT, PGUSER,
    PGPASSWORD, PGDATABASE.

Uso:
    python cnpj_to_postgres.py                          # último mês fechado
    python cnpj_to_postgres.py 2026-04 --schema cnpj
    python cnpj_to_postgres.py --csv-dir cnpj_2026_04   # CSVs já extraídos
    python cnpj_to_postgres.py --only empresas socios
"""

import argparse
import csv
import logging
import os
import sys
from pathlib import Path

import psycopg
from psycopg import sql

from cnpj_pipeline import (
    ALL_TABLES,
    download_competencia,
    extract_tar_gz,
    find_csvs,
    last_closed_month,
    resolve_output_dir,
    unzip_inner_zips,
    validate_competencia,
)


def find_existing_archive(download_dir: Path, ano_mes: str) -> Path | None:
    """Procura um arquivo já baixado pra esta competência, em qualquer
    nome plausível (cnpj_<ano_mes>.tar.gz, dados.tar.gz, .zip etc.)."""
    candidates = [
        download_dir / f"cnpj_{ano_mes}.tar.gz",
        download_dir / f"cnpj_{ano_mes}.zip",
        download_dir / "dados.tar.gz",
        download_dir / "dados.zip",
    ]
    for p in candidates:
        if p.exists() and p.stat().st_size > 100 * 1024 * 1024:  # >100MB pra evitar HTML de erro
            return p
    return None

BATCH_FLUSH = 50_000  # linhas entre logs de progresso

INDEXES = [
    ("empresas", "idx_empresas_cnpj", ["cnpj_basico"]),
    ("estabelecimentos", "idx_estab_cnpj", ["cnpj_basico"]),
    ("estabelecimentos", "idx_estab_uf", ["uf"]),
    ("estabelecimentos", "idx_estab_municipio", ["municipio"]),
    ("socios", "idx_socios_cnpj", ["cnpj_basico"]),
    ("simples", "idx_simples_cnpj", ["cnpj_basico"]),
]

log = logging.getLogger("cnpj-pg")


# ==============================
# SCHEMA
# ==============================

def ensure_schema(conn: psycopg.Connection, schema: str) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
    conn.commit()


def create_tables(
    conn: psycopg.Connection,
    schema: str,
    tables: dict,
    unlogged: bool,
) -> None:
    """Recria as tabelas (DROP + CREATE). Todas as colunas como TEXT —
    conversões para numérico/data ficam para views ou ALTERs posteriores,
    para não travar a carga em valores fora do padrão."""
    table_kw = "UNLOGGED TABLE" if unlogged else "TABLE"
    with conn.cursor() as cur:
        for name, cfg in tables.items():
            fq = sql.Identifier(schema, name)
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(fq))
            cols = sql.SQL(", ").join(
                sql.SQL("{} TEXT").format(sql.Identifier(col)) for col, _ in cfg["columns"]
            )
            cur.execute(sql.SQL("CREATE {kw} {tbl} ({cols})").format(
                kw=sql.SQL(table_kw),
                tbl=fq,
                cols=cols,
            ))
    conn.commit()


# ==============================
# CARGA VIA COPY
# ==============================

def copy_file_to_table(
    conn: psycopg.Connection,
    csv_path: Path,
    schema: str,
    table: str,
    columns: list[str],
) -> int:
    """Carrega um CSV no Postgres via COPY ... FROM STDIN, lendo via
    csv.reader (tolera arquivos com colunas a menos/mais)."""
    fq = sql.Identifier(schema, table)
    col_idents = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    copy_sql = sql.SQL("COPY {} ({}) FROM STDIN").format(fq, col_idents)

    n_cols = len(columns)
    rows_loaded = 0

    with open(csv_path, "r", encoding="latin1", newline="") as fh, conn.cursor() as cur:
        reader = csv.reader(fh, delimiter=";", quotechar='"')
        with cur.copy(copy_sql) as cp:
            for row in reader:
                if len(row) < n_cols:
                    row = row + [""] * (n_cols - len(row))
                elif len(row) > n_cols:
                    row = row[:n_cols]
                # PG distingue string vazia de NULL; mantemos string vazia
                # para bater com o comportamento do SQLite original.
                cp.write_row(row)
                rows_loaded += 1
                if rows_loaded % BATCH_FLUSH == 0:
                    log.debug("    %d linhas...", rows_loaded)

    return rows_loaded


def load_table(
    conn: psycopg.Connection,
    base_dir: Path,
    schema: str,
    table: str,
    cfg: dict,
) -> None:
    files = find_csvs(base_dir, cfg["prefix"])
    if not files:
        log.warning("Nenhum arquivo encontrado para %s (prefixo %s)", table, cfg["prefix"])
        return

    columns = [c for c, _ in cfg["columns"]]
    log.info("Carregando %s.%s (%d arquivo(s))", schema, table, len(files))
    total = 0

    for file in files:
        log.info("  → %s", file.name)
        try:
            rows = copy_file_to_table(conn, file, schema, table, columns)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        log.info("     %d linhas", rows)
        total += rows

    log.info("  total em %s.%s: %d linhas", schema, table, total)


# ==============================
# ÍNDICES E PÓS-CARGA
# ==============================

def create_indexes(conn: psycopg.Connection, schema: str, only_tables: set[str]) -> None:
    log.info("Criando índices")
    with conn.cursor() as cur:
        for table, idx_name, cols in INDEXES:
            if table not in only_tables:
                continue
            cur.execute(sql.SQL("CREATE INDEX IF NOT EXISTS {} ON {} ({})").format(
                sql.Identifier(idx_name),
                sql.Identifier(schema, table),
                sql.SQL(", ").join(sql.Identifier(c) for c in cols),
            ))
    conn.commit()


def set_logged(conn: psycopg.Connection, schema: str, tables: list[str]) -> None:
    """Converte UNLOGGED → LOGGED no final, para tornar o banco
    durável depois da carga. Reescreve a tabela inteira, então é caro."""
    log.info("Convertendo tabelas para LOGGED (pode demorar)")
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(sql.SQL("ALTER TABLE {} SET LOGGED").format(sql.Identifier(schema, t)))
    conn.commit()


def analyze(conn: psycopg.Connection, schema: str, tables: list[str]) -> None:
    log.info("Rodando ANALYZE")
    # ANALYZE não pode rodar em transação no autocommit padrão; usamos autocommit.
    old_autocommit = conn.autocommit
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            for t in tables:
                cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(schema, t)))
    finally:
        conn.autocommit = old_autocommit


# ==============================
# PIPELINE
# ==============================

def run(args: argparse.Namespace) -> None:
    competencia = args.competencia or last_closed_month()
    if not args.competencia:
        log.info("Sem competência informada. Usando último mês fechado: %s", competencia)

    slug = competencia.replace("-", "_")

    # 1. Garantir CSVs disponíveis.
    if args.csv_dir:
        csv_dir = args.csv_dir
        if not csv_dir.exists():
            log.error("Diretório de CSVs não encontrado: %s", csv_dir)
            sys.exit(2)
        log.info("Usando CSVs já extraídos em %s", csv_dir)
    else:
        base = resolve_output_dir(args)
        download_dir = args.download_dir or (base / "downloads")
        csv_dir = args.work_dir or (base / "extracted" / f"cnpj_{slug}")
        for d in (download_dir, csv_dir.parent):
            d.mkdir(parents=True, exist_ok=True)

        existing = find_existing_archive(download_dir, competencia)
        if args.skip_download:
            if not existing:
                log.error(
                    "Nenhum arquivo encontrado em %s para %s. "
                    "Rode sem --skip-download para baixar.",
                    download_dir, competencia,
                )
                sys.exit(2)
            log.info("--skip-download: usando arquivo existente %s", existing.name)
            tar_file = existing
        elif existing:
            log.info("Arquivo já presente em %s, pulando download.", existing.name)
            tar_file = existing
        else:
            tar_file = download_competencia(competencia, download_dir)

        extract_tar_gz(tar_file, csv_dir)
        unzip_inner_zips(csv_dir)

    # 2. Selecionar tabelas.
    tables = ALL_TABLES if not args.only else {k: ALL_TABLES[k] for k in args.only}

    # 3. Conectar.
    dsn = args.dsn or os.environ.get("DATABASE_URL")
    log.info("Conectando ao Postgres (%s)", "DSN explícito" if dsn else "variáveis PG*")
    with psycopg.connect(dsn) if dsn else psycopg.connect() as conn:
        # COPY já é eficiente sozinho; mantemos autocommit desligado
        # e damos commit por arquivo (já feito em load_table).
        ensure_schema(conn, args.schema)
        create_tables(conn, args.schema, tables, unlogged=not args.logged)

        for name, cfg in tables.items():
            load_table(conn, csv_dir, args.schema, name, cfg)

        loaded_tables = list(tables.keys())
        create_indexes(conn, args.schema, set(loaded_tables))

        if not args.logged:
            set_logged(conn, args.schema, loaded_tables)

        analyze(conn, args.schema, loaded_tables)

    log.info("Pronto. Dados disponíveis no schema %s.", args.schema)


# ==============================
# CLI
# ==============================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "competencia",
        nargs="?",
        type=validate_competencia,
        help="Competência YYYY-MM (default: último mês fechado).",
    )
    p.add_argument("--dsn", help="DSN do Postgres (postgresql://...). Default: variáveis PG*.")
    p.add_argument("--schema", default="cnpj", help="Schema de destino (default: cnpj).")
    p.add_argument(
        "--only",
        nargs="+",
        choices=list(ALL_TABLES.keys()),
        help="Carregar apenas estas tabelas.",
    )
    p.add_argument(
        "--csv-dir",
        type=Path,
        help="Pular download/extração e usar CSVs deste diretório.",
    )
    p.add_argument(
        "--skip-download",
        action="store_true",
        help="Não baixar nada; usar o arquivo já existente em --download-dir / output-dir/downloads.",
    )
    p.add_argument(
        "--output-dir",
        type=Path,
        help="Pasta base onde criar downloads/ e extracted/<comp>/. "
             "Se omitido, o script pergunta interativamente.",
    )
    p.add_argument("--download-dir", type=Path, help="Sobrepõe a subpasta de downloads.")
    p.add_argument("--work-dir", type=Path, help="Sobrepõe a subpasta de CSVs extraídos.")
    p.add_argument("--no-prompt", action="store_true", help="Não pergunta nada; usa o diretório atual como base.")
    p.add_argument(
        "--logged",
        action="store_true",
        help="Criar tabelas LOGGED desde o início. Default: UNLOGGED durante a carga + ALTER no final (mais rápido).",
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
    try:
        run(args)
    except psycopg.Error as e:
        log.error("Erro do Postgres: %s", e)
        return 1
    except KeyboardInterrupt:
        log.warning("Interrompido pelo usuário.")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
