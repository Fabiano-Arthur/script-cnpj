"""
Pipeline ETL para os dados públicos de CNPJ da Receita Federal.

Baixa o dump de uma competência (YYYY-MM), extrai os ZIPs internos e
carrega tudo num SQLite local. Sem argumento, usa o último mês fechado
(mês anterior ao atual), porque a competência do mês corrente costuma
não estar publicada ainda.

Uso:
    python cnpj_pipeline.py                 # último mês fechado
    python cnpj_pipeline.py 2026-04         # competência específica
    python cnpj_pipeline.py --keep-cache    # mantém tar.gz e CSVs
    python cnpj_pipeline.py --only empresas estabelecimentos
"""

import argparse
import csv
import logging
import re
import sqlite3
import sys
import tarfile
import zipfile
from datetime import date
from pathlib import Path

import requests
from tqdm import tqdm

# ==============================
# CONFIGURAÇÕES
# ==============================

NEXTCLOUD_BASE_URL = (
    "https://arquivos.receitafederal.gov.br/index.php/s/"
    "YggdBLfdninEJX9/download?path=/%2F{}"
)

# Tamanho do lote para INSERT em massa. Maior = menos overhead de Python,
# mas mais memória. 50k é um bom compromisso para CSVs largos.
BATCH_SIZE = 50_000

# Tabelas principais: dezenas/centenas de milhões de linhas.
MAIN_TABLES = {
    "empresas": {
        "prefix": "EMPRECSV",
        "columns": [
            ("cnpj_basico", "TEXT"),
            ("razao_social", "TEXT"),
            ("natureza_juridica", "TEXT"),
            ("qualificacao_responsavel", "TEXT"),
            ("capital_social", "TEXT"),  # vem com vírgula decimal; convertido depois
            ("porte_empresa", "TEXT"),
            ("ente_federativo", "TEXT"),
        ],
    },
    "estabelecimentos": {
        "prefix": "ESTABELE",
        "columns": [
            ("cnpj_basico", "TEXT"),
            ("cnpj_ordem", "TEXT"),
            ("cnpj_dv", "TEXT"),
            ("matriz_filial", "TEXT"),
            ("nome_fantasia", "TEXT"),
            ("situacao_cadastral", "TEXT"),
            ("data_situacao_cadastral", "TEXT"),
            ("motivo_situacao_cadastral", "TEXT"),
            ("nome_cidade_exterior", "TEXT"),
            ("pais", "TEXT"),
            ("data_inicio_atividade", "TEXT"),
            ("cnae_principal", "TEXT"),
            ("cnae_secundario", "TEXT"),
            ("tipo_logradouro", "TEXT"),
            ("logradouro", "TEXT"),
            ("numero", "TEXT"),
            ("complemento", "TEXT"),
            ("bairro", "TEXT"),
            ("cep", "TEXT"),
            ("uf", "TEXT"),
            ("municipio", "TEXT"),
            ("ddd1", "TEXT"),
            ("telefone1", "TEXT"),
            ("ddd2", "TEXT"),
            ("telefone2", "TEXT"),
            ("ddd_fax", "TEXT"),
            ("fax", "TEXT"),
            ("email", "TEXT"),
            ("situacao_especial", "TEXT"),
            ("data_situacao_especial", "TEXT"),
        ],
    },
    "socios": {
        "prefix": "SOCIOCSV",
        "columns": [
            ("cnpj_basico", "TEXT"),
            ("tipo_socio", "TEXT"),
            ("nome_socio", "TEXT"),
            ("documento_socio", "TEXT"),
            ("qualificacao_socio", "TEXT"),
            ("data_entrada", "TEXT"),
            ("pais", "TEXT"),
            ("cpf_representante", "TEXT"),
            ("nome_representante", "TEXT"),
            ("qualificacao_representante", "TEXT"),
            ("faixa_etaria", "TEXT"),
        ],
    },
    "simples": {
        "prefix": "SIMPLES",
        "columns": [
            ("cnpj_basico", "TEXT"),
            ("opcao_simples", "TEXT"),
            ("data_opcao_simples", "TEXT"),
            ("data_exclusao_simples", "TEXT"),
            ("opcao_mei", "TEXT"),
            ("data_opcao_mei", "TEXT"),
            ("data_exclusao_mei", "TEXT"),
        ],
    },
}

# Tabelas auxiliares (código → descrição). Pequenas.
LOOKUP_TABLES = {
    "cnaes": {"prefix": "CNAECSV", "columns": [("codigo", "TEXT"), ("descricao", "TEXT")]},
    "municipios": {"prefix": "MUNICCSV", "columns": [("codigo", "TEXT"), ("descricao", "TEXT")]},
    "naturezas": {"prefix": "NATJUCSV", "columns": [("codigo", "TEXT"), ("descricao", "TEXT")]},
    "paises": {"prefix": "PAISCSV", "columns": [("codigo", "TEXT"), ("descricao", "TEXT")]},
    "qualificacoes": {"prefix": "QUALSCSV", "columns": [("codigo", "TEXT"), ("descricao", "TEXT")]},
    "motivos": {"prefix": "MOTICSV", "columns": [("codigo", "TEXT"), ("descricao", "TEXT")]},
}

ALL_TABLES = {**MAIN_TABLES, **LOOKUP_TABLES}

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_empresas_cnpj ON empresas (cnpj_basico)",
    "CREATE INDEX IF NOT EXISTS idx_estab_cnpj ON estabelecimentos (cnpj_basico)",
    "CREATE INDEX IF NOT EXISTS idx_socios_cnpj ON socios (cnpj_basico)",
    "CREATE INDEX IF NOT EXISTS idx_simples_cnpj ON simples (cnpj_basico)",
    "CREATE INDEX IF NOT EXISTS idx_estab_uf ON estabelecimentos (uf)",
    "CREATE INDEX IF NOT EXISTS idx_estab_municipio ON estabelecimentos (municipio)",
]

log = logging.getLogger("cnpj")


# ==============================
# UTILITÁRIOS
# ==============================

def last_closed_month() -> str:
    """Mês anterior ao corrente em formato YYYY-MM."""
    today = date.today()
    year, month = (today.year, today.month - 1) if today.month > 1 else (today.year - 1, 12)
    return f"{year:04d}-{month:02d}"


def validate_competencia(value: str) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        raise argparse.ArgumentTypeError(f"Competência inválida: {value!r} (esperado YYYY-MM)")
    return value


# ==============================
# DOWNLOAD COM RETOMADA
# ==============================

def download_competencia(ano_mes: str, output_dir: Path) -> Path:
    """Baixa o cnpj.tar.gz da competência, retomando se já existir parcialmente."""
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"cnpj_{ano_mes}.tar.gz"
    url = NEXTCLOUD_BASE_URL.format(ano_mes)

    # HEAD para descobrir tamanho final esperado.
    head = requests.head(url, allow_redirects=True, timeout=30)
    head.raise_for_status()
    expected_size = int(head.headers.get("content-length", 0))

    existing = output_file.stat().st_size if output_file.exists() else 0
    if expected_size and existing == expected_size:
        log.info("Arquivo já baixado integralmente, pulando download: %s", output_file.name)
        return output_file

    headers = {}
    mode = "wb"
    if existing and expected_size and existing < expected_size:
        log.info("Retomando download de %s a partir de %d bytes", output_file.name, existing)
        headers["Range"] = f"bytes={existing}-"
        mode = "ab"

    log.info("Baixando competência %s (%s)", ano_mes, url)
    with requests.get(url, stream=True, timeout=60, headers=headers) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0)) + existing
        with open(output_file, mode) as f, tqdm(
            total=total or None,
            initial=existing,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            desc="Download",
        ) as bar:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
                    bar.update(len(chunk))

    return output_file


# ==============================
# EXTRAÇÃO
# ==============================

def _detect_format(path: Path) -> str:
    """Detecta o formato do arquivo pelos primeiros bytes (magic number).
    Retorna 'gzip', 'zip' ou 'unknown'."""
    with open(path, "rb") as f:
        magic = f.read(4)
    if magic[:2] == b"\x1f\x8b":
        return "gzip"
    if magic[:2] == b"PK":
        return "zip"
    return "unknown"


def extract_tar_gz(tar_path: Path, extract_to: Path) -> None:
    """Extrai o pacote baixado. Apesar do nome, aceita tanto tar.gz quanto
    zip — o Nextcloud da Receita às vezes serve um, às vezes o outro."""
    log.info("Extraindo %s", tar_path.name)
    extract_to.mkdir(parents=True, exist_ok=True)

    fmt = _detect_format(tar_path)
    if fmt == "gzip":
        with tarfile.open(tar_path, "r:gz") as tar:
            # filter="data" (Python 3.12+) bloqueia path traversal e links absolutos.
            try:
                tar.extractall(path=extract_to, filter="data")
            except TypeError:
                tar.extractall(path=extract_to)
    elif fmt == "zip":
        with zipfile.ZipFile(tar_path, "r") as z:
            z.extractall(extract_to)
    else:
        # Provavelmente uma página HTML de erro do servidor.
        with open(tar_path, "rb") as f:
            preview = f.read(200)
        raise RuntimeError(
            f"Formato desconhecido em {tar_path.name}. Primeiros bytes: {preview[:80]!r}. "
            "Apague o arquivo e baixe de novo."
        )


def unzip_inner_zips(base_dir: Path) -> None:
    zips = list(base_dir.rglob("*.zip"))
    if not zips:
        return
    log.info("Extraindo %d ZIPs internos", len(zips))
    for zip_file in zips:
        log.debug("Descompactando %s", zip_file.name)
        with zipfile.ZipFile(zip_file, "r") as z:
            z.extractall(zip_file.parent)
        zip_file.unlink()


# ==============================
# SCHEMA E CARGA
# ==============================

def create_schema(conn: sqlite3.Connection, tables: dict) -> None:
    for name, cfg in tables.items():
        cols_ddl = ", ".join(f'"{c}" {t}' for c, t in cfg["columns"])
        conn.execute(f'DROP TABLE IF EXISTS "{name}"')
        conn.execute(f'CREATE TABLE "{name}" ({cols_ddl})')
    conn.commit()


def find_csvs(base_dir: Path, prefix: str) -> list[Path]:
    """Acha CSVs cujo nome começa com o prefixo. Os arquivos vêm como
    EMPRECSV, K3241.K03200Y0.D40510.EMPRECSV etc., dependendo da competência."""
    prefix_upper = prefix.upper()
    matches = []
    for f in base_dir.rglob("*"):
        if not f.is_file():
            continue
        name = f.name.upper()
        if prefix_upper in name and ("CSV" in name or name.endswith(".CSV")):
            matches.append(f)
    return sorted(matches)


def load_table(conn: sqlite3.Connection, base_dir: Path, table: str, cfg: dict) -> None:
    files = find_csvs(base_dir, cfg["prefix"])
    if not files:
        log.warning("Nenhum arquivo encontrado para %s (prefixo %s)", table, cfg["prefix"])
        return

    columns = [c for c, _ in cfg["columns"]]
    placeholders = ",".join("?" * len(columns))
    col_list = ",".join(f'"{c}"' for c in columns)
    sql = f'INSERT INTO "{table}" ({col_list}) VALUES ({placeholders})'

    log.info("Carregando %s (%d arquivo(s))", table, len(files))
    total_rows = 0

    for file in files:
        log.info("  → %s", file.name)
        with open(file, "r", encoding="latin1", newline="") as fh:
            reader = csv.reader(fh, delimiter=";", quotechar='"')
            batch: list[tuple] = []
            file_rows = 0

            conn.execute("BEGIN")
            try:
                for row in reader:
                    # Alguns CSVs vêm com colunas a menos/mais conforme a competência.
                    if len(row) < len(columns):
                        row = row + [""] * (len(columns) - len(row))
                    elif len(row) > len(columns):
                        row = row[: len(columns)]
                    batch.append(tuple(row))

                    if len(batch) >= BATCH_SIZE:
                        conn.executemany(sql, batch)
                        file_rows += len(batch)
                        batch.clear()

                if batch:
                    conn.executemany(sql, batch)
                    file_rows += len(batch)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

        total_rows += file_rows
        log.info("     %d linhas", file_rows)

    log.info("  total em %s: %d linhas", table, total_rows)


def create_indexes(conn: sqlite3.Connection) -> None:
    log.info("Criando índices")
    for stmt in INDEXES:
        conn.execute(stmt)
    conn.commit()


# ==============================
# PIPELINE
# ==============================

def run_pipeline(
    ano_mes: str,
    only: list[str] | None,
    keep_cache: bool,
    download_dir: Path,
    work_dir: Path,
    db_path: Path,
) -> None:
    tar_file = download_competencia(ano_mes, download_dir)
    extract_tar_gz(tar_file, work_dir)
    unzip_inner_zips(work_dir)

    tables = ALL_TABLES if not only else {k: ALL_TABLES[k] for k in only}

    log.info("Criando banco SQLite: %s", db_path)
    conn = sqlite3.connect(db_path)
    try:
        for pragma in (
            "PRAGMA journal_mode=WAL",
            "PRAGMA synchronous=NORMAL",
            "PRAGMA temp_store=MEMORY",
            "PRAGMA cache_size=-200000",  # ~200 MB
        ):
            conn.execute(pragma)

        create_schema(conn, tables)
        for name, cfg in tables.items():
            load_table(conn, work_dir, name, cfg)
        create_indexes(conn)
        conn.execute("ANALYZE")
    finally:
        conn.close()

    if not keep_cache:
        log.info("Limpando arquivos intermediários")
        for csv_file in work_dir.rglob("*"):
            if csv_file.is_file():
                csv_file.unlink()
        tar_file.unlink(missing_ok=True)

    log.info("Pronto. Banco final: %s", db_path)


# ==============================
# CLI
# ==============================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "competencia",
        nargs="?",
        type=validate_competencia,
        help="Competência YYYY-MM. Sem argumento, usa o último mês fechado.",
    )
    p.add_argument(
        "--only",
        nargs="+",
        choices=list(ALL_TABLES.keys()),
        help="Carregar apenas estas tabelas.",
    )
    p.add_argument("--keep-cache", action="store_true", help="Mantém tar.gz e CSVs extraídos.")
    p.add_argument(
        "--output-dir",
        type=Path,
        help="Pasta base onde tudo será criado (downloads/, extracted/<comp>/, <comp>.db). "
             "Se omitido, o script pergunta interativamente.",
    )
    p.add_argument("--download-dir", type=Path, help="Sobrepõe a subpasta de downloads.")
    p.add_argument("--work-dir", type=Path, help="Sobrepõe a subpasta de CSVs extraídos.")
    p.add_argument("--db", type=Path, help="Sobrepõe o caminho do .db.")
    p.add_argument("--no-prompt", action="store_true", help="Não pergunta nada; usa o diretório atual como base.")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args()


def resolve_output_dir(args: argparse.Namespace) -> Path:
    """Resolve a pasta base a partir do flag, prompt ou default."""
    if args.output_dir:
        base = args.output_dir
    elif args.no_prompt or not sys.stdin.isatty():
        base = Path.cwd()
    else:
        default = Path.cwd()
        try:
            answer = input(f"Em qual pasta salvar os dados? [{default}]: ").strip()
        except EOFError:
            answer = ""
        base = Path(answer).expanduser() if answer else default
    base = base.expanduser().resolve()
    base.mkdir(parents=True, exist_ok=True)
    log.info("Pasta base: %s", base)
    return base


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    competencia = args.competencia or last_closed_month()
    if not args.competencia:
        log.info("Nenhuma competência informada. Usando último mês fechado: %s", competencia)

    slug = competencia.replace("-", "_")
    base = resolve_output_dir(args)
    download_dir = args.download_dir or (base / "downloads")
    work_dir = args.work_dir or (base / "extracted" / f"cnpj_{slug}")
    db_path = args.db or (base / f"cnpj_{slug}.db")

    for d in (download_dir, work_dir.parent):
        d.mkdir(parents=True, exist_ok=True)

    try:
        run_pipeline(
            ano_mes=competencia,
            only=args.only,
            keep_cache=args.keep_cache,
            download_dir=download_dir,
            work_dir=work_dir,
            db_path=db_path,
        )
    except requests.RequestException as e:
        log.error("Erro de rede: %s", e)
        return 1
    except KeyboardInterrupt:
        log.warning("Interrompido pelo usuário.")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
