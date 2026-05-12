"""
Sincroniza as competências de CNPJ da Receita Federal localmente.

Varre os meses de 2023-05 até o mês atual e baixa só os que ainda não
têm pasta no diretório base. Cada mês vira uma pasta YYYY-MM/ com o
arquivo dados.tar.gz dentro.

Regras de idempotência:
- Pasta NÃO existe -> baixa.
- Pasta EXISTE sem o marcador .downloading -> pula (considera concluído,
  mesmo que esteja vazia).
- Pasta EXISTE com o marcador .downloading -> retoma/refaz (download
  anterior foi interrompido).

O arquivo agregado cnpj.tar.gz na raiz do share (dezenas de GB) NÃO é
baixado — só os arquivos mensais.

Uso:
    python sync_months.py                       # pasta atual
    python sync_months.py --output-dir /dados
    python sync_months.py --start 2024-01       # ignora antes de 2024-01
    python sync_months.py --end 2026-03         # não tenta meses futuros
    python sync_months.py --dry-run             # só lista o que faria
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

import requests
from tqdm import tqdm

from cnpj_pipeline import (
    NEXTCLOUD_BASE_URL,
    resolve_output_dir,
    validate_competencia,
)

EARLIEST = "2023-05"
DOWNLOAD_FILENAME = "dados.tar.gz"
MARKER_NAME = ".downloading"

log = logging.getLogger("cnpj-sync")


def iter_months(start: str, end: str):
    """Itera YYYY-MM de start até end (inclusive)."""
    y, m = (int(x) for x in start.split("-"))
    ey, em = (int(x) for x in end.split("-"))
    while (y, m) <= (ey, em):
        yield f"{y:04d}-{m:02d}"
        m += 1
        if m > 12:
            m, y = 1, y + 1


def current_month() -> str:
    today = date.today()
    return f"{today.year:04d}-{today.month:02d}"


def needs_download(folder: Path) -> bool:
    """True se o mês ainda não foi baixado com sucesso."""
    if not folder.exists():
        return True
    if (folder / MARKER_NAME).exists():
        log.info("%s: marcador .downloading presente, baixando de novo", folder.name)
        return True
    return False


def download_month(ano_mes: str, dest_folder: Path) -> bool:
    """Baixa o mês. Retorna True se concluiu, False se não havia dados (404)."""
    dest_folder.mkdir(parents=True, exist_ok=True)
    marker = dest_folder / MARKER_NAME
    output_file = dest_folder / DOWNLOAD_FILENAME
    url = NEXTCLOUD_BASE_URL.format(ano_mes)

    marker.touch()

    try:
        with requests.get(url, stream=True, timeout=60) as r:
            if r.status_code == 404:
                log.warning("%s: não disponível no servidor (404)", ano_mes)
                # Limpa pasta vazia + marcador para que a próxima execução tente de novo.
                marker.unlink(missing_ok=True)
                if not any(dest_folder.iterdir()):
                    dest_folder.rmdir()
                return False
            r.raise_for_status()

            total = int(r.headers.get("content-length", 0))
            with open(output_file, "wb") as f, tqdm(
                total=total or None,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                desc=ano_mes,
            ) as bar:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))

        marker.unlink(missing_ok=True)
        return True

    except BaseException:
        # Em qualquer falha (rede, Ctrl+C, etc.), deixamos o marcador para
        # a próxima execução refazer o mês.
        raise


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--output-dir", type=Path, help="Pasta base (default: pergunta).")
    p.add_argument(
        "--no-prompt",
        action="store_true",
        help="Usa o diretório atual sem perguntar.",
    )
    p.add_argument(
        "--start",
        type=validate_competencia,
        default=EARLIEST,
        help=f"Primeira competência a considerar (default: {EARLIEST}).",
    )
    p.add_argument(
        "--end",
        type=validate_competencia,
        help="Última competência (default: mês atual).",
    )
    p.add_argument("--dry-run", action="store_true", help="Só lista o que faria.")
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
    end = args.end or current_month()

    if args.start > end:
        log.error("--start (%s) maior que --end (%s)", args.start, end)
        return 2

    log.info("Verificando meses de %s até %s em %s", args.start, end, base)

    todo = [m for m in iter_months(args.start, end) if needs_download(base / m)]

    if not todo:
        log.info("Nada a baixar — tudo em dia.")
        return 0

    log.info("Meses faltando (%d): %s", len(todo), ", ".join(todo))

    if args.dry_run:
        log.info("Dry-run: nada baixado.")
        return 0

    failed: list[str] = []
    not_published: list[str] = []

    for month in todo:
        folder = base / month
        try:
            ok = download_month(month, folder)
            if not ok:
                not_published.append(month)
        except requests.RequestException as e:
            log.error("Falha em %s: %s", month, e)
            failed.append(month)
        except KeyboardInterrupt:
            log.warning("Interrompido em %s. Pasta marcada para retomar na próxima execução.", month)
            return 130

    log.info("---")
    log.info("Concluídos: %d", len(todo) - len(failed) - len(not_published))
    if not_published:
        log.info("Sem dados no servidor (provavelmente mês ainda não publicado): %s", ", ".join(not_published))
    if failed:
        log.warning("Falhas: %s", ", ".join(failed))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
