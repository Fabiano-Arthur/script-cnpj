# script-cnpj

Pipeline ETL para os **dados públicos de CNPJ** publicados pela Receita
Federal. Baixa o dump mensal, extrai os ZIPs internos e carrega tudo em
um banco local (SQLite) ou remoto (PostgreSQL).

Fonte oficial: [arquivos.receitafederal.gov.br](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/).

---

## O que tem aqui

| Arquivo | O que faz |
|---|---|
| `cnpj_pipeline.py` | Baixa, extrai e carrega em **SQLite** local. |
| `cnpj_to_postgres.py` | Mesma carga, mas em **PostgreSQL** via `COPY FROM STDIN`. Reaproveita o download do pipeline SQLite. |
| `requirements.txt` | Dependências (`requests`, `tqdm`, `psycopg`). |

Tabelas carregadas: `empresas`, `estabelecimentos`, `socios`, `simples`,
mais auxiliares (`cnaes`, `municipios`, `naturezas`, `paises`,
`qualificacoes`, `motivos`).

---

## Instalação

Python 3.10+.

```bash
git clone git@github.com:Fabiano-Arthur/script-cnpj.git
cd script-cnpj
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Uso — SQLite

Modo mais simples: tudo num arquivo `.db` local.

```bash
# Último mês fechado (mês anterior ao atual)
python cnpj_pipeline.py

# Competência específica
python cnpj_pipeline.py 2026-04

# Escolhendo onde salvar
python cnpj_pipeline.py --output-dir /dados/cnpj

# Só algumas tabelas
python cnpj_pipeline.py --only empresas estabelecimentos

# Mantém arquivos intermediários (tar.gz e CSVs)
python cnpj_pipeline.py --keep-cache
```

Sem `--output-dir`, o script pergunta a pasta interativamente (em CI/cron
use `--no-prompt` para pular).

### Estrutura criada

```
<output-dir>/
├── downloads/
│   └── cnpj_2026-04.tar.gz
├── extracted/
│   └── cnpj_2026_04/
│       └── *.CSV
└── cnpj_2026_04.db
```

---

## Uso — PostgreSQL

```bash
# Credenciais via variáveis padrão libpq
export PGHOST=localhost PGPORT=5432
export PGUSER=meuusuario PGPASSWORD=senha PGDATABASE=meubanco

python cnpj_to_postgres.py 2026-04 --output-dir /dados/cnpj
```

Ou passando o DSN direto:

```bash
python cnpj_to_postgres.py \
    --dsn 'postgresql://user:senha@host:5432/db' \
    --schema cnpj \
    --output-dir /dados/cnpj
```

Reaproveitando CSVs que você já baixou (não baixa de novo):

```bash
python cnpj_to_postgres.py --csv-dir /dados/cnpj/extracted/cnpj_2026_04
```

### Flags úteis

| Flag | Default | Descrição |
|---|---|---|
| `--schema` | `cnpj` | Schema de destino no Postgres. |
| `--only T1 T2 ...` | todas | Carrega só estas tabelas. |
| `--logged` | (off) | Cria tabelas `LOGGED` desde o início. Por default usa `UNLOGGED` durante a carga + `SET LOGGED` no final (~2-3x mais rápido). |
| `--output-dir` | prompt | Pasta base para downloads/CSVs. |
| `--csv-dir` | — | Pula download/extração e usa CSVs deste diretório. |
| `-v` | (off) | Modo verboso. |

---

## Como funciona internamente

1. **Download** com retomada via header `Range` e cache (não baixa de novo se o arquivo está completo).
2. **Extração em duas camadas**: o `.tar.gz` da Receita contém vários `.zip`, que por sua vez contêm os `.CSV`.
3. **Carga**:
   - SQLite: `executemany` com transação por arquivo, todas as colunas como `TEXT`.
   - Postgres: `COPY ... FROM STDIN` via `cursor.copy().write_row()`, tabelas `UNLOGGED` durante a carga.
4. **Schema explícito + `DROP TABLE IF EXISTS`**: rodar duas vezes não duplica linhas.
5. **Índices só ao final** da carga, mais `ANALYZE` para o planner.

### Notas sobre os dados

- Todas as colunas são carregadas como **texto**. Campos como `cnpj_basico` e `cep` têm zeros à esquerda; `capital_social` vem com vírgula decimal e datas no formato `YYYYMMDD`. Converta depois com `CREATE VIEW` ou `ALTER COLUMN ... USING`.
- **Encoding** dos CSVs originais: `latin1`.
- A Receita publica os dumps com **1-2 meses de atraso**. Por isso o default do script é o **mês anterior**, não o corrente.

---

## Tamanhos aproximados

| Item | Tamanho |
|---|---|
| `cnpj.tar.gz` baixado | ~6 GB |
| CSVs extraídos | ~25 GB |
| Banco SQLite final | ~30 GB |
| Postgres (com índices) | ~40 GB |
| Linhas em `estabelecimentos` | ~60 milhões |

Tempo total ponta a ponta: **algumas horas**, dominado pelo I/O.

---

## Limitações

- O layout dos CSVs da Receita muda eventualmente; se a competência vier com colunas diferentes, o loader trunca/padroniza, mas vale rodar com `-v` na primeira vez.
- Não há tratamento de retry inteligente para o servidor da Receita (que cai com alguma frequência). Se o download falhar, rode de novo — a retomada cuida do resto.
