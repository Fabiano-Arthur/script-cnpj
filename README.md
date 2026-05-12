# script-cnpj

Baixa os **dados públicos de CNPJ da Receita Federal** e carrega num banco
local (SQLite) ou remoto (PostgreSQL). Cobre desde o dump mais antigo
(maio/2023) até o mês atual, com histórico mês a mês.

Fonte oficial: [arquivos.receitafederal.gov.br](https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/).

---

## Qual script eu uso?

Depende do que você quer fazer:

| Eu quero... | Use |
|---|---|
| Só o **último mês** num SQLite local pra explorar | `cnpj_pipeline.py` |
| Só o **último mês** direto num Postgres | `cnpj_to_postgres.py` |
| **Baixar todos os meses** (sem carregar em banco) | `sync_months.py` |
| **Carregar todos os meses já baixados** num Postgres, com histórico | `bulk_load_postgres.py` |

O fluxo recomendado para **histórico completo** é:
**1)** `sync_months.py` baixa tudo →
**2)** `bulk_load_postgres.py` carrega tudo no Postgres.

---

## Instalação

Python 3.10 ou mais novo.

```bash
git clone git@github.com:Fabiano-Arthur/script-cnpj.git
cd script-cnpj

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
```

Dependências: `requests`, `tqdm`, `psycopg`.

---

## Quick start — histórico completo no Postgres

```bash
# 1. Configure conexão com o Postgres (uma vez)
export PGHOST=localhost
export PGPORT=5432
export PGUSER=meuusuario
export PGPASSWORD=minhasenha
export PGDATABASE=meubanco

# 2. Baixa todos os meses (2023-05 até hoje) — leva horas
python sync_months.py --output-dir /dados/cnpj

# 3. Carrega tudo no Postgres — também leva horas
python bulk_load_postgres.py --output-dir /dados/cnpj
```

Pronto. Os dados ficam no schema `cnpj` com uma coluna `competencia`
identificando a qual mês cada linha pertence.

> **Importante:** isso vai gerar **centenas de GB** baixados e potencialmente
> **terabytes** no Postgres. Para experimentar antes, restrinja a poucos meses:
> ```bash
> python sync_months.py --start 2026-04 --end 2026-05 --output-dir /dados/cnpj
> python bulk_load_postgres.py --output-dir /dados/cnpj --only 2026-04 2026-05
> ```

---

## Scripts em detalhe

### `sync_months.py` — baixar os meses

Varre o servidor da Receita e baixa cada competência **que ainda não está**
na sua pasta. Idempotente — pode rodar quantas vezes quiser, ele só baixa
o que falta.

```bash
# Baixa tudo, perguntando onde salvar
python sync_months.py

# Especificando a pasta direto
python sync_months.py --output-dir /dados/cnpj

# Apenas um intervalo
python sync_months.py --start 2024-01 --end 2024-12 --output-dir /dados/cnpj

# Ver o que faria, sem baixar
python sync_months.py --dry-run --output-dir /dados/cnpj
```

**Estrutura criada:**

```
/dados/cnpj/
├── 2023-05/
│   └── dados.tar.gz
├── 2023-06/
│   └── dados.tar.gz
├── ...
└── 2026-05/
    └── dados.tar.gz
```

**Regras de idempotência:**

| Estado da pasta `YYYY-MM/` | O que o script faz |
|---|---|
| Não existe | Cria e baixa |
| Existe sem marcador `.downloading` | Pula (considera concluída) |
| Existe com marcador `.downloading` | Refaz (download anterior foi interrompido) |

Se um download é interrompido (Ctrl+C, queda de rede), o marcador
`.downloading` fica na pasta — a próxima execução refaz aquele mês
automaticamente.

---

### `bulk_load_postgres.py` — carregar tudo no Postgres

Lê os `dados.tar.gz` baixados pelo `sync_months.py` e carrega num Postgres,
**acumulando** os meses na mesma tabela (com uma coluna `competencia`).

```bash
# Carrega todos os meses encontrados em /dados/cnpj
python bulk_load_postgres.py --output-dir /dados/cnpj

# Só alguns meses
python bulk_load_postgres.py --output-dir /dados/cnpj --only 2024-01 2024-02

# Re-processar um mês (DELETE + reload)
python bulk_load_postgres.py --output-dir /dados/cnpj --only 2024-01 --reload
```

**O que cria no banco:**

```
cnpj/                                ← schema
├── empresas (competencia, ...)
├── estabelecimentos (competencia, ...)
├── socios (competencia, ...)
├── simples (competencia, ...)
├── cnaes, municipios, naturezas, ...  ← auxiliares, sem competencia
└── competencias_carregadas             ← tabela de controle
```

**Idempotência:** o script consulta `competencias_carregadas` antes de
carregar cada mês. Se já estiver lá, pula. Use `--reload` para forçar.

**Interrupção:** se você der Ctrl+C no meio, a competência em andamento
é revertida (transação), mas as anteriores ficam preservadas. É seguro
rodar de novo depois.

---

### `cnpj_pipeline.py` — pipeline completo num SQLite (1 mês)

Tudo num único arquivo `.db`. Bom pra explorar localmente sem instalar
banco nenhum.

```bash
# Último mês fechado
python cnpj_pipeline.py --output-dir /dados/cnpj-sqlite

# Mês específico
python cnpj_pipeline.py 2026-04 --output-dir /dados/cnpj-sqlite

# Só algumas tabelas (rápido)
python cnpj_pipeline.py --only empresas estabelecimentos --output-dir /dados/cnpj-sqlite
```

Gera arquivo `cnpj_<comp>.db` na pasta base. Cada execução substitui
o anterior (uma competência por vez, sem histórico).

---

### `cnpj_to_postgres.py` — pipeline completo num Postgres (1 mês)

Baixa + extrai + carrega no Postgres, **substituindo** os dados a cada
rodada. Diferente do `bulk_load_postgres.py`, que acumula.

```bash
python cnpj_to_postgres.py 2026-04 --output-dir /dados/cnpj --schema cnpj_snapshot
```

Use isso se você só quer **o snapshot mais recente** no banco, sem
histórico. Schema fica isolado para não conflitar com o `bulk_load_postgres.py`.

---

## Configurando o Postgres

Os scripts seguem o padrão libpq — qualquer destas formas funciona:

**Via variáveis de ambiente:**
```bash
export PGHOST=localhost
export PGPORT=5432
export PGUSER=meuusuario
export PGPASSWORD=minhasenha
export PGDATABASE=meubanco
```

**Via DSN:**
```bash
python bulk_load_postgres.py --dsn 'postgresql://user:senha@host:5432/db'
```

**Via DATABASE_URL:**
```bash
export DATABASE_URL='postgresql://user:senha@host:5432/db'
python bulk_load_postgres.py
```

---

## Exemplos de queries

Depois do `bulk_load_postgres.py`, com histórico:

```sql
-- Quantas empresas em cada competência
SELECT competencia, COUNT(*) AS total
FROM cnpj.empresas
GROUP BY competencia
ORDER BY competencia;

-- Evolução de uma empresa específica
SELECT competencia, razao_social, capital_social, porte_empresa
FROM cnpj.empresas
WHERE cnpj_basico = '12345678'
ORDER BY competencia;

-- Empresas NOVAS em 2026-04 (que não existiam em 2026-03)
SELECT n.cnpj_basico, n.razao_social
FROM cnpj.empresas n
LEFT JOIN cnpj.empresas a
  ON a.cnpj_basico = n.cnpj_basico AND a.competencia = '2026-03'
WHERE n.competencia = '2026-04' AND a.cnpj_basico IS NULL;

-- Top 10 municípios em número de estabelecimentos ativos (última competência)
SELECT m.descricao, COUNT(*) AS qtd
FROM cnpj.estabelecimentos e
JOIN cnpj.municipios m ON m.codigo = e.municipio
WHERE e.competencia = (SELECT MAX(competencia) FROM cnpj.estabelecimentos)
  AND e.situacao_cadastral = '02'  -- ativa
GROUP BY m.descricao
ORDER BY qtd DESC
LIMIT 10;
```

---

## Estrutura das tabelas

Todas as colunas são carregadas como `TEXT` por design (campos como
`cnpj_basico` têm zeros à esquerda, `capital_social` usa vírgula decimal,
datas vêm como `YYYYMMDD`). Converta nas suas queries com `CAST`/`TO_DATE`
ou crie views tipadas.

**Encoding** dos CSVs originais: `latin1` — os scripts já tratam isso.

---

## Tamanhos e tempos esperados

| | Por competência | 36 competências (histórico completo) |
|---|---|---|
| Download (tar.gz) | ~6 GB | ~210 GB |
| Postgres (com índices) | ~40 GB | ~1.4 TB |
| Tempo de download | 10-30 min | 6-18 h |
| Tempo de carga | 30-90 min | 1-3 dias |

Por isso o `sync_months.py` é idempotente e o `bulk_load_postgres.py` é
retomável — rode em background, em pedaços, ou em um servidor que não
desligue.

---

## Limitações conhecidas

- O **mês atual** muitas vezes ainda não está publicado no servidor. O
  script trata o 404 como aviso e segue — você pode rodar de novo daqui
  a alguns dias.
- O layout dos CSVs da Receita pode mudar em competências futuras. Se
  isso acontecer, rode com `-v` para ver detalhes; o loader trunca/padroniza
  linhas com colunas a menos/mais.
- O servidor da Receita cai eventualmente. Se isso acontecer durante o
  download, basta rodar `sync_months.py` de novo — ele retoma do ponto
  que parou.

---

## Estrutura do projeto

```
script-cnpj/
├── sync_months.py          # baixa tudo (idempotente)
├── bulk_load_postgres.py   # carrega tudo no Postgres (histórico)
├── cnpj_pipeline.py        # pipeline 1-mês → SQLite
├── cnpj_to_postgres.py     # pipeline 1-mês → Postgres (substitui)
├── requirements.txt
└── README.md
```
