# Futebol Estatísticas

Pipeline de dados de futebol com extração automatizada, armazenamento em camadas (medallion architecture) e persistência em PostgreSQL — orquestrado por **Apache Airflow**.

## Objetivo

1. Extrair dados de futebol de forma automatizada (soccerdata / ESPN).
2. Processar e normalizar os dados com Apache Spark (Bronze → Silver).
3. Persistir dados curados em PostgreSQL como fonte confiável.
4. Realizar análises estatísticas a partir dos dados curados.
5. Treinar modelos de Machine Learning com PyTorch (futuro).

## Stack

| Camada | Tecnologia |
|---|---|
| Orquestração | Apache Airflow (VPS) |
| Processamento | Apache Spark (Docker / VPS) |
| Banco de dados | PostgreSQL |
| Extração | soccerdata (ESPN provider) |
| Linguagem | Python 3.11+ |
| Libs auxiliares | Pandas, Pendulum, python-dotenv |

## Arquitetura

```
ESPN (soccerdata)
      │
      ▼
  ┌────────────────────────┐
  │  Airflow DAG:          │
  │  get_brasileirao       │──► Raw files (JSON + CSV)
  │  (extração)            │──► Fila JSONL (pending.jsonl)
  └────────────────────────┘
              │
              ▼
  ┌────────────────────────┐
  │  Airflow DAG:          │
  │  consume_brasileirao   │──► PostgreSQL (tabelas raw_soccerdata_*)
  │  _queue_to_pg          │──► Controle (raw_ingestion_events)
  │  (ingestão)            │──► done.jsonl / failed.jsonl
  └────────────────────────┘
              │
              ▼
  ┌────────────────────────┐
  │  Spark (planejado)     │
  │  Bronze → Silver       │──► PostgreSQL (tabelas curadas)
  └────────────────────────┘
```

### Camadas de Dados (Medallion)

- **Bronze**: dados brutos extraídos, persistidos tal qual nas tabelas `raw_soccerdata_*`.
- **Silver**: dados limpos, normalizados e enriquecidos (Spark — em desenvolvimento).
- **Gold**: agregações e métricas prontas para análise e ML (futuro).

## DAGs

### `get_brasileirao`
> `dags/brasileirao_teams_to_pg.py`

Extrai dados do Brasileirão via **soccerdata** (provider ESPN):
- Lê o schedule completo da temporada.
- Para cada partida, extrai o lineup de jogadores.
- Grava dados brutos em arquivos locais (JSON por partida, CSV por lineup/time/jogador).
- Enfileira mensagens em `output/queue/pending.jsonl` para ingestão posterior.

### `consume_brasileirao_queue_to_pg`
> `dags/consume_brasileirao_queue_to_pg.py`

Consome a fila JSONL e persiste no PostgreSQL:
- Deduplicação por hash + chave semântica.
- Criação dinâmica de tabelas e colunas a partir do payload.
- Upsert via `ON CONFLICT` (idempotente).
- Tabela de controle `raw_ingestion_events` para rastreabilidade.
- Move arquivos processados para `output/processed/`.

## Configuração

### Variáveis de ambiente (`.env`)

```env
API_FOOTBALL_KEY=seu_token
PGHOST=localhost
PGPORT=5432
PGDATABASE=nome_do_banco
PGUSER=usuario
PGPASSWORD=senha
```

### Variáveis de ambiente do Airflow

| Variável | Default | Descrição |
|---|---|---|
| `OUTPUT_DIR` | `output` | Diretório raiz para dados extraídos |
| `MAX_MATCHES` | `0` (todas) | Limite de partidas por execução |
| `LOG_EVERY` | `10` | Frequência de log por partida |
| `PG_CONN_ID` | `db-pg-futebol-dados` | Connection ID do PostgreSQL no Airflow |
| `QUEUE_BATCH_SIZE` | `100` | Tamanho do lote de ingestão |
| `MOVE_PROCESSED_FILES` | `true` | Mover arquivos após ingestão |

## Roadmap

1. ⬜ Integrar Apache Spark para camada Bronze → Silver.
2. ⬜ Criar camadas de qualidade de dados (checks de consistência e completude).
3. ⬜ Módulo de análise estatística exploratória e séries temporais.
4. ⬜ Preparar dataset para ML e treinar modelos em PyTorch.
5. ⬜ Versionamento de datasets e modelos para reproducibilidade.
