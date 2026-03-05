# Futebol Estatísticas Data Pipeline

Pipeline de dados de de futebol de ponta a ponta, projetado com práticas modernas de Data Engineering. O pipeline realiza a extração automatizada de dados, o armazenamento em camadas seguindo a arquitetura Medallion (Data Lakehouse) e a persistência final em PostgreSQL estruturado. A orquestração é inteiramente gerenciada pelo **Apache Airflow**.

## Arquitetura de Dados (Data Engineering)

Este projeto segue princípios de **Arquitetura Lambda/Batch** para Data Engineering, priorizando resiliência, observabilidade e qualidade dos dados.

### Camadas de Dados (Medallion Architecture)

- **Bronze (Raw)**: Dados brutos recém extraídos da fonte. No nosso caso, arquivos locais (JSON, CSV, JSONL) injetados "as-is" no PostgreSQL (`raw_soccerdata_*`).
  - O pipeline garante a extração confiável de entidades como partidas (matches), escalações (lineups) e perfis detalhados de jogadores (com `athlete_id` único da ESPN).
- **Silver (Cleansed/Conformed)**: Dados limpos, normalizados e enriquecidos. (Fase 2 - Apache Spark/dbt).
  - Tipagem estrita de colunas, tratamento de valores nulos, padronização de formatações de tempo e data.
- **Gold (Curated/Analytics)**: Modelos dimensionais (Star Schema) agregados e prontos para alimentar dashboards de BI e recursos para Machine Learning.

## Pipeline Flow

```text
       [Source: ESPN API via soccerdata]
                    │
                    ▼
  ┌────────────────────────────────────┐
  │ Airflow DAG: get_brasileirao     │
  │ (Data Extraction & Enrichment)     │
  └────────────────────────────────────┘
        │   ├─ fetch_schedule
        │   ├─ extract_matches     (Extrai IDs únicos de atletas)
        │   └─ enrich_profiles     (Enriquece perfis na API ESPN)
        ▼
[Data Lake (Local/S3): output/raw & queue/pending.jsonl]
                    │
                    ▼
  ┌────────────────────────────────────┐
  │ Airflow DAG: consume_queue_to_pg │
  │ (Data Ingestion & Deduplication)   │
  └────────────────────────────────────┘
        │   ├─ load_payload
        │   ├─ deduplicate         (Hash & Semantic Dedupe)
        │   └─ dynamic_upsert      (Idempotent ON CONFLICT)
        ▼
[PostgreSQL: tabelas raw_soccerdata_*]  <-- BRONZE LAYER
```

## Stack Tecnológico

| Categoria | Tecnologia | Propósito no Projeto |
|---|---|---|
| **Orquestração** | Apache Airflow 2.x | Agendamento, dependências, retries, observabilidade |
| **Ingestão/Extração** | Python 3.11+, soccerdata | Requests HTTP, paralelismo iterativo, parsing de HTML/JSON |
| **Processamento Batch**| Apache Spark (planejado) | Transformações pesadas e data quality (Silver/Gold) |
| **Armazenamento** | PostgreSQL, Local FS | Storage relacional (DW) e Lakehouse inicial (Arquivos) |
| **Data Quality** | Em breve (dbt/Great Expectations) | Contratos de dados e validações (Testes de esquema e completude) |

## Estrutura de Diretórios

```text
football_statistics/
├── dags/                     # Airflow DAGs
│   ├── lib/                  # Extracted business logic & modular code
│   ├── brasileirao_teams_to_pg.py
│   ├── consume_brasileirao_queue_to_pg.py
│   └── README.md             # DAG Patterns documentation
├── tests/                    # Unit tests & Data Quality
│   └── test_extraction_helpers.py
├── output/                   # Data Lake (Git Ignored)
├── .env                      # Environment Variables
├── .gitignore
└── README.md
```

## Configuração do Ambiente

1. **Python Virtual Environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   pip install -r requirements.txt
   ```

2. **Variáveis de Ambiente (`.env`)**:
   ```env
   # PostgreSQL
   PGHOST=localhost
   PGPORT=5432
   PGDATABASE=nome_do_banco
   PGUSER=usuario
   PGPASSWORD=senha
   
   # Airflow / Runtime
   OUTPUT_DIR=output
   MAX_MATCHES=0
   LOG_EVERY=10
   PG_CONN_ID=db-pg-futebol-dados
   ESPN_API_DELAY=0.5
   QUEUE_BATCH_SIZE=100
   MOVE_PROCESSED_FILES=true
   ```

3. **Symlink do Airflow**:
   Para integrar com sua instância Airflow local ou na VPS:
   ```bash
   ln -sfn $(pwd)/dags $AIRFLOW_HOME/dags
   ```

## Testes e Qualidade

O projeto utiliza `pytest` para testes unitários da lógica de transformação e extração localizada em `dags/lib/`. 

```bash
pytest tests/ -v
```

## Roteiro de Desenvolvimento (Roadmap)

- [x] Extração confiável com `soccerdata` (capturando `athlete_id` robusto).
- [x] Pipeline incremental (arquitetura Queue-based com `.jsonl`).
- [x] Ingestão Idempotente no PostgreSQL (Dynamic Upsert).
- [ ] Construir a camada Silver e Testes de Qualidade de Dados (dbt / Spark).
- [ ] Modelagem Dimensional (Gold) para features analíticas.
- [ ] Treinamento de Modelos Preditivos e Machine Learning (PyTorch).
