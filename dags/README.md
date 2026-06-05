# Airflow DAG Patterns & Best Practices

Este diretório contém os DAGs do Apache Airflow e os módulos auxiliares (`lib/`) que orquestram nossos pipelines de dados. Nós seguimos os padrões recomendados de *Software Engineering for Data* para garantir a manutenibilidade, resiliência e a estabilidade do pipeline em produção.

## Princípios de Design de DAGs

Nossos DAGs são construídos com base nestes 4 princípios fundamentais:

1. **Idempotência**:
   - Rodar a mesma task/DAG duas ou dez vezes produzirá exatamente o mesmo resultado final no banco de dados.
   - Isso é garantido via operações lógicas (`ON CONFLICT DO UPDATE` no PostgreSQL) ou recriando estados em vez de mutá-los.
2. **Atomicidade**:
   - As tasks são elaboradas para executar de ponta a ponta com sucesso, ou falhar completamente (rollback em DB, ou clean up). Sem processamentos incompletos.
3. **Task-Driven Logic Isolation**:
   - Manter a lógica de negócio **FORA** do arquivo principal do código do DAG. Todo o *heavy-lifting* (manipulação de DataFrames, transformações severas, operações de banco) deve estar contido em módulos testáveis no pacote `dags/lib/`.
4. **Resiliência e Observabilidade**:
   - Todas as tasks definem parâmetros de `retries`, `retry_exponential_backoff` e `execution_timeout` por padrão.

## Arquitetura do Pipeline (Medallion + Control Plane)

O pipeline segue a arquitetura Medallion, com DAGs desacoplados via `Dataset`
(*data-aware scheduling*) e coordenados por uma tabela de controle no PostgreSQL
(`pipeline_season_control`):

1. **Bronze** (`brasileirao_bronze_extraction.py`): *factory* que registra um DAG
   por liga (`bronze_extraction__<liga>`, `@hourly`). Extrai schedule → matchsheet
   → lineup → events do ESPN (via `soccerdata`) e sobe os JSONs para o MinIO
   (`datalake-raw`). Emite o `Dataset` Bronze compartilhado.
2. **Silver** (`brasileirao_silver_processing.py`): disparado pelo `Dataset` Bronze.
   Sobe o container Spark, executa o notebook Silver (tabelas Iceberg `teams`,
   `players`, `match_statistics`, `player_match_stats`) com *quality gates* reais
   que abortam antes de gravar se os dados estiverem ruins, e emite o `Dataset` Silver.
3. **Gold** (`brasileirao_gold_processing.py`): disparado pelo `Dataset` Silver.
   Agrega `player_season_stats`.

O estado de cada season (`pending → bronze_running → … → complete | failed`) vive
em `pipeline_season_control`; os helpers em `lib/season_helpers.py` leem/atualizam
essa tabela, permitindo **retry incremental a partir do stage que falhou**.

> Nota: a antiga ingestão em fila (`pending.jsonl` → Postgres `raw_soccerdata_*`)
> foi **aposentada** em favor deste fluxo Medallion. O PostgreSQL permanece como
> control plane (e futuro serving layer alimentado a partir do Gold).

## Organização do Diretório

```text
dags/
├── lib/                              # Lógica isolada e testável
│   ├── __init__.py
│   ├── extraction_helpers.py        # soccerdata/ESPN → DataFrames → MinIO (Bronze)
│   ├── league_config.py             # Registro de ligas + mapeamentos ESPN
│   ├── minio_config.py              # Resolução de credenciais MinIO (sem segredos no código)
│   ├── season_helpers.py            # Control plane (pipeline_season_control)
│   └── quality_helpers.py           # Registro de quality checks
├── brasileirao_bronze_extraction.py # Factory: 1 DAG Bronze por liga
├── brasileirao_silver_processing.py # Bronze → Silver (Iceberg + quality gates)
└── brasileirao_gold_processing.py   # Silver → Gold (Iceberg)
```

## Como Adicionar/Modificar Lógica

Se você precisa alterar regras de negócio:
- **NÃO** adicione lógica em `dags/*.py`. Em vez disso, encontre a função correspondente em `lib/` e modifique-a.
- **ESCREVA UM TESTE UNITÁRIO**. Toda função complexa em `lib/` requer uma suíte correspondente na pasta base `tests/` do projeto.

## Referências e Padrões Airflow

- **TaskFlow API Decorators (`@dag`, `@task`)**: Usado para eliminar boilerplate obscuro de injetar operadores base e XCom pulls automáticos.
- **Evitar `depends_on_past`**: Deixamos o pipeline ser o mais stateless possível com upserts assíncronos (Idempotência sobrecarregando states antigos).
- **Timeouts rigorosos**: Impede a existência de tarefas zumbi (Zombie tasks) que bloqueiam os pools de instâncias dos workers.
