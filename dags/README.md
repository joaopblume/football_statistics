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

## Arquitetura de Ingestão (Queue/Batch)

Em vez de abrir uma conexão com banco de dados para cada linha baixada no momento da extração, nós utilizamos um padrão de **Desacoplamento por Fila (Queueing)**:

1. **DAG Extrator** (`brasileirao_teams_to_pg.py`): Realiza o trabalho pesado I/O bound e network bound. Escreve arquivos na pasta `output/` e anexa metadados (no state/format) numa fila `pending.jsonl`.
2. **DAG Ingestor** (`consume_brasileirao_queue_to_pg.py`): Puxa lotes limitados (`QUEUE_BATCH_SIZE`) do `pending.jsonl`, realiza deduplicação lógica com tabelas de controle (`raw_ingestion_events`) e upserts massivos no Postgres.

Esse padrão permite *backpressure*, evita overload no banco de dados alvo, facilita debugging, e nos permite processar o arquivo "pending" novamente se o DB estava off-line.

## Organização do Diretório

```text
dags/
├── lib/                             # Lógica de extração e ingestão isolada
│   ├── __init__.py
│   ├── extraction_helpers.py      # Requests, Parsing JSON, DataFrames
│   └── ingestion_helpers.py       # SQL Strings, psycopg2 execs, Dedups
├── brasileirao_teams_to_pg.py       # Definition & config da orquestração extratora
└── consume_brasileirao_queue_to_pg.py # Definition & config do worker ingestor
```

## Como Adicionar/Modificar Lógica

Se você precisa alterar regras de negócio:
- **NÃO** adicione lógica em `dags/*.py`. Em vez disso, encontre a função correspondente em `lib/` e modifique-a.
- **ESCREVA UM TESTE UNITÁRIO**. Toda função complexa em `lib/` requer uma suíte correspondente na pasta base `tests/` do projeto.

## Referências e Padrões Airflow

- **TaskFlow API Decorators (`@dag`, `@task`)**: Usado para eliminar boilerplate obscuro de injetar operadores base e XCom pulls automáticos.
- **Evitar `depends_on_past`**: Deixamos o pipeline ser o mais stateless possível com upserts assíncronos (Idempotência sobrecarregando states antigos).
- **Timeouts rigorosos**: Impede a existência de tarefas zumbi (Zombie tasks) que bloqueiam os pools de instâncias dos workers.
