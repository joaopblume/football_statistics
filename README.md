# Futebol Estatisticas

Projeto para construir uma base confiavel de dados de futebol com extracao automatizada, persistencia em PostgreSQL e evolucao para analises estatisticas e modelos de Machine Learning.

## Objetivo

1. Extrair dados de futebol de forma automatizada (API e, futuramente, scraping controlado).
2. Sincronizar os dados em um banco PostgreSQL como fonte unica e confiavel.
3. Realizar analises estatisticas a partir dos dados coletados.
4. Testar treinamento de modelos de ML com PyTorch.

## Stack Atual

- Python 3.11+
- PostgreSQL
- SQLAlchemy
- Requests
- python-dotenv

## Arquitetura Atual

- `database/`
  - `base.py`: declarative base do SQLAlchemy.
  - `connection.py`: engine e `SessionLocal`.
  - `models/`: entidades separadas por tabela (`League`, `Season`, `Team`, `Venue`, `Player`, `PlayerStatistics`).
- `extraction/`
  - `api_client.py`: cliente da API-Football, com controle de rate limit e limites de plano.
  - `mappers.py`: normalizacao/mapeamento do payload da API para o schema relacional.
  - `repositories.py`: upsert em lote usando `ON CONFLICT` (PostgreSQL).
  - `run_extraction.py`: orquestracao do fluxo de extracao + persistencia.

## Fluxo de Extracao

1. Validar conexao e existencia das tabelas no banco.
2. Extrair e salvar liga e temporada.
3. Extrair e salvar times e estadios.
4. Extrair e salvar jogadores e estatisticas.
5. Commit unico ao final; em caso de erro, rollback.

## Pre-requisitos

- Banco PostgreSQL ativo e acessivel.
- Tabelas ja criadas no banco.
- Chave da API-Football valida.

## Configuracao

Crie/ajuste o arquivo `.env` com:

```env
API_FOOTBALL_KEY=seu_token
PGHOST=localhost
PGPORT=5432
PGDATABASE=nome_do_banco
PGUSER=usuario
PGPASSWORD=senha
```

Instale dependencias:

```bash
pip install -r requirements.txt
```

## Execucao

Rodar extracao da Serie A (id padrao = 71) para uma temporada:

```bash
python extraction/run_extraction.py --season 2023
```

Ou definir explicitamente a liga:

```bash
python extraction/run_extraction.py --league-id 71 --season 2023
```

Extrair um intervalo de temporadas (ex.: 2022 ate 2024):

```bash
python extraction/run_extraction.py --league-id 71 --season-start 2022 --season-end 2024
```

### Parametros de linha de comando

- `--league-id`: define a liga na API-Football (ex.: Serie A = `71`).
- `--season`: extrai um unico ano.
- `--season-start` + `--season-end`: extrai um intervalo de anos (inclusive).
- Regra de uso: informe `--season` **ou** o par `--season-start/--season-end`.

## Comportamentos Importantes

- O script possui validacao de banco antes de chamar a API.
- Em limite diario da API (`errors.requests`), a execucao e encerrada imediatamente.
- Em rate limit por minuto, o cliente aguarda e tenta novamente.
- Em plano free, o endpoint de players respeita limite de paginas suportado.
- Se a API retornar `errors.plan` para temporadas fora do acesso do plano, use anos suportados (no seu caso atual: `2022` a `2024`).

## Roadmap

1. Orquestrar pipeline com Apache Airflow (DAGs agendadas, retries, observabilidade).
2. Criar camadas de qualidade de dados (checks de consistencia e completude).
3. Construir modulo de analise estatistica exploratoria e series temporais.
4. Preparar dataset para ML e treinar modelos em PyTorch.
5. Versionar datasets e modelos para reproducibilidade.
