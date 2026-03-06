# Apache Airflow

Este diretório contém configurações, logs e metadados dedicados ao instanciamento do Apache Airflow. Diferente de toda as outras dependências (Minio, Spark) que rodam via Docker, **o Airflow é executado nativamente no Sistema Operacional (Ubuntu).**

## Propósito na Arquitetura

O Airflow funciona como o nosso Grande Maestro Operacional. Ele não executa cargas intensivas de processamento (isso é dever do PySpark). Ele apenas sabe "O que rodar", "Quando rodar", e "O que fazer se falhar".

Ele deve ser configurado com pipelines de DAGs que ditam a frequência com que:
1. Extraímos os dados da API de campeonatos.
2. Acionamos tarefas ETL ou scripts interconectados via sensores.
3. Movemos os dados processados para a Database ou atualizamos views materializadas.

## Instalação e Execução (Nativa via systemd)

Para esse ambiente, evitamos o overhead massivo causado por dependências como Celery/Redis que normalmente acompanham clusters Airflow via Docker. No diretório raiz existe uma representação estrutural da Virtual Environment (`venv/`) contendo a instalação do Airflow 3.x.

Na versão 3.0 do Airflow, rodamos 3 serviços essenciais de forma nativa e paralela com gerenciamento do sistema (Linux `systemd`):
1. **API Server** (Substitui o Webserver clássico)
2. **Scheduler**
3. **DAG Processor** (Foi desacoplado no Airflow 3.x)

### Passos de Referência (Ubuntu VM):

1. **Geração e Instalação de Serviços:**
   ```bash
   make airflow-install-services
   ```
   *Isso irá copiar os scripts em `infra/airflow/*.service` para o sistema (`/etc/systemd/...`) e habilitá-los no boot.*

2. **Iniciando o Orquestrador:**
   ```bash
   make airflow-up
   ```

3. **Monitoramento e Logs Centralizados:**
   Diga adeus ao "Screen" confuso. Agora seguimos os logs de todos os 3 serviços em tempo real:
   ```bash
   make logs-airflow
   ```

A UI do orquestrador estará acessível via Web:
👉 [http://IP-DA-VM-DO-UBUNTU:8080](http://localhost:8080)

*Nota: Os arquivos das suas DAGs reais (`/dags`) ficam disponíveis com versionamento git na raiz principal, sendo lincados nativamente (`ln -s`) ou lidos pelo DAG Processor a partir do seu `$AIRFLOW_HOME`.*
