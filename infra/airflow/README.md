# Apache Airflow

Este diretório contém configurações, logs e metadados dedicados ao instanciamento do Apache Airflow. Diferente de toda as outras dependências (Minio, Spark) que rodam via Docker, **o Airflow é executado nativamente no Sistema Operacional (Ubuntu).**

## Propósito na Arquitetura

O Airflow funciona como o nosso Grande Maestro Operacional. Ele não executa cargas intensivas de processamento (isso é dever do PySpark). Ele apenas sabe "O que rodar", "Quando rodar", e "O que fazer se falhar".

Ele deve ser configurado com pipelines de DAGs que ditam a frequência com que:
1. Extraímos os dados da API de campeonatos.
2. Acionamos tarefas ETL ou scripts interconectados via sensores.
3. Movemos os dados processados para a Database ou atualizamos views materializadas.

## Instalação e Execução (Nativa via "screen")

Para esse ambiente, evitamos o overhead massivo causado pelo Celery/Redis que normalmente acompanham clusters Airflow via Docker. No diretório raiz existe uma representação estrutural da Virtual Environment (`venv/` ou `env_airflow`) contendo a instalação do Airflow:

### Passos de Referência (Ubuntu VM):

1. Ative seu ambiente virtual Python local onde o Airflow foi instalado.
   `source ~/env_airflow/bin/activate` *(Exemplo)*
2. Defina o home do Airflow para que ele não tente criar pastas ocultas indesejadas na home do usuário linux.
   `export AIRFLOW_HOME=~/football_statistics/airflow`
3. Como rodamos sem docker, o recomendando para deixar os processos rodando em background na máquina virtual Ubuntu é através de instâncias nativas do `screen`:
   * Em um Terminal Screen 1: `airflow webserver --port 8080`
   * Em um Terminal Screen 2: `airflow scheduler`

A UI do orquestrador estará acessiva via Web:
👉 [http://IP-DA-VM-DO-UBUNTU:8080](http://localhost:8080)

*Nota: Os arquivos das suas DAGs reais (`/dags`) ficam disponíveis com versionamento git na raiz principal, sendo lincados nativamente (`ln -s`) ou referenciados no arquivo `airflow.cfg`.*
