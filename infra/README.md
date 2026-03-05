# Infrastructure Environment

Este diretório contém toda a configuração de infraestrutura da nossa plataforma de dados (Data Lakehouse). 

A arquitetura foi desenhada para ser modular, simulando um ambiente de produção real, mas otimizada para rodar localmente com baixo consumo de recursos (via Docker Compose ou instalações diretas no SO).

## Estrutura da Plataforma

Nossa arquitetura é composta pelos seguintes pilares:

* **[`minio/`](minio/README.md)**: O coração do nosso Data Lake. Serve como um armazenamento de objetos compatível com S3. Aqui armazenamos desde os dados brutos recém engolidos até nossas tabelas curadas em formato Apache Iceberg.
* **[`spark/`](spark/README.md)**: Nosso motor de processamento distribuído, configurado de forma enxuta rodando dentro de um contêiner Jupyter Notebook (PySpark). Ele é responsável por ler os dados brutos, transformar e salvar as tabelas Iceberg no MinIO.
* **[`postgres/`](postgres/README.md)**: Nosso Data Warehouse / Banco Relacional. Onde dados altamente estruturados e sumarizados podem ser servidos para ferramentas de BI ou aplicações finais.
* **[`airflow/`](airflow/README.md)**: O cérebro da operação. Responsável por orquestrar todo o pipeline: desde buscar os dados na API, chamar os scripts do Spark, até carregar as tabelas finais no Postgres.

## Como Gerenciar (Makefile)

Para facilitar o desenvolvimento, usamos um `Makefile` na raiz do projeto (`../Makefile`). A partir da pasta principal do projeto, você pode gerenciar a infraestrutura Dockerizada usando:

* `make infra-up`: Sobem o MinIO e o ambiente Jupyter PySpark na ordem correta, conectando-os na mesma rede (`datalake-network`).
* `make infra-down`: Desliga e limpa os contêineres e redes (seus dados não são perdidos, pois estão em volumes).
* `make infra-restart`: Reinicia a infraestrutura.
* `make logs-spark`: Exibe os logs do PySpark (útil para descobrir o token de acesso do Jupyter).
