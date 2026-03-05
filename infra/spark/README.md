# Jupyter PySpark (Motor de Processamento)

Este diretório contém a configuração otimizada do motor de processamento distribuído **Apache Spark**, embrulhado dentro de um ambiente Jupyter Notebook para facilitar a experimentação interativa.

## Propósito

Em vez de subir um cluster Spark completo (arquitetura Master/Worker que consome muita RAM mesmo ocioso), optamos por condensar o Spark em um único contêiner inteligente (`jupyter/pyspark-notebook`). 

Neste projeto de Estatísticas de Futebol, o Spark é o "músculo" que processará os logs em lote do `datalake-raw`, limpará os dados estáticos e dinâmicos, e os injetará como tabelas validadas no MinIO gerenciadas via **Apache Iceberg**.

## Stack Integrado (Iceberg + S3A)

A beleza dessa configuração é que o Spark por padrão não sabe o que é Iceberg nem como falar com MinIO nativamente. Configuramos a ponte exata no arquivo `conf/spark-defaults.conf` e na variável de ambiente do `docker-compose.yaml`:

* **Downgrade Estratégico do Hadoop-AWS:** Descobrimos que a imagem usa um `hadoop` core levemente desatualizado em relação às bibliotecas mais modernas do S3A (ex: erro `PrefetchingStatistics`). Por isso, forçamos o PySpark a baixar no boot a versão customizada via:
  `--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262`
* **Catálogo Lake:** A pasta `conf/` define um catálogo chamado `lake` que mapeia todas as consultas `spark.sql()` diretamente para o bucket `s3a://datalake-warehouse/iceberg` sem precisar de um Metastore pesado como o Hive.

## Comunicação e Redes

Este contêiner não possui configurações complexas de rede internamente. Em vez disso, informamos o Docker Compose para anexá-lo à rede externa `datalake-network` (criada ao subir o MinIO). 

Isso garante que ao digitar algo como `.endpoint=http://minio:9000` nas opções do Spark, o DNS interno do Docker resolva perfeitamente o nome "minio" para o contêiner de storage de destino, sem precisar expor a porta local para a internet.

## Como Usar

Suba junto da stack completa via Makefile (`make infra-up` na raiz) ou execute no diretório:
`docker compose up -d`

* **Jupyter UI:** [http://localhost:8888](http://localhost:8888) (Token gerado via logs: `docker compose logs -f`)
* **Spark UI:** [http://localhost:4040](http://localhost:4040) (Ativa apenas enquanto uma sessão Spark estiver atrelada a uma célula do notebook ligada).

> **Aviso:** Seus notebooks são sincronizados automaticamente e não se perdem ao matar o contêiner devido ao mapeamento volumétrico para a pasta local `./notebooks`.
