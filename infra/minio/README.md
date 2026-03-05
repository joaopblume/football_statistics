# MinIO (Data Lake Storage)

O MinIO atua como o nosso armazenamento de objetos (Object Storage), sendo 100% compatível com a API do Amazon S3. Ele é a fundação do nosso Data Lakehouse local.

## Propósito

Na nossa arquitetura, o MinIO tem o papel central de armazenar todos os arquivos físicos gerados pelas nossas extrações e processamentos, garantindo durabilidade e isolamento. Ele substitui a necessidade de criar pastas bagunçadas no sistema operacional.

## Estrutura de Buckets (Camadas de Dados)

O nosso `docker-compose.yaml` já está configurado (junto com o serviço de inicialização `mc`) para criar automaticamente uma estrutura profissional de Data Lake assim que sobe pela primeira vez:

1. **`datalake-raw` (Bronze):** A porta de entrada. Armazena os arquivos originais (JSON/CSV) exatamente do jeito que vieram das APIs de futebol. Zero processamento. Imutável.
2. **`datalake-warehouse` (Silver/Gold):** Onde gravamos as tabelas refinadas gerenciadas pelo **Apache Iceberg** (arquivos Parquet + metadados). É aqui que ocorrem as integrações, otimizações e o versionamento dos dados.
3. **`datalake-artifacts`:** Repositório auxiliar para guardar scripts, arquivos JAR e configurações que outras ferramentas (como Spark ou Airflow) precisem baixar futuramente.

## Configurações Específicas

* **Credenciais Genéricas:** `MINIO_ROOT_USER=minioadmin` e `MINIO_ROOT_PASSWORD=minioadmin123`. Funcionalmente equivalentes à `AWS_ACCESS_KEY_ID` e `AWS_SECRET_ACCESS_KEY`.
* **Região Simulada:** Definimos `MINIO_REGION_NAME=us-east-1` por padrão para evitar que bibliotecas S3 muito estritas (como o conector do Hadoop) falhem ao não encontrar uma região configurada.
* **External Network:** O MinIO cria uma Docker Network externa nativa chamada `datalake-network`. Essa rede é vital, pois é através dela que o contêiner do Spark consegue se comunicar diretamente com o endpoint S3 local (`http://minio:9000`).
* **Volume Permanente:** Todo o estado do seu MinIO é salvo localmente na pasta oculta `./minio-data`. Seus buckets e tabelas sobrevivem a desligamentos do Docker.

## Como Acessar

Se preferir não usar o `make`, os comandos nativos são:
`docker compose up -d` / `docker compose down`

A **Console UI** de administração visual fica disponível através do seu navegador em: 
👉 [http://localhost:9001](http://localhost:9001) (acesso com admin / admin123).
