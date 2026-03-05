# PostgreSQL

Este repositório documenta a instância final de serving camada da infraestrutura. O PostgreSQL **deve ser instalado diretamente no sistema operacional (Ubuntu)** e não dockerizado, seguindo a filosofia de isolamento de infraestrutura definida no projeto.

## Propósito no Projeto

Em qualquer Data Platform bem dimensionada há uma diferença significativa entre um **Data Lakehouse** (Iceberg/MinIO) e um **Database Transacional/Relacional** Clássico de serviço.

No projeto de Estatísticas de Futebol:
* **Iceberg** armazena todo cenário de Machine Learning, tabelões não-normalizados de dezenas de terabytes e históricos densos analíticos.
* **PostgreSQL** é o nosso Serving Layer. As tabelas analíticas mastigadas são sincronizadas para cá. Esta base fornece baixa latência essencial se o seu foco com esse projeto de futebol for mostrar dashboards performáticos no Streamlit/Metabase ou responder APIs RESTful transacionais aos usuários que consultarão os times pela internet de forma massiva.

## Configuração do SO (Recados de Apoio)

Siga os repositórios oficiais via pacote do APT (Debian/Ubuntu):

```bash
# Exemplo genérico
sudo apt update
sudo apt install postgresql postgresql-contrib
```

* Os serviços podem ser inicializados via `systemctl start postgresql.service`.
* Lembre-se de configurar e liberar o arquivo nativo `pg_hba.conf` no Ubuntu além de setar o `listen_addresses='*'` em `postgresql.conf` caso outras máquinas (ou o próprio Spark via IP da ponte docker) necessitem pingar essa base para as inserções via JDBC.
