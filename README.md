## Market Data Medallion Lakehouse

### Visão geral
Este repositório implementa um pipeline de dados com arquitetura medallion (Bronze → Silver → Gold) para centralizar cotações de câmbio, criptoativos e índices de mercado. Toda a orquestração é feita em Python, com persistência em MariaDB executando via Docker Compose.

### Principais componentes
- Ingestão Bronze: scripts que coletam dados crus das APIs públicas (ECB, Banco Central/PTAX, CoinGecko) e do provider Stooq para índices.
- Camada Silver: normaliza moedas, índices e cripto, padronizando colunas e tipos de dados antes de persistir.
- Camada Gold: agrega e expõe métricas consolidadas prontas para consumo analítico.
- Orquestração local: `scripts/run_all.py` executa todas as etapas de ponta a ponta.

### Estrutura do repositório
```
configs/                # Configurações das fontes (ex.: listas de pares e índices)
docker-compose.yml      # Subir MariaDB estruturadas em bronze/silver/gold
docker/                 # Configurações específicas do container (init, my.cnf)
etl/                    # Pipelines Python separados por camada
  bronze/               # Ingestões brutas das fontes externas
  silver/               # Normalizações e harmonização de dados
  gold/                 # Construção da camada final
  common/               # Reuso: conexão DB, IO e variáveis de ambiente
scripts/run_all.py      # Executor sequencial das pipelines
requirements.txt        # Dependências Python
```

### Pré-requisitos
1. Python 3.11+ (recomendado criar um ambiente virtual).
2. Docker e Docker Compose.
3. Arquivo `.env` na raiz com, pelo menos:
   ```
   START_DATE=2024-01-01
   SQLALCHEMY_URL=mysql+pymysql://etl:etl@localhost:3306/md_catalog
   TZ=America/Sao_Paulo
   COINGECKO_API_KEY=<opcional se a API exigir>
   COINGECKO_API_KEY_HEADER=
   COINGECKO_API_KEY_QUERY_PARAM=
   ```
   Ajuste as credenciais conforme os valores configurados no `docker/mariadb/initdb/00_init.sql`.

### Como executar
1. Suba o banco:
   ```bash
   docker compose up -d
   ```
2. Ative o ambiente virtual e instale dependências:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```
3. Rode o pipeline completo:
   ```bash
   python scripts/run_all.py
   ```
   O script executa cada módulo em sequência, finalizando com a construção da camada Gold.

### Próximos passos sugeridos
- Automatizar agendamentos (Airflow, Prefect ou GitHub Actions + cron).
- Implementar testes automatizados para validar transformações Silver/Gold.
- Adicionar visualizações/relatórios consumindo a camada Gold.
