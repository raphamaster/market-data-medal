-- Bancos por camada (em MariaDB, "database" = "schema")
CREATE DATABASE IF NOT EXISTS md_bronze CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
CREATE DATABASE IF NOT EXISTS md_silver CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
CREATE DATABASE IF NOT EXISTS md_gold   CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

-- Catálogo auxiliar (já vem de .env como MARIADB_DATABASE=md_catalog)
CREATE DATABASE IF NOT EXISTS md_catalog CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

-- Usuário "etl" já é criado pelo entrypoint usando .env,
-- mas garantimos os GRANTs para todas as camadas:
GRANT ALL PRIVILEGES ON md_bronze.*  TO 'etl'@'%';
GRANT ALL PRIVILEGES ON md_silver.*  TO 'etl'@'%';
GRANT ALL PRIVILEGES ON md_gold.*    TO 'etl'@'%';
GRANT ALL PRIVILEGES ON md_catalog.* TO 'etl'@'%';

FLUSH PRIVILEGES;

-- Tabelas de controle (opcional, úteis para idempotência, checkpoint de ingestão)
CREATE TABLE IF NOT EXISTS md_catalog.ingestion_log (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  source_name VARCHAR(100) NOT NULL,
  run_id CHAR(36) NOT NULL,
  start_ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  end_ts TIMESTAMP NULL,
  status ENUM('STARTED','SUCCESS','FAILED') NOT NULL,
  rows_ingested BIGINT DEFAULT 0,
  message TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS md_catalog.watermarks (
  source_name VARCHAR(100) PRIMARY KEY,
  last_extracted_at DATETIME NULL,
  last_extracted_key VARCHAR(100) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
