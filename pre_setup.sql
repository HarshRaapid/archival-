SET @MAIN_DB = 'main_db';
SET @ARCH_DB = 'archival_db';

/* ===========================================
   Create logging tables (idempotent) in ARCH
   (each DDL is a single statement per PREPARE)
   =========================================== */
SET @sql = CONCAT('
CREATE TABLE IF NOT EXISTS ', @ARCH_DB, '.archival_run (
  run_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
  project_list  TEXT NOT NULL,
  started_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ended_at      TIMESTAMP NULL,
  status        ENUM(''RUNNING'',''SUCCESS'',''FAILED'') NOT NULL DEFAULT ''RUNNING'',
  error_message TEXT NULL
) ENGINE=InnoDB;');
PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

SET @sql = CONCAT('
CREATE TABLE IF NOT EXISTS ', @ARCH_DB, '.archival_log (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  run_id        BIGINT NOT NULL,
  project_id    BIGINT NULL,
  project_name  VARCHAR(255) NULL,
  table_name    VARCHAR(255) NOT NULL,
  rows_in_main  BIGINT NOT NULL DEFAULT 0,
  rows_copied   BIGINT NOT NULL DEFAULT 0,
  rows_deleted  BIGINT NOT NULL DEFAULT 0,
  status        ENUM(''PENDING'',''COPIED'',''VALIDATED'',''DELETED'',''FAILED'') NOT NULL DEFAULT ''PENDING'',
  note          TEXT NULL,
  created_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    TIMESTAMP NULL,
  INDEX (run_id),
  CONSTRAINT fk_archival_log_run FOREIGN KEY (run_id) REFERENCES ', @ARCH_DB, '.archival_run(run_id)
    ON DELETE CASCADE
) ENGINE=InnoDB;');
PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;