/* ===========================
   CONFIG: set your schema names
   =========================== */
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

DELIMITER $$

/* ===========================================================
   Master procedure: Archive projects by comma-separated names
   - Copies → validates → deletes
   - Per-table logging and transaction safety
   =========================================================== */
CREATE OR REPLACE PROCEDURE archive_projects(IN p_project_names_csv TEXT)
BEGIN
  /* ---------- error handling ---------- */
  DECLARE v_run_id BIGINT DEFAULT NULL;
  DECLARE v_old_fk INT DEFAULT 1;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    /* try to restore FK checks safely */
    SET FOREIGN_KEY_CHECKS = v_old_fk;
    IF v_run_id IS NOT NULL THEN
      UPDATE /* arch */ 
        (SELECT * FROM (SELECT 1) x) dummy /* hack to allow dynamic schema below */
      ;
      /* dynamic UPDATE because schema is variable */
      SET @sql = CONCAT('UPDATE ', @ARCH_DB, '.archival_run ',
                        'SET status=''FAILED'', ended_at=NOW(), error_message=COALESCE(error_message, ''FAILED'') ',
                        'WHERE run_id = ', v_run_id);
      PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
    END IF;
  END;

  /* ---------- open a run ---------- */
  SET @sql = CONCAT('INSERT INTO ', @ARCH_DB, '.archival_run (project_list) VALUES (?)');
  PREPARE s FROM @sql; SET @plist := p_project_names_csv; EXECUTE s USING @plist; DEALLOCATE PREPARE s;
  /* capture run_id */
  SET @sql = CONCAT('SELECT LAST_INSERT_ID() INTO @rid');
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  SET v_run_id = @rid;

  START TRANSACTION;

  /* ---------- FK checks off (session only) ---------- */
  SET v_old_fk = @@FOREIGN_KEY_CHECKS;
  SET FOREIGN_KEY_CHECKS = 0;

  /* =================================================
     Build TEMP ID sets (split each statement properly)
     ================================================= */

  /* projects_to_archive_tmp */
  DROP TEMPORARY TABLE IF EXISTS projects_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE projects_to_archive_tmp AS ',
    'SELECT id, name, client_id FROM ', @MAIN_DB, '.project_mst ',
    'WHERE FIND_IN_SET(name, ?) > 0'
  );
  PREPARE s FROM @sql; SET @p := p_project_names_csv; EXECUTE s USING @p; DEALLOCATE PREPARE s;
  ALTER TABLE projects_to_archive_tmp ADD PRIMARY KEY (id), ADD INDEX idx_cli (client_id);

  /* clients_to_archive_tmp */
  DROP TEMPORARY TABLE IF EXISTS clients_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE clients_to_archive_tmp AS ',
    'SELECT DISTINCT c.id FROM ', @MAIN_DB, '.client_mst c ',
    'JOIN projects_to_archive_tmp p ON p.client_id = c.id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE clients_to_archive_tmp ADD PRIMARY KEY (id);

  /* encounters_to_archive_tmp */
  DROP TEMPORARY TABLE IF EXISTS encounters_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE encounters_to_archive_tmp AS ',
    'SELECT e.id FROM ', @MAIN_DB, '.encounter_mst e ',
    'JOIN projects_to_archive_tmp p ON p.id = e.project_id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE encounters_to_archive_tmp ADD PRIMARY KEY (id);

  /* documents_to_archive_tmp */
  DROP TEMPORARY TABLE IF EXISTS documents_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE documents_to_archive_tmp AS ',
    'SELECT d.id FROM ', @MAIN_DB, '.document_mst d ',
    'JOIN encounters_to_archive_tmp e ON e.id = d.encounter_id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE documents_to_archive_tmp ADD PRIMARY KEY (id);

  /* leaf temp sets */
  DROP TEMPORARY TABLE IF EXISTS document_codes_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE document_codes_to_archive_tmp AS ',
    'SELECT dc.id FROM ', @MAIN_DB, '.document_code dc ',
    'JOIN documents_to_archive_tmp dt ON dt.id = dc.document_id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE document_codes_to_archive_tmp ADD PRIMARY KEY (id);

  DROP TEMPORARY TABLE IF EXISTS cm_codes_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE cm_codes_to_archive_tmp AS ',
    'SELECT c.id FROM ', @MAIN_DB, '.cm_code c ',
    'JOIN encounters_to_archive_tmp e ON e.id = c.encounter_id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE cm_codes_to_archive_tmp ADD PRIMARY KEY (id);

  DROP TEMPORARY TABLE IF EXISTS encounter_dos_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE encounter_dos_to_archive_tmp AS ',
    'SELECT ed.id FROM ', @MAIN_DB, '.encounter_dos ed ',
    'JOIN encounters_to_archive_tmp e ON e.id = ed.encounter_id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE encounter_dos_to_archive_tmp ADD PRIMARY KEY (id);

  DROP TEMPORARY TABLE IF EXISTS document_dos_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE document_dos_to_archive_tmp AS ',
    'SELECT dd.id FROM ', @MAIN_DB, '.document_dos dd ',
    'JOIN documents_to_archive_tmp d ON d.id = dd.document_id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE document_dos_to_archive_tmp ADD PRIMARY KEY (id);

  DROP TEMPORARY TABLE IF EXISTS document_evidence_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE document_evidence_to_archive_tmp AS ',
    'SELECT de.id FROM ', @MAIN_DB, '.document_evidence de ',
    'JOIN documents_to_archive_tmp d ON d.id = de.document_id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE document_evidence_to_archive_tmp ADD PRIMARY KEY (id);

  DROP TEMPORARY TABLE IF EXISTS discussions_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE discussions_to_archive_tmp AS ',
    'SELECT dm.id FROM ', @MAIN_DB, '.discussion_mst dm ',
    'JOIN encounters_to_archive_tmp e ON e.id = dm.encounter_id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE discussions_to_archive_tmp ADD PRIMARY KEY (id);

  DROP TEMPORARY TABLE IF EXISTS compliance_gaps_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE compliance_gaps_to_archive_tmp AS ',
    'SELECT cg.id FROM ', @MAIN_DB, '.compliance_gaps cg ',
    'JOIN projects_to_archive_tmp p ON p.id = cg.project_id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE compliance_gaps_to_archive_tmp ADD PRIMARY KEY (id);

  DROP TEMPORARY TABLE IF EXISTS configurations_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE configurations_to_archive_tmp AS ',
    'SELECT c.id FROM ', @MAIN_DB, '.configuration_mst c ',
    'JOIN projects_to_archive_tmp p ON p.id = c.project_id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE configurations_to_archive_tmp ADD PRIMARY KEY (id);

  -- DROP TEMPORARY TABLE IF EXISTS buckets_to_archive_tmp;
  -- SET @sql = CONCAT(
  --   'CREATE TEMPORARY TABLE buckets_to_archive_tmp AS ',
  --   'SELECT b.id FROM ', @MAIN_DB, '.bucket_mst b ',
  --   'JOIN projects_to_archive_tmp p ON p.id = b.project_id'
  -- );
  -- PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  -- ALTER TABLE buckets_to_archive_tmp ADD PRIMARY KEY (id);

  DROP TEMPORARY TABLE IF EXISTS claim_details_to_archive_tmp;
  SET @sql = CONCAT(
    'CREATE TEMPORARY TABLE claim_details_to_archive_tmp AS ',
    'SELECT cd.id FROM ', @MAIN_DB, '.claim_detail cd ',
    'JOIN encounters_to_archive_tmp e ON e.id = cd.encounter_id'
  );
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
  ALTER TABLE claim_details_to_archive_tmp ADD PRIMARY KEY (id);

  /* ===============================================
     Work plan: which tables to process & join logic
     (store predicates as text; we will use them in
      single-statement dynamic queries)
     =============================================== */
  DROP TEMPORARY TABLE IF EXISTS _work_plan;
  CREATE TEMPORARY TABLE _work_plan (
    seq INT PRIMARY KEY,
    table_name VARCHAR(128),
    exists_pred TEXT,   -- WHERE <exists/subquery> (predicate against MAIN rows)
    from_join   TEXT    -- FROM ... JOIN ... (for INSERT SELECT)
  );

  /* grandchildren of document_code */
  INSERT INTO _work_plan VALUES
  ( 10, 'document_code_evidence_map',
    'EXISTS (SELECT 1 FROM document_codes_to_archive_tmp x WHERE x.id = t.document_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_code_evidence_map t JOIN document_codes_to_archive_tmp k ON k.id = t.document_code_id')),
  ( 11, 'document_code_meat_evidence_map',
    'EXISTS (SELECT 1 FROM document_codes_to_archive_tmp x WHERE x.id = t.document_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_code_meat_evidence_map t JOIN document_codes_to_archive_tmp k ON k.id = t.document_code_id')),
  ( 12, 'document_code_combination_map',
    'EXISTS (SELECT 1 FROM document_codes_to_archive_tmp x WHERE x.id = t.document_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_code_combination_map t JOIN document_codes_to_archive_tmp k ON k.id = t.document_code_id')),
  ( 13, 'document_code_hcc_group_map',
    'EXISTS (SELECT 1 FROM document_codes_to_archive_tmp x WHERE x.id = t.document_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_code_hcc_group_map t JOIN document_codes_to_archive_tmp k ON k.id = t.document_code_id')),
  ( 14, 'document_code_cms_hcc_v28_group_map',
    'EXISTS (SELECT 1 FROM document_codes_to_archive_tmp x WHERE x.id = t.document_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_code_cms_hcc_v28_group_map t JOIN document_codes_to_archive_tmp k ON k.id = t.document_code_id')),
  ( 15, 'document_code_hhs_hcc_group_map',
    'EXISTS (SELECT 1 FROM document_codes_to_archive_tmp x WHERE x.id = t.document_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_code_hhs_hcc_group_map t JOIN document_codes_to_archive_tmp k ON k.id = t.document_code_id')),
  ( 16, 'document_code_rxhcc_hcc_group_map',
    'EXISTS (SELECT 1 FROM document_codes_to_archive_tmp x WHERE x.id = t.document_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_code_rxhcc_hcc_group_map t JOIN document_codes_to_archive_tmp k ON k.id = t.document_code_id')),

  /* evidence & dos evidence */
  ( 20, 'document_evidence_coordinates',
    'EXISTS (SELECT 1 FROM document_evidence_to_archive_tmp x WHERE x.id = t.document_evidence_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_evidence_coordinates t JOIN document_evidence_to_archive_tmp k ON k.id = t.document_evidence_id')),
  ( 21, 'document_dos_evidence_map',
    'EXISTS (SELECT 1 FROM document_dos_to_archive_tmp x WHERE x.id = t.document_dos_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_dos_evidence_map t JOIN document_dos_to_archive_tmp k ON k.id = t.document_dos_id')),

  /* cm_code descendants */
  ( 30, 'cm_code_comment',
    'EXISTS (SELECT 1 FROM cm_codes_to_archive_tmp x WHERE x.id = t.cm_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code_comment t JOIN cm_codes_to_archive_tmp k ON k.id = t.cm_code_id')),
  ( 31, 'cm_code_evidence_map',
    'EXISTS (SELECT 1 FROM cm_codes_to_archive_tmp x WHERE x.id = t.cm_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code_evidence_map t JOIN cm_codes_to_archive_tmp k ON k.id = t.cm_code_id')),
  ( 32, 'cm_code_history',
    'EXISTS (SELECT 1 FROM cm_codes_to_archive_tmp x WHERE x.id = t.cm_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code_history t JOIN cm_codes_to_archive_tmp k ON k.id = t.cm_code_id')),
  ( 33, 'cm_code_meat_evidence_map',
    'EXISTS (SELECT 1 FROM cm_codes_to_archive_tmp x WHERE x.id = t.cm_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code_meat_evidence_map t JOIN cm_codes_to_archive_tmp k ON k.id = t.cm_code_id')),
  ( 34, 'cm_code_reject_reason_map',
    'EXISTS (SELECT 1 FROM cm_codes_to_archive_tmp x WHERE x.id = t.cm_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code_reject_reason_map t JOIN cm_codes_to_archive_tmp k ON k.id = t.cm_code_id')),
  ( 35, 'cm_code_combination_map',
    'EXISTS (SELECT 1 FROM cm_codes_to_archive_tmp x WHERE x.id = t.cm_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code_combination_map t JOIN cm_codes_to_archive_tmp k ON k.id = t.cm_code_id')),
  ( 36, 'cm_code_hcc_group_map',
    'EXISTS (SELECT 1 FROM cm_codes_to_archive_tmp x WHERE x.id = t.cm_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code_hcc_group_map t JOIN cm_codes_to_archive_tmp k ON k.id = t.cm_code_id')),
  ( 37, 'cm_code_cms_hcc_v28_group_map',
    'EXISTS (SELECT 1 FROM cm_codes_to_archive_tmp x WHERE x.id = t.cm_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code_cms_hcc_v28_group_map t JOIN cm_codes_to_archive_tmp k ON k.id = t.cm_code_id')),
  ( 38, 'cm_code_hhs_hcc_group_map',
    'EXISTS (SELECT 1 FROM cm_codes_to_archive_tmp x WHERE x.id = t.cm_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code_hhs_hcc_group_map t JOIN cm_codes_to_archive_tmp k ON k.id = t.cm_code_id')),
  ( 39, 'cm_code_rxhcc_hcc_group_map',
    'EXISTS (SELECT 1 FROM cm_codes_to_archive_tmp x WHERE x.id = t.cm_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code_rxhcc_hcc_group_map t JOIN cm_codes_to_archive_tmp k ON k.id = t.cm_code_id')),
  ( 40, 'cm_code_suppressed_cm_code_map',
    'EXISTS (SELECT 1 FROM cm_codes_to_archive_tmp x WHERE x.id = t.cm_code_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code_suppressed_cm_code_map t JOIN cm_codes_to_archive_tmp k ON k.id = t.cm_code_id')),

  /* encounter_dos descendants */
  ( 50, 'encounter_dos_comment',
    'EXISTS (SELECT 1 FROM encounter_dos_to_archive_tmp x WHERE x.id = t.encounter_dos_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_dos_comment t JOIN encounter_dos_to_archive_tmp k ON k.id = t.encounter_dos_id')),
  ( 51, 'encounter_dos_evidence_map',
    'EXISTS (SELECT 1 FROM encounter_dos_to_archive_tmp x WHERE x.id = t.encounter_dos_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_dos_evidence_map t JOIN encounter_dos_to_archive_tmp k ON k.id = t.encounter_dos_id')),
  ( 52, 'encounter_dos_last_worked',
    'EXISTS (SELECT 1 FROM encounter_dos_to_archive_tmp x WHERE x.id = t.encounter_dos_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_dos_last_worked t JOIN encounter_dos_to_archive_tmp k ON k.id = t.encounter_dos_id')),
  ( 53, 'encounter_dos_page_range_detail',
    'EXISTS (SELECT 1 FROM encounter_dos_to_archive_tmp x WHERE x.id = t.encounter_dos_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_dos_page_range_detail t JOIN encounter_dos_to_archive_tmp k ON k.id = t.encounter_dos_id')),
  ( 54, 'encounter_dos_reject_reason_map',
    'EXISTS (SELECT 1 FROM encounter_dos_to_archive_tmp x WHERE x.id = t.encounter_dos_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_dos_reject_reason_map t JOIN encounter_dos_to_archive_tmp k ON k.id = t.encounter_dos_id')),
  ( 55, 'encounter_dos_standard_comment',
    'EXISTS (SELECT 1 FROM encounter_dos_to_archive_tmp x WHERE x.id = t.encounter_dos_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_dos_standard_comment t JOIN encounter_dos_to_archive_tmp k ON k.id = t.encounter_dos_id')),

  /* discussion descendants */
  ( 60, 'discussion_comment',
    'EXISTS (SELECT 1 FROM discussions_to_archive_tmp x WHERE x.id = t.discussion_id)',
    CONCAT('FROM ', @MAIN_DB, '.discussion_comment t JOIN discussions_to_archive_tmp k ON k.id = t.discussion_id')),
  ( 61, 'discussion_user_map',
    'EXISTS (SELECT 1 FROM discussions_to_archive_tmp x WHERE x.id = t.discussion_id)',
    CONCAT('FROM ', @MAIN_DB, '.discussion_user_map t JOIN discussions_to_archive_tmp k ON k.id = t.discussion_id')),

  /* claim descendants */
  ( 70, 'claim_code_map',
    'EXISTS (SELECT 1 FROM claim_details_to_archive_tmp x WHERE x.id = t.claim_id)',
    CONCAT('FROM ', @MAIN_DB, '.claim_code_map t JOIN claim_details_to_archive_tmp k ON k.id = t.claim_id')),
  ( 71, 'claim_procedure_code_map',
    'EXISTS (SELECT 1 FROM claim_details_to_archive_tmp x WHERE x.id = t.claim_id)',
    CONCAT('FROM ', @MAIN_DB, '.claim_procedure_code_map t JOIN claim_details_to_archive_tmp k ON k.id = t.claim_id')),

  /* compliance/config/bucket descendants */
  -- ( 80, 'bucket_encounter_status_map',
  --   'EXISTS (SELECT 1 FROM buckets_to_archive_tmp x WHERE x.id = t.bucket_id)',
  --   CONCAT('FROM ', @MAIN_DB, '.bucket_encounter_status_map t JOIN buckets_to_archive_tmp k ON k.id = t.bucket_id')),
  ( 81, 'compliance_gaps_details',
    'EXISTS (SELECT 1 FROM compliance_gaps_to_archive_tmp x WHERE x.id = t.compliance_gaps_id)',
    CONCAT('FROM ', @MAIN_DB, '.compliance_gaps_details t JOIN compliance_gaps_to_archive_tmp k ON k.id = t.compliance_gaps_id')),
  ( 82, 'compliance_gaps_evidence_map',
    'EXISTS (SELECT 1 FROM compliance_gaps_to_archive_tmp x WHERE x.id = t.compliance_gaps_id)',
    CONCAT('FROM ', @MAIN_DB, '.compliance_gaps_evidence_map t JOIN compliance_gaps_to_archive_tmp k ON k.id = t.compliance_gaps_id')),
  ( 83, 'configuration_mst_history',
    'EXISTS (SELECT 1 FROM configurations_to_archive_tmp x WHERE x.id = t.configuration_mst_id)',
    CONCAT('FROM ', @MAIN_DB, '.configuration_mst_history t JOIN configurations_to_archive_tmp k ON k.id = t.configuration_mst_id')),

  /* direct children */
  (100, 'document_code',
    'EXISTS (SELECT 1 FROM documents_to_archive_tmp x WHERE x.id = t.document_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_code t JOIN documents_to_archive_tmp x ON x.id = t.document_id')),
  (101, 'document_demographic',
    'EXISTS (SELECT 1 FROM documents_to_archive_tmp x WHERE x.id = t.document_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_demographic t JOIN documents_to_archive_tmp x ON x.id = t.document_id')),
  (102, 'document_dos',
    'EXISTS (SELECT 1 FROM documents_to_archive_tmp x WHERE x.id = t.document_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_dos t JOIN documents_to_archive_tmp x ON x.id = t.document_id')),
  (103, 'document_evidence',
    'EXISTS (SELECT 1 FROM documents_to_archive_tmp x WHERE x.id = t.document_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_evidence t JOIN documents_to_archive_tmp x ON x.id = t.document_id')),
  (104, 'document_page_detail',
    'EXISTS (SELECT 1 FROM documents_to_archive_tmp x WHERE x.id = t.document_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_page_detail t JOIN documents_to_archive_tmp x ON x.id = t.document_id')),
  (105, 'document_processing_detail',
    'EXISTS (SELECT 1 FROM documents_to_archive_tmp x WHERE x.id = t.document_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_processing_detail t JOIN documents_to_archive_tmp x ON x.id = t.document_id')),
  (106, 'document_review_map',
    'EXISTS (SELECT 1 FROM documents_to_archive_tmp x WHERE x.id = t.document_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_review_map t JOIN documents_to_archive_tmp x ON x.id = t.document_id')),
  (107, 'document_section_map',
    'EXISTS (SELECT 1 FROM documents_to_archive_tmp x WHERE x.id = t.document_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_section_map t JOIN documents_to_archive_tmp x ON x.id = t.document_id')),
  (108, 'document_sentence',
    'EXISTS (SELECT 1 FROM documents_to_archive_tmp x WHERE x.id = t.document_id)',
    CONCAT('FROM ', @MAIN_DB, '.document_sentence t JOIN documents_to_archive_tmp x ON x.id = t.document_id')),
  (109, 'genai_processing_detail',
    'EXISTS (SELECT 1 FROM documents_to_archive_tmp x WHERE x.id = t.document_id)',
    CONCAT('FROM ', @MAIN_DB, '.genai_processing_detail t JOIN documents_to_archive_tmp x ON x.id = t.document_id')),
  (110, 'cm_code',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.cm_code t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (111, 'encounter_dos',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_dos t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (112, 'encounter_cms_hcc_config_map',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_cms_hcc_config_map t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (113, 'encounter_event_map',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_event_map t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (114, 'encounter_more_specific_less_specific_code_map',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_more_specific_less_specific_code_map t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (115, 'encounter_nonlegible_page_detail',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_nonlegible_page_detail t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (116, 'encounter_nonlegible_page_user_map',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_nonlegible_page_user_map t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (117, 'encounter_status_map',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_status_map t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (118, 'encounter_status_map_history',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_status_map_history t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (119, 'encounter_suppressed_hcc',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_suppressed_hcc t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (120, 'encounter_trumped_hcc',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_trumped_hcc t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (121, 'discussion_mst',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.discussion_mst t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (122, 'claim_detail',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.claim_detail t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (123, 'codes_backup',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.encounter_id)',
    CONCAT('FROM ', @MAIN_DB, '.codes_backup t JOIN encounters_to_archive_tmp x ON x.id = t.encounter_id')),
  (124, 'bucket_mst',
    'EXISTS (SELECT 1 FROM projects_to_archive_tmp x WHERE x.id = t.project_id)',
    CONCAT('FROM ', @MAIN_DB, '.bucket_mst t JOIN projects_to_archive_tmp x ON x.id = t.project_id')),
  (125, 'project_auto_accept_code_config',
    'EXISTS (SELECT 1 FROM projects_to_archive_tmp x WHERE x.id = t.project_id)',
    CONCAT('FROM ', @MAIN_DB, '.project_auto_accept_code_config t JOIN projects_to_archive_tmp x ON x.id = t.project_id')),
  (126, 'project_compliance_config',
    'EXISTS (SELECT 1 FROM projects_to_archive_tmp x WHERE x.id = t.project_id)',
    CONCAT('FROM ', @MAIN_DB, '.project_compliance_config t JOIN projects_to_archive_tmp x ON x.id = t.project_id')),
  (127, 'project_hcc_model_map',
    'EXISTS (SELECT 1 FROM projects_to_archive_tmp x WHERE x.id = t.project_id)',
    CONCAT('FROM ', @MAIN_DB, '.project_hcc_model_map t JOIN projects_to_archive_tmp x ON x.id = t.project_id')),
  (128, 'project_ocr_engine_map',
    'EXISTS (SELECT 1 FROM projects_to_archive_tmp x WHERE x.id = t.project_id)',
    CONCAT('FROM ', @MAIN_DB, '.project_ocr_engine_map t JOIN projects_to_archive_tmp x ON x.id = t.project_id')),
  (129, 'project_risk_category_mst',
    'EXISTS (SELECT 1 FROM projects_to_archive_tmp x WHERE x.id = t.project_id)',
    CONCAT('FROM ', @MAIN_DB, '.project_risk_category_mst t JOIN projects_to_archive_tmp x ON x.id = t.project_id')),
  (130, 'user_project_map',
    'EXISTS (SELECT 1 FROM projects_to_archive_tmp x WHERE x.id = t.project_id)',
    CONCAT('FROM ', @MAIN_DB, '.user_project_map t JOIN projects_to_archive_tmp x ON x.id = t.project_id')),
  (131, 'user_saved_search',
    'EXISTS (SELECT 1 FROM projects_to_archive_tmp x WHERE x.id = t.project_id)',
    CONCAT('FROM ', @MAIN_DB, '.user_saved_search t JOIN projects_to_archive_tmp x ON x.id = t.project_id')),
  (132, 'compliance_gaps',
    'EXISTS (SELECT 1 FROM projects_to_archive_tmp x WHERE x.id = t.project_id)',
    CONCAT('FROM ', @MAIN_DB, '.compliance_gaps t JOIN projects_to_archive_tmp x ON x.id = t.project_id')),
  (133, 'configuration_mst',
    'EXISTS (SELECT 1 FROM projects_to_archive_tmp x WHERE x.id = t.project_id)',
    CONCAT('FROM ', @MAIN_DB, '.configuration_mst t JOIN projects_to_archive_tmp x ON x.id = t.project_id')),
  (134, 'client_product_map',
    'EXISTS (SELECT 1 FROM clients_to_archive_tmp x WHERE x.id = t.client_id)',
    CONCAT('FROM ', @MAIN_DB, '.client_product_map t JOIN clients_to_archive_tmp x ON x.id = t.client_id')),
  (135, 'client_configuration',
    'EXISTS (SELECT 1 FROM clients_to_archive_tmp x WHERE x.id = t.client_id)',
    CONCAT('FROM ', @MAIN_DB, '.client_configuration t JOIN clients_to_archive_tmp x ON x.id = t.client_id')),
  (136, 'customer_report_download_log',
    'EXISTS (SELECT 1 FROM clients_to_archive_tmp x WHERE x.id = t.client_id)',
    CONCAT('FROM ', @MAIN_DB, '.customer_report_download_log t JOIN clients_to_archive_tmp x ON x.id = t.client_id')),

  /* parents (must be last) */
  (200, 'document_mst',
    'EXISTS (SELECT 1 FROM documents_to_archive_tmp x WHERE x.id = t.id)',
    CONCAT('FROM ', @MAIN_DB, '.document_mst t JOIN documents_to_archive_tmp x ON x.id = t.id')),
  (201, 'encounter_mst',
    'EXISTS (SELECT 1 FROM encounters_to_archive_tmp x WHERE x.id = t.id)',
    CONCAT('FROM ', @MAIN_DB, '.encounter_mst t JOIN encounters_to_archive_tmp x ON x.id = t.id')),
  (202, 'client_mst',
    'EXISTS (SELECT 1 FROM clients_to_archive_tmp x WHERE x.id = t.id)',
    CONCAT('FROM ', @MAIN_DB, '.client_mst t JOIN clients_to_archive_tmp x ON x.id = t.id')),
  (203, 'project_mst',
    'EXISTS (SELECT 1 FROM projects_to_archive_tmp x WHERE x.id = t.id)',
    CONCAT('FROM ', @MAIN_DB, '.project_mst t JOIN projects_to_archive_tmp x ON x.id = t.id'));

  /* ======================================
     Iterate plan: copy -> validate -> delete
     ====================================== */
  DECLARE v_done INT DEFAULT 0;
  DECLARE v_seq INT; 
  DECLARE v_tbl VARCHAR(128);
  DECLARE v_where TEXT;
  DECLARE v_from TEXT;
  DECLARE cur CURSOR FOR SELECT seq, table_name, exists_pred, from_join FROM _work_plan ORDER BY seq;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

  OPEN cur;
  read_loop: LOOP
    FETCH cur INTO v_seq, v_tbl, v_where, v_from;
    IF v_done = 1 THEN LEAVE read_loop; END IF;

    /* Count rows in MAIN */
    SET @sql = CONCAT('SELECT COUNT(*) INTO @ct_main FROM ', @MAIN_DB, '.', v_tbl, ' t WHERE ', v_where);
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
    SET @rows_main := @ct_main;

    /* init log row */
    SET @sql = CONCAT('INSERT INTO ', @ARCH_DB, '.archival_log (run_id, table_name, rows_in_main, status) ',
                      'VALUES (', v_run_id, ', ''', v_tbl, ''', ', IFNULL(@rows_main,0), ', ''PENDING'')');
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    IF IFNULL(@rows_main,0) = 0 THEN
      SET @sql = CONCAT('UPDATE ', @ARCH_DB, '.archival_log ',
                        'SET status=''VALIDATED'', updated_at=NOW(), note=''No rows in scope'' ',
                        'WHERE run_id=', v_run_id, ' AND table_name=''', v_tbl, ''' AND status=''PENDING''');
      PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
      ITERATE read_loop;
    END IF;

    /* COPY to ARCH */
    SET @sql = CONCAT('INSERT INTO ', @ARCH_DB, '.', v_tbl, ' SELECT t.* ', ' ', v_from);
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
    SET @rows_ins := ROW_COUNT();

    SET @sql = CONCAT('UPDATE ', @ARCH_DB, '.archival_log ',
                      'SET rows_copied=', IFNULL(@rows_ins,0), ', status=''COPIED'', updated_at=NOW() ',
                      'WHERE run_id=', v_run_id, ' AND table_name=''', v_tbl, ''' AND status IN (''PENDING'',''COPIED'')');
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    /* VALIDATE counts in ARCH */
    SET @sql = CONCAT('SELECT COUNT(*) INTO @ct_arch FROM ', @ARCH_DB, '.', v_tbl, ' t WHERE ', v_where);
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    IF IFNULL(@ct_arch,0) <> IFNULL(@rows_main,0) OR IFNULL(@rows_ins,0) <> IFNULL(@rows_main,0) THEN
      SET @msg := CONCAT('Count mismatch on ', v_tbl,
                         ' main=', IFNULL(@rows_main,0),
                         ' inserted=', IFNULL(@rows_ins,0),
                         ' arch=', IFNULL(@ct_arch,0));
      SET @sql = CONCAT('UPDATE ', @ARCH_DB, '.archival_log SET status=''FAILED'', note=? , updated_at=NOW() ',
                        'WHERE run_id=', v_run_id, ' AND table_name=''', v_tbl, '''');
      PREPARE s FROM @sql; SET @note := @msg; EXECUTE s USING @note; DEALLOCATE PREPARE s;
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Validation failed';
    END IF;

    SET @sql = CONCAT('UPDATE ', @ARCH_DB, '.archival_log ',
                      'SET status=''VALIDATED'', updated_at=NOW() ',
                      'WHERE run_id=', v_run_id, ' AND table_name=''', v_tbl, '''');
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

    /* DELETE from MAIN */
    SET @sql = CONCAT('DELETE t FROM ', @MAIN_DB, '.', v_tbl, ' t WHERE ', v_where);
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
    SET @rows_del := ROW_COUNT();

    IF IFNULL(@rows_del,0) <> IFNULL(@rows_main,0) THEN
      SET @msg := CONCAT('Delete mismatch on ', v_tbl,
                         ' expected=', IFNULL(@rows_main,0),
                         ' deleted=', IFNULL(@rows_del,0));
      SET @sql = CONCAT('UPDATE ', @ARCH_DB, '.archival_log SET status=''FAILED'', rows_deleted=', IFNULL(@rows_del,0), ', note=? , updated_at=NOW() ',
                        'WHERE run_id=', v_run_id, ' AND table_name=''', v_tbl, '''');
      PREPARE s FROM @sql; SET @note := @msg; EXECUTE s USING @note; DEALLOCATE PREPARE s;
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Delete failed';
    END IF;

    SET @sql = CONCAT('UPDATE ', @ARCH_DB, '.archival_log ',
                      'SET rows_deleted=', IFNULL(@rows_del,0), ', status=''DELETED'', updated_at=NOW() ',
                      'WHERE run_id=', v_run_id, ' AND table_name=''', v_tbl, '''');
    PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;

  END LOOP;
  CLOSE cur;

  /* restore FK checks and commit */
  SET FOREIGN_KEY_CHECKS = v_old_fk;
  COMMIT;

  SET @sql = CONCAT('UPDATE ', @ARCH_DB, '.archival_run ',
                    'SET status=''SUCCESS'', ended_at=NOW() WHERE run_id=', v_run_id);
  PREPARE s FROM @sql; EXECUTE s; DEALLOCATE PREPARE s;
END$$

DELIMITER ;
