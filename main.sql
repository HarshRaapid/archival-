CREATE DEFINER=`azureuser`@`%` PROCEDURE `archive_project_1`(IN p_project_names_csv TEXT )
BEGIN
    
    DECLARE v_run_id BIGINT DEFAULT NULL;
    DECLARE v_old_fk INT DEFAULT 1;
    DECLARE v_tbl_processing VARCHAR(128) DEFAULT 'INITIALIZATION';


   
   
    
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 @sqlstate = RETURNED_SQLSTATE, @errno = MYSQL_ERRNO, @text = MESSAGE_TEXT;
        SET @full_error = CONCAT('ERROR ', @errno, ' (', @sqlstate, ') processing table `', v_tbl_processing, '`: ', @text);

        ROLLBACK;
        SET FOREIGN_KEY_CHECKS = v_old_fk;

        IF v_run_id IS NOT NULL THEN
            UPDATE `ra_audit_apigateway_archival`.archival_run SET status='FAILED', ended_at=NOW(), error_message = @full_error WHERE run_id = v_run_id;
        END IF;
    END;

    
    SET v_tbl_processing = 'archival_run';
    INSERT INTO `ra_audit_apigateway_archival`.archival_run (project_list) VALUES (p_project_names_csv);
    SET v_run_id = LAST_INSERT_ID();

    SET v_tbl_processing = 'Temp ID Set Creation';

    
    DROP TEMPORARY TABLE IF EXISTS projects_to_archive_tmp;
    CREATE TEMPORARY TABLE projects_to_archive_tmp AS SELECT id, name, client_id FROM `ra_audit_apigateway`.project_mst WHERE FIND_IN_SET(name, p_project_names_csv);
    ALTER TABLE projects_to_archive_tmp ADD PRIMARY KEY (id), ADD INDEX idx_cli (client_id);

    DROP TEMPORARY TABLE IF EXISTS clients_to_archive_tmp;
    CREATE TEMPORARY TABLE clients_to_archive_tmp AS SELECT DISTINCT c.id FROM `ra_audit_apigateway`.client_mst c JOIN projects_to_archive_tmp p ON p.client_id = c.id;
    ALTER TABLE clients_to_archive_tmp ADD PRIMARY KEY (id);

    DROP TEMPORARY TABLE IF EXISTS encounters_to_archive_tmp;
    CREATE TEMPORARY TABLE encounters_to_archive_tmp AS SELECT e.id FROM `ra_audit_apigateway`.encounter_mst e JOIN projects_to_archive_tmp p ON p.id = e.project_id;
    ALTER TABLE encounters_to_archive_tmp ADD PRIMARY KEY (id);

    DROP TEMPORARY TABLE IF EXISTS documents_to_archive_tmp;
    CREATE TEMPORARY TABLE documents_to_archive_tmp AS SELECT d.id FROM `ra_audit_apigateway`.document_mst d JOIN encounters_to_archive_tmp e ON e.id = d.encounter_id;
    ALTER TABLE documents_to_archive_tmp ADD PRIMARY KEY (id);

    DROP TEMPORARY TABLE IF EXISTS document_codes_to_archive_tmp;
    CREATE TEMPORARY TABLE document_codes_to_archive_tmp AS SELECT dc.id FROM `ra_audit_apigateway`.document_code dc JOIN documents_to_archive_tmp dt ON dt.id = dc.document_id;
    ALTER TABLE document_codes_to_archive_tmp ADD PRIMARY KEY (id);

    DROP TEMPORARY TABLE IF EXISTS cm_codes_to_archive_tmp;
    CREATE TEMPORARY TABLE cm_codes_to_archive_tmp AS SELECT c.id FROM `ra_audit_apigateway`.cm_code c JOIN encounters_to_archive_tmp e ON e.id = c.encounter_id;
    ALTER TABLE cm_codes_to_archive_tmp ADD PRIMARY KEY (id);

    DROP TEMPORARY TABLE IF EXISTS encounter_dos_to_archive_tmp;
    CREATE TEMPORARY TABLE encounter_dos_to_archive_tmp AS SELECT ed.id FROM `ra_audit_apigateway`.encounter_dos ed JOIN encounters_to_archive_tmp e ON e.id = ed.encounter_id;
    ALTER TABLE encounter_dos_to_archive_tmp ADD PRIMARY KEY (id);

    DROP TEMPORARY TABLE IF EXISTS document_dos_to_archive_tmp;
    CREATE TEMPORARY TABLE document_dos_to_archive_tmp AS SELECT dd.id FROM `ra_audit_apigateway`.document_dos dd JOIN documents_to_archive_tmp d ON d.id = dd.document_id;
    ALTER TABLE document_dos_to_archive_tmp ADD PRIMARY KEY (id);

    DROP TEMPORARY TABLE IF EXISTS document_evidence_to_archive_tmp;
    CREATE TEMPORARY TABLE document_evidence_to_archive_tmp AS SELECT de.id FROM `ra_audit_apigateway`.document_evidence de JOIN documents_to_archive_tmp d ON d.id = de.document_id;
    ALTER TABLE document_evidence_to_archive_tmp ADD PRIMARY KEY (id);

    DROP TEMPORARY TABLE IF EXISTS discussions_to_archive_tmp;
    CREATE TEMPORARY TABLE discussions_to_archive_tmp AS SELECT dm.id FROM `ra_audit_apigateway`.discussion_mst dm JOIN encounters_to_archive_tmp e ON e.id = dm.encounter_id;
    ALTER TABLE discussions_to_archive_tmp ADD PRIMARY KEY (id);

    DROP TEMPORARY TABLE IF EXISTS compliance_gaps_to_archive_tmp;
    CREATE TEMPORARY TABLE compliance_gaps_to_archive_tmp AS SELECT cg.id FROM `ra_audit_apigateway`.compliance_gaps cg JOIN projects_to_archive_tmp p ON p.id = cg.project_id;
    ALTER TABLE compliance_gaps_to_archive_tmp ADD PRIMARY KEY (id);

    DROP TEMPORARY TABLE IF EXISTS configurations_to_archive_tmp;
    CREATE TEMPORARY TABLE configurations_to_archive_tmp AS SELECT c.id FROM `ra_audit_apigateway`.configuration_mst c JOIN projects_to_archive_tmp p ON p.id = c.project_id;
    ALTER TABLE configurations_to_archive_tmp ADD PRIMARY KEY (id);

    DROP TEMPORARY TABLE IF EXISTS claim_details_to_archive_tmp;
    CREATE TEMPORARY TABLE claim_details_to_archive_tmp AS SELECT cd.id FROM `ra_audit_apigateway`.claim_detail cd JOIN encounters_to_archive_tmp e ON e.id = cd.encounter_id;
    ALTER TABLE claim_details_to_archive_tmp ADD PRIMARY KEY (id);

    
    SET v_old_fk = @@FOREIGN_KEY_CHECKS;
    SET FOREIGN_KEY_CHECKS = 0;
    START TRANSACTION;

    
    SET v_tbl_processing = 'document_code_evidence_map';
    INSERT INTO `ra_audit_apigateway_archival`.`document_code_evidence_map` SELECT t.* FROM `ra_audit_apigateway`.`document_code_evidence_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_code_evidence_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_code_meat_evidence_map';
    INSERT INTO `ra_audit_apigateway_archival`.`document_code_meat_evidence_map` SELECT t.* FROM `ra_audit_apigateway`.`document_code_meat_evidence_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_code_meat_evidence_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;
    
    SET v_tbl_processing = 'document_code_combination_map';
    INSERT INTO `ra_audit_apigateway_archival`.`document_code_combination_map` SELECT t.* FROM `ra_audit_apigateway`.`document_code_combination_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_code_combination_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_code_hcc_group_map';
    INSERT INTO `ra_audit_apigateway_archival`.`document_code_hcc_group_map` SELECT t.* FROM `ra_audit_apigateway`.`document_code_hcc_group_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_code_hcc_group_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_code_cms_hcc_v28_group_map';
    INSERT INTO `ra_audit_apigateway_archival`.`document_code_cms_hcc_v28_group_map` SELECT t.* FROM `ra_audit_apigateway`.`document_code_cms_hcc_v28_group_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_code_cms_hcc_v28_group_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_code_hhs_hcc_group_map';
    INSERT INTO `ra_audit_apigateway_archival`.`document_code_hhs_hcc_group_map` SELECT t.* FROM `ra_audit_apigateway`.`document_code_hhs_hcc_group_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_code_hhs_hcc_group_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_code_rxhcc_hcc_group_map';
    INSERT INTO `ra_audit_apigateway_archival`.`document_code_rxhcc_hcc_group_map` SELECT t.* FROM `ra_audit_apigateway`.`document_code_rxhcc_hcc_group_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_code_rxhcc_hcc_group_map` t JOIN document_codes_to_archive_tmp k ON t.document_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;
    
    SET v_tbl_processing = 'document_evidence_coordinates';
    INSERT INTO `ra_audit_apigateway_archival`.`document_evidence_coordinates` SELECT t.* FROM `ra_audit_apigateway`.`document_evidence_coordinates` t JOIN document_evidence_to_archive_tmp k ON t.document_evidence_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_evidence_coordinates` t JOIN document_evidence_to_archive_tmp k ON t.document_evidence_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;
    
    SET v_tbl_processing = 'document_dos_evidence_map';
    INSERT INTO `ra_audit_apigateway_archival`.`document_dos_evidence_map` SELECT t.* FROM `ra_audit_apigateway`.`document_dos_evidence_map` t JOIN document_dos_to_archive_tmp k ON t.document_dos_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_dos_evidence_map` t JOIN document_dos_to_archive_tmp k ON t.document_dos_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'cm_code_comment';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code_comment` SELECT t.* FROM `ra_audit_apigateway`.`cm_code_comment` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code_comment` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;
    
    SET v_tbl_processing = 'cm_code_evidence_map';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code_evidence_map` SELECT t.* FROM `ra_audit_apigateway`.`cm_code_evidence_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code_evidence_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'cm_code_history';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code_history` SELECT t.* FROM `ra_audit_apigateway`.`cm_code_history` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code_history` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'cm_code_meat_evidence_map';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code_meat_evidence_map` SELECT t.* FROM `ra_audit_apigateway`.`cm_code_meat_evidence_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code_meat_evidence_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'cm_code_reject_reason_map';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code_reject_reason_map` SELECT t.* FROM `ra_audit_apigateway`.`cm_code_reject_reason_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code_reject_reason_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'cm_code_combination_map';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code_combination_map` SELECT t.* FROM `ra_audit_apigateway`.`cm_code_combination_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code_combination_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'cm_code_hcc_group_map';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code_hcc_group_map` SELECT t.* FROM `ra_audit_apigateway`.`cm_code_hcc_group_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code_hcc_group_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'cm_code_cms_hcc_v28_group_map';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code_cms_hcc_v28_group_map` SELECT t.* FROM `ra_audit_apigateway`.`cm_code_cms_hcc_v28_group_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code_cms_hcc_v28_group_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'cm_code_hhs_hcc_group_map';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code_hhs_hcc_group_map` SELECT t.* FROM `ra_audit_apigateway`.`cm_code_hhs_hcc_group_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code_hhs_hcc_group_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'cm_code_rxhcc_hcc_group_map';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code_rxhcc_hcc_group_map` SELECT t.* FROM `ra_audit_apigateway`.`cm_code_rxhcc_hcc_group_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code_rxhcc_hcc_group_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'cm_code_suppressed_cm_code_map';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code_suppressed_cm_code_map` SELECT t.* FROM `ra_audit_apigateway`.`cm_code_suppressed_cm_code_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code_suppressed_cm_code_map` t JOIN cm_codes_to_archive_tmp k ON t.cm_code_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_dos_comment';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_dos_comment` SELECT t.* FROM `ra_audit_apigateway`.`encounter_dos_comment` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_dos_comment` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_dos_evidence_map';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_dos_evidence_map` SELECT t.* FROM `ra_audit_apigateway`.`encounter_dos_evidence_map` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_dos_evidence_map` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_dos_last_worked';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_dos_last_worked` SELECT t.* FROM `ra_audit_apigateway`.`encounter_dos_last_worked` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_dos_last_worked` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_dos_page_range_detail';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_dos_page_range_detail` SELECT t.* FROM `ra_audit_apigateway`.`encounter_dos_page_range_detail` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_dos_page_range_detail` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_dos_reject_reason_map';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_dos_reject_reason_map` SELECT t.* FROM `ra_audit_apigateway`.`encounter_dos_reject_reason_map` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_dos_reject_reason_map` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_dos_standard_comment';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_dos_standard_comment` SELECT t.* FROM `ra_audit_apigateway`.`encounter_dos_standard_comment` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_dos_standard_comment` t JOIN encounter_dos_to_archive_tmp k ON t.encounter_dos_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'discussion_comment';
    INSERT INTO `ra_audit_apigateway_archival`.`discussion_comment` SELECT t.* FROM `ra_audit_apigateway`.`discussion_comment` t JOIN discussions_to_archive_tmp k ON t.discussion_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`discussion_comment` t JOIN discussions_to_archive_tmp k ON t.discussion_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'discussion_user_map';
    INSERT INTO `ra_audit_apigateway_archival`.`discussion_user_map` SELECT t.* FROM `ra_audit_apigateway`.`discussion_user_map` t JOIN discussions_to_archive_tmp k ON t.discussion_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`discussion_user_map` t JOIN discussions_to_archive_tmp k ON t.discussion_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'claim_code_map';
    INSERT INTO `ra_audit_apigateway_archival`.`claim_code_map` SELECT t.* FROM `ra_audit_apigateway`.`claim_code_map` t JOIN claim_details_to_archive_tmp k ON t.claim_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`claim_code_map` t JOIN claim_details_to_archive_tmp k ON t.claim_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'claim_procedure_code_map';
    INSERT INTO `ra_audit_apigateway_archival`.`claim_procedure_code_map` SELECT t.* FROM `ra_audit_apigateway`.`claim_procedure_code_map` t JOIN claim_details_to_archive_tmp k ON t.claim_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`claim_procedure_code_map` t JOIN claim_details_to_archive_tmp k ON t.claim_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'compliance_gaps_details';
    INSERT INTO `ra_audit_apigateway_archival`.`compliance_gaps_details` SELECT t.* FROM `ra_audit_apigateway`.`compliance_gaps_details` t JOIN compliance_gaps_to_archive_tmp k ON t.compliance_gaps_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`compliance_gaps_details` t JOIN compliance_gaps_to_archive_tmp k ON t.compliance_gaps_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'compliance_gaps_evidence_map';
    INSERT INTO `ra_audit_apigateway_archival`.`compliance_gaps_evidence_map` SELECT t.* FROM `ra_audit_apigateway`.`compliance_gaps_evidence_map` t JOIN compliance_gaps_to_archive_tmp k ON t.compliance_gaps_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`compliance_gaps_evidence_map` t JOIN compliance_gaps_to_archive_tmp k ON t.compliance_gaps_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'configuration_mst_history';
    INSERT INTO `ra_audit_apigateway_archival`.`configuration_mst_history` SELECT t.* FROM `ra_audit_apigateway`.`configuration_mst_history` t JOIN configurations_to_archive_tmp k ON t.configuration_mst_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`configuration_mst_history` t JOIN configurations_to_archive_tmp k ON t.configuration_mst_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    
    SET v_tbl_processing = 'document_code';
    INSERT INTO `ra_audit_apigateway_archival`.`document_code` SELECT t.* FROM `ra_audit_apigateway`.`document_code` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_code` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;
    
    SET v_tbl_processing = 'document_demographic';
    INSERT INTO `ra_audit_apigateway_archival`.`document_demographic` SELECT t.* FROM `ra_audit_apigateway`.`document_demographic` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_demographic` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_dos';
    INSERT INTO `ra_audit_apigateway_archival`.`document_dos` SELECT t.* FROM `ra_audit_apigateway`.`document_dos` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_dos` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_evidence';
    INSERT INTO `ra_audit_apigateway_archival`.`document_evidence` SELECT t.* FROM `ra_audit_apigateway`.`document_evidence` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_evidence` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_page_detail';
    INSERT INTO `ra_audit_apigateway_archival`.`document_page_detail` SELECT t.* FROM `ra_audit_apigateway`.`document_page_detail` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_page_detail` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_processing_detail';
    INSERT INTO `ra_audit_apigateway_archival`.`document_processing_detail` SELECT t.* FROM `ra_audit_apigateway`.`document_processing_detail` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_processing_detail` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_review_map';
    INSERT INTO `ra_audit_apigateway_archival`.`document_review_map` SELECT t.* FROM `ra_audit_apigateway`.`document_review_map` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_review_map` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_section_map';
    INSERT INTO `ra_audit_apigateway_archival`.`document_section_map` SELECT t.* FROM `ra_audit_apigateway`.`document_section_map` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_section_map` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'document_sentence';
    INSERT INTO `ra_audit_apigateway_archival`.`document_sentence` SELECT t.* FROM `ra_audit_apigateway`.`document_sentence` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_sentence` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'genai_processing_detail';
    INSERT INTO `ra_audit_apigateway_archival`.`genai_processing_detail` SELECT t.* FROM `ra_audit_apigateway`.`genai_processing_detail` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`genai_processing_detail` t JOIN documents_to_archive_tmp k ON t.document_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'cm_code';
    INSERT INTO `ra_audit_apigateway_archival`.`cm_code` SELECT t.* FROM `ra_audit_apigateway`.`cm_code` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`cm_code` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_dos';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_dos` SELECT t.* FROM `ra_audit_apigateway`.`encounter_dos` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_dos` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_cms_hcc_config_map';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_cms_hcc_config_map` SELECT t.* FROM `ra_audit_apigateway`.`encounter_cms_hcc_config_map` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_cms_hcc_config_map` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_event_map';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_event_map` SELECT t.* FROM `ra_audit_apigateway`.`encounter_event_map` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_event_map` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_more_specific_less_specific_code_map';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_more_specific_less_specific_code_map` SELECT t.* FROM `ra_audit_apigateway`.`encounter_more_specific_less_specific_code_map` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_more_specific_less_specific_code_map` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_nonlegible_page_detail';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_nonlegible_page_detail` SELECT t.* FROM `ra_audit_apigateway`.`encounter_nonlegible_page_detail` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_nonlegible_page_detail` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_nonlegible_page_user_map';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_nonlegible_page_user_map` SELECT t.* FROM `ra_audit_apigateway`.`encounter_nonlegible_page_user_map` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_nonlegible_page_user_map` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_status_map';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_status_map` SELECT t.* FROM `ra_audit_apigateway`.`encounter_status_map` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_status_map` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_status_map_history';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_status_map_history` SELECT t.* FROM `ra_audit_apigateway`.`encounter_status_map_history` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_status_map_history` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_suppressed_hcc';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_suppressed_hcc` SELECT t.* FROM `ra_audit_apigateway`.`encounter_suppressed_hcc` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_suppressed_hcc` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_trumped_hcc';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_trumped_hcc` SELECT t.* FROM `ra_audit_apigateway`.`encounter_trumped_hcc` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_trumped_hcc` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'discussion_mst';
    INSERT INTO `ra_audit_apigateway_archival`.`discussion_mst` SELECT t.* FROM `ra_audit_apigateway`.`discussion_mst` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`discussion_mst` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'claim_detail';
    INSERT INTO `ra_audit_apigateway_archival`.`claim_detail` SELECT t.* FROM `ra_audit_apigateway`.`claim_detail` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`claim_detail` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'codes_backup';
    INSERT INTO `ra_audit_apigateway_archival`.`codes_backup` SELECT t.* FROM `ra_audit_apigateway`.`codes_backup` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`codes_backup` t JOIN encounters_to_archive_tmp k ON t.encounter_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;
    
    SET v_tbl_processing = 'project_auto_accept_code_config';
    INSERT INTO `ra_audit_apigateway_archival`.`project_auto_accept_code_config` SELECT t.* FROM `ra_audit_apigateway`.`project_auto_accept_code_config` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`project_auto_accept_code_config` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'project_compliance_config';
    INSERT INTO `ra_audit_apigateway_archival`.`project_compliance_config` SELECT t.* FROM `ra_audit_apigateway`.`project_compliance_config` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`project_compliance_config` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'project_hcc_model_map';
    INSERT INTO `ra_audit_apigateway_archival`.`project_hcc_model_map` SELECT t.* FROM `ra_audit_apigateway`.`project_hcc_model_map` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`project_hcc_model_map` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'project_ocr_engine_map';
    INSERT INTO `ra_audit_apigateway_archival`.`project_ocr_engine_map` SELECT t.* FROM `ra_audit_apigateway`.`project_ocr_engine_map` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`project_ocr_engine_map` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'project_risk_category_mst';
    INSERT INTO `ra_audit_apigateway_archival`.`project_risk_category_mst` SELECT t.* FROM `ra_audit_apigateway`.`project_risk_category_mst` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`project_risk_category_mst` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'user_project_map';
    INSERT INTO `ra_audit_apigateway_archival`.`user_project_map` SELECT t.* FROM `ra_audit_apigateway`.`user_project_map` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`user_project_map` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'user_saved_search';
    INSERT INTO `ra_audit_apigateway_archival`.`user_saved_search` SELECT t.* FROM `ra_audit_apigateway`.`user_saved_search` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`user_saved_search` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'compliance_gaps';
    INSERT INTO `ra_audit_apigateway_archival`.`compliance_gaps` SELECT t.* FROM `ra_audit_apigateway`.`compliance_gaps` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`compliance_gaps` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'configuration_mst';
    INSERT INTO `ra_audit_apigateway_archival`.`configuration_mst` SELECT t.* FROM `ra_audit_apigateway`.`configuration_mst` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`configuration_mst` t JOIN projects_to_archive_tmp k ON t.project_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;
    
    SET v_tbl_processing = 'client_product_map';
    INSERT INTO `ra_audit_apigateway_archival`.`client_product_map` SELECT t.* FROM `ra_audit_apigateway`.`client_product_map` t JOIN clients_to_archive_tmp k ON t.client_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`client_product_map` t JOIN clients_to_archive_tmp k ON t.client_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'client_configuration';
    INSERT INTO `ra_audit_apigateway_archival`.`client_configuration` SELECT t.* FROM `ra_audit_apigateway`.`client_configuration` t JOIN clients_to_archive_tmp k ON t.client_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`client_configuration` t JOIN clients_to_archive_tmp k ON t.client_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'customer_report_download_log';
    INSERT INTO `ra_audit_apigateway_archival`.`customer_report_download_log` SELECT t.* FROM `ra_audit_apigateway`.`customer_report_download_log` t JOIN clients_to_archive_tmp k ON t.client_id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`customer_report_download_log` t JOIN clients_to_archive_tmp k ON t.client_id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    
    SET v_tbl_processing = 'document_mst';
    INSERT INTO `ra_audit_apigateway_archival`.`document_mst` SELECT t.* FROM `ra_audit_apigateway`.`document_mst` t JOIN documents_to_archive_tmp k ON t.id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`document_mst` t JOIN documents_to_archive_tmp k ON t.id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'encounter_mst';
    INSERT INTO `ra_audit_apigateway_archival`.`encounter_mst` SELECT t.* FROM `ra_audit_apigateway`.`encounter_mst` t JOIN encounters_to_archive_tmp k ON t.id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`encounter_mst` t JOIN encounters_to_archive_tmp k ON t.id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'client_mst';
    INSERT INTO `ra_audit_apigateway_archival`.`client_mst` SELECT t.* FROM `ra_audit_apigateway`.`client_mst` t JOIN clients_to_archive_tmp k ON t.id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`client_mst` t JOIN clients_to_archive_tmp k ON t.id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    SET v_tbl_processing = 'project_mst';
    INSERT INTO `ra_audit_apigateway_archival`.`project_mst` SELECT t.* FROM `ra_audit_apigateway`.`project_mst` t JOIN projects_to_archive_tmp k ON t.id = k.id;
    SET @rows_copied = ROW_COUNT();
    DELETE t FROM `ra_audit_apigateway`.`project_mst` t JOIN projects_to_archive_tmp k ON t.id = k.id;
    SET @rows_deleted = ROW_COUNT();
    IF @rows_copied <> @rows_deleted THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Count mismatch'; END IF;
    IF @rows_copied > 0 THEN 
        INSERT INTO `ra_audit_apigateway_archival`.archival_log (run_id, table_name, status, rows_in_main, rows_copied, rows_deleted) VALUES (v_run_id, v_tbl_processing, 'SUCCESS', @rows_copied, @rows_copied, @rows_deleted);
    END IF;

    
    SET v_tbl_processing = 'Finalizing';
    COMMIT;
    SET FOREIGN_KEY_CHECKS = v_old_fk;

    
    UPDATE `ra_audit_apigateway_archival`.archival_run SET status='SUCCESS', ended_at=NOW() WHERE run_id = v_run_id;