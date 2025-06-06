-- $BEGIN
CREATE TABLE mamba_fact_encounter_hiv_art_summary
(
    id                                          INT AUTO_INCREMENT,
    encounter_id                                INT NULL,
    client_id                                   INT NULL,
    encounter_datetime                          DATE NULL,
    allergy                                     TEXT NULL,
    hepatitis_b_test_qualitative                VARCHAR(255) NULL,
    hepatitis_c_test_qualitative                VARCHAR(255) NULL,
    lost_to_followup                            VARCHAR(255) NULL,
    currently_in_school                         VARCHAR(255) NULL,
    pmtct                                       VARCHAR(255) NULL,
    entry_point_into_hiv_care                   VARCHAR(255) NULL,
    name_of_location_transferred_from           TEXT NULL,
    date_lost_to_followup                       VARCHAR(255) NULL,
    name_of_location_transferred_to             TEXT NULL,
    patient_unique_identifier                   VARCHAR(255) NULL,
    address                                     TEXT NULL,
    date_positive_hiv_test_confirmed            VARCHAR(255) NULL,
    hiv_care_status                             VARCHAR(255) NULL,
    treatment_supporter_telephone_number        TEXT NULL,
    transfered_out_to_another_facility          VARCHAR(255) NULL,
    prior_art                                   VARCHAR(255) NULL,
    post_exposure_prophylaxis                   VARCHAR(255) NULL,
    prior_art_not_transfer                      VARCHAR(255) NULL,
    baseline_regimen                            VARCHAR(255) NULL,
    transfer_in_regimen                         VARCHAR(255) NULL,
    baseline_weight                             VARCHAR(255) NULL,
    baseline_stage                              VARCHAR(255) NULL,
    baseline_cd4                                VARCHAR(255) NULL,
    baseline_pregnancy                          VARCHAR(255) NULL,
    name_of_family_member                       TEXT NULL,
    age_of_family_member                        VARCHAR(255) NULL,
    hiv_test                                    VARCHAR(255) NULL,
    hiv_test_facility                           TEXT NULL,
    other_care_entry_point                      TEXT NULL,
    treatment_supporter_tel_no_owner            TEXT NULL,
    treatment_supporter_name                    TEXT NULL,
    pep_regimen_start_date                      DATE NULL,
    pmtct_regimen_start_date                    DATE NULL,
    earlier_arv_not_transfer_regimen_start_date DATE NULL,
    transfer_in_regimen_start_date              DATE NULL,
    baseline_regimen_start_date                 DATE NULL,
    transfer_out_date                           DATE NULL,
    baseline_regimen_other                      TEXT NULL,
    transfer_in_regimen_other                   TEXT NULL,
    hep_b_prior_art                             VARCHAR(255) NULL,
    hep_b_prior_art_regimen_start_date          VARCHAR(255) NULL,
    baseline_lactating                          VARCHAR(255) NULL,
    age_unit                                    VARCHAR(255) NULL,
    eid_enrolled                                VARCHAR(255) NULL,
    drug_restart_date                           DATE NULL,
    relationship_to_patient                     VARCHAR(255) NULL,
    pre_exposure_prophylaxis                    VARCHAR(255) NULL,
    hts_special_category                        VARCHAR(255) NULL,
    special_category                            VARCHAR(255) NULL,
    other_special_category                      TEXT NULL,
    tpt_start_date                              VARCHAR(255) NULL,
    tpt_completion_date                         DATE NULL,
    treatment_interruption_type                 VARCHAR(255) NULL,
    treatment_interruption                      VARCHAR(255) NULL,
    treatment_interruption_stop_date            DATE NULL,
    treatment_interruption_reason               TEXT NULL,
    hepatitis_b_test_date                       DATE NULL,
    hepatitis_c_test_date                       DATE NULL,
    blood_sugar_test_date                       DATE NULL,
    pre_exposure_prophylaxis_start_date         DATE NULL,
    prep_duration_in_months                     VARCHAR(255) NULL,
    pep_duration_in_months                      VARCHAR(255) NULL,
    hep_b_duration_in_months                    VARCHAR(255) NULL,
    blood_sugar_test_result                     VARCHAR(255) NULL,
    pmtct_duration_in_months                    VARCHAR(255) NULL,
    earlier_arv_not_transfer_duration_in_months VARCHAR(255) NULL,
    family_member_hiv_status                    VARCHAR(255) NULL,
    family_member_hiv_test_date                 DATE NULL,
    hiv_enrollment_date                         DATE NULL,
    relationship_to_index_clients             VARCHAR(255) NULL,
    other_relationship_to_index_client        VARCHAR(255) NULL,

    PRIMARY KEY (id)
) CHARSET = UTF8;

CREATE INDEX
    mamba_fact_encounter_hiv_art_summary_client_id_index ON mamba_fact_encounter_hiv_art_summary (client_id);

CREATE INDEX
    mamba_fact_encounter_hiv_art_summary_encounter_id_index ON mamba_fact_encounter_hiv_art_summary (encounter_id);

CREATE INDEX
    mamba_fact_encounter_hiv_art_summary_encounter_date_index ON mamba_fact_encounter_hiv_art_summary (encounter_datetime);


-- $END

