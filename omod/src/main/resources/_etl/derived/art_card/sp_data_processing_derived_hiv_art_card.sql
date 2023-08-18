-- $BEGIN
-- CALL sp_dim_client_hiv_hts;
CALL sp_fact_encounter_hiv_art_card;
CALL sp_fact_encounter_hiv_art_summary;
CALL sp_fact_encounter_hiv_art_health_education;
CALL sp_fact_active_in_care;
CALL sp_fact_latest_adherence_patients;
CALL sp_fact_latest_advanced_disease_patients;
CALL sp_fact_latest_arv_days_dispensed_patients;
CALL sp_fact_latest_current_regimen_patients;
CALL sp_fact_latest_family_planning_patients;
CALL sp_fact_latest_hepatitis_b_test_patients;
CALL sp_fact_latest_viral_load_patients;
CALL sp_fact_latest_iac_decision_outcome_patients;
CALL sp_fact_latest_iac_sessions_patients;
CALL sp_fact_latest_index_tested_children_patients;
CALL sp_fact_latest_index_tested_children_status_patients;
CALL sp_fact_latest_index_tested_partners_patients;
CALL sp_fact_latest_index_tested_partners_status_patients;
CALL sp_fact_latest_nutrition_assesment_patients;
CALL sp_fact_latest_nutrition_support_patients;
CALL sp_fact_latest_regimen_line_patients;
CALL sp_fact_latest_return_date_patients;
CALL sp_fact_latest_tb_status_patients;
CALL sp_fact_latest_tpt_status_patients;
CALL sp_fact_latest_viral_load_ordered_patients;
CALL sp_fact_latest_vl_after_iac_patients;
CALL sp_fact_latest_who_stage_patients;
CALL sp_fact_marital_status_patients;
CALL sp_fact_nationality_patients;
CALL sp_fact_latest_patient_demographics_patients;
CALL sp_fact_art_patients;
CALL sp_fact_current_arv_regimen_start_date;
CALL sp_fact_calhiv_patients;

-- $END