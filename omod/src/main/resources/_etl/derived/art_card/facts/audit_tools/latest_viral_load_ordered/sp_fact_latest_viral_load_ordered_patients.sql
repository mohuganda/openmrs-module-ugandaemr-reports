-- $BEGIN
CALL sp_fact_latest_viral_load_ordered_patients_create();
CALL sp_fact_latest_viral_load_ordered_patients_insert();
CALL sp_fact_latest_viral_load_ordered_patients_update();
-- $END