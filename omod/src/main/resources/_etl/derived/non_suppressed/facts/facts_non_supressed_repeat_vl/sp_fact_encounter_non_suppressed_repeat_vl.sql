-- $BEGIN
CALL sp_fact_encounter_non_suppressed_repeat_vl_create();
CALL sp_fact_encounter_non_suppressed_repeat_vl_insert();
CALL sp_fact_encounter_non_suppressed_repeat_vl_update();
-- $END