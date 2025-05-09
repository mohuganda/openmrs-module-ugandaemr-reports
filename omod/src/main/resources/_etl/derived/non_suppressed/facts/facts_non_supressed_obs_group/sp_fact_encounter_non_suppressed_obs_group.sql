-- $BEGIN
CALL sp_fact_encounter_non_suppressed_obs_group_create();
CALL sp_fact_encounter_non_suppressed_obs_group_insert();
CALL sp_fact_encounter_non_suppressed_obs_group_update();
-- $END