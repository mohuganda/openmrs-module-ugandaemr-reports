-- $BEGIN
INSERT INTO mamba_fact_non_suppressed_repeat_vl (encounter_id,
                                               client_id,
                                                 obs_datetime,
                                                 obs_group_id,
                                                 vl_sample_collection,
                                                 hivdr_sample_Collection,
                                                 vl_repeat_date,
                                                 iac_results,
                                                 copies ,
                                                 date_vl_received,
                                                 hivdr_results_received,
                                                 hivdr_results,
                                                 hivdr_result_date)
SELECT
    og.encounter_id,
    og.person_id,
    og.obs_datetime,
    og.obs_id AS obs_group_id,

    MAX(CASE WHEN o.concept_id = 199121 THEN o.value_coded END) AS vl_sample_collected,
    MAX(CASE WHEN o.concept_id = 164989 THEN o.value_coded END) AS hivdr_sample_sample_collected,
    MAX(CASE WHEN o.concept_id = 163023 THEN o.value_datetime END) AS vl_repeat_date,
    MAX(CASE WHEN o.concept_id = 1305 THEN o.value_coded END) AS iac_results,
    MAX(CASE WHEN o.concept_id = 856 THEN o.value_numeric END) AS copies,
    MAX(CASE WHEN o.concept_id = 163150 THEN o.value_datetime END) AS recieved_vl_date,
    MAX(CASE WHEN o.concept_id = 199122 THEN o.value_coded END) AS hivdr_results_received,
    MAX(CASE WHEN o.concept_id = 165824 THEN o.value_text END) AS hivdr_results,
    MAX(CASE WHEN o.concept_id = 165823 THEN o.value_datetime END) AS hivdr_results_date

FROM
    obs og
        LEFT JOIN obs o ON o.obs_group_id = og.obs_id AND o.voided = 0

WHERE
    og.concept_id = 163157

  AND og.voided = 0
GROUP BY
    og.obs_id, og.encounter_id, og.person_id, og.obs_datetime;
-- $END