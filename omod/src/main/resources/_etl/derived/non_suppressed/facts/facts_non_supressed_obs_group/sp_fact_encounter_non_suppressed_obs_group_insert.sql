-- $BEGIN
INSERT INTO mamba_fact_non_suppressed_obs_group (encounter_id,
                                               client_id,
                                                 obs_datetime,
                                                 obs_group_id,
                                                 session_date,
                                                 adherence_code,
                                                 score )
SELECT
    og.encounter_id,
    og.person_id,
    og.obs_datetime,
    og.obs_id AS obs_group_id,

    MAX(CASE WHEN o.concept_id = 163154 THEN o.value_datetime END) AS session_date,
    MAX(CASE WHEN o.concept_id = 90221 THEN cn.name END) AS adherence_code,
    MAX(CASE WHEN o.concept_id = 163155 THEN o.value_numeric END) AS score

FROM
    obs og
        LEFT JOIN obs o ON o.obs_group_id = og.obs_id AND o.voided = 0
        LEFT JOIN concept_name cn
                  ON o.value_coded = cn.concept_id AND cn.locale = 'en' AND cn.voided = 0 and cn.concept_name_type='FULLY_SPECIFIED'
WHERE
    og.concept_id = 163153

  AND og.voided = 0
GROUP BY
    og.obs_id, og.encounter_id, og.person_id, og.obs_datetime;
-- $END