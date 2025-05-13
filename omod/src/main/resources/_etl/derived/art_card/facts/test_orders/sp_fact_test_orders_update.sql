-- $BEGIN
UPDATE mamba_fact_medication_orders mo
    JOIN (
        SELECT cn.concept_id, cn.name
        FROM concept_name cn
        WHERE cn.locale = 'en'
          AND cn.voided = 0
          AND (
            cn.locale_preferred = 1
                OR cn.concept_name_type = 'FULLY_SPECIFIED'
            )
    ) best_names ON best_names.concept_id = mo.drug_concept_id
SET mo.drug = best_names.name;
-- $END