-- $BEGIN
INSERT INTO mamba_fact_medication_orders(order_id, client_id,
                                         drug_concept_id,
                                         encounter_id,
                                         instructions,
                                         date_activated,
                                         urgency,
                                         order_number,
                                         dose,
                                         dose_units,
                                         quantity,
                                         quantity_units,
                                         duration,
                                         duration_units)
SELECT o.order_id,
       patient_id   AS client,
       o.concept_id AS drug,
       encounter_id,
       instructions,
       date_activated,
       urgency,
       order_number,
       dose,
       cn3.name     AS dose_units,
       quantity,
       cn1.name     AS quantity_units,
       duration,
       cn2.name     AS duration_units
FROM orders o
         INNER JOIN order_type ot ON o.order_type_id = ot.order_type_id
         INNER JOIN drug_order d_o ON o.order_id = d_o.order_id
         LEFT JOIN concept_name cn1 ON d_o.quantity_units = cn1.concept_id
    AND cn1.locale = 'en' AND cn1.concept_name_type = 'FULLY_SPECIFIED' AND cn1.locale_preferred = 1
         LEFT JOIN concept_name cn2 ON d_o.duration_units = cn2.concept_id AND cn2.locale_preferred = 1
    AND cn2.locale = 'en' AND cn2.concept_name_type = 'FULLY_SPECIFIED'
         LEFT JOIN concept_name cn3 ON d_o.dose_units = cn3.concept_id AND cn3.locale_preferred = 1
    AND cn3.locale = 'en' AND cn3.concept_name_type = 'FULLY_SPECIFIED'

WHERE ot.uuid = '131168f4-15f5-102d-96e4-000c29c2a5d7'
  AND o.voided = 0
;
-- $END