-- $BEGIN
INSERT INTO mamba_fact_test_orders(order_id, client_id,
                                   encounter_id,
                                   test_concept_id,
                                   test_name, orderer,
                                   instructions,
                                   date_activated,
                                   date_stopped,
                                   accession_number,
                                   order_number,
                                   specimen_source)
SELECT o.order_id,
       patient_id,
       encounter_id,
       o.concept_id,
       cn.name  as test_name,
       orderer,
       instructions,
       date_activated,
       date_stopped,
       accession_number,
       order_number,
       cn1.name as specimen_source
FROM orders o
         INNER JOIN order_type ot ON o.order_type_id = ot.order_type_id
         INNER JOIN test_order t_o ON o.order_id = t_o.order_id
         LEFT JOIN concept_name cn ON o.concept_id = cn.concept_id
    AND cn.locale = 'en' AND cn.concept_name_type = 'FULLY_SPECIFIED' AND cn.locale_preferred = 1
         LEFT JOIN concept_name cn1 ON specimen_source = cn1.concept_id
    AND cn1.locale = 'en' AND cn1.concept_name_type = 'FULLY_SPECIFIED' AND cn1.locale_preferred = 1
WHERE ot.uuid = '52a447d3-a64a-11e3-9aeb-50e549534c5e'
  and o.voided = 0;
-- $END