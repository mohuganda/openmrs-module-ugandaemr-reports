-- $BEGIN
CALL sp_fact_medication_orders_create();
CALL sp_fact_medication_orders_insert();
CALL sp_fact_medication_orders_update();
-- $END