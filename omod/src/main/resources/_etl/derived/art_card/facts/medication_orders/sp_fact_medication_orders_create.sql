-- $BEGIN
CREATE TABLE mamba_fact_medication_orders
(
    id        INT AUTO_INCREMENT,
    client_id INT NULL,
    order_id    INT NOT NULL,
    drug_concept_id  INT NOT NULL,
    drug        VARCHAR(255) NULL,
    encounter_id INT  NULL,
    instructions   VARCHAR(255) NULL,
    date_activated  DATETIME,
    urgency         VARCHAR(250) NULL,
    order_number    VARCHAR(100) NULL,
    dose            INT NULL ,
    dose_units      VARCHAR(200) NULL,
    quantity        INT NULL,
    quantity_units VARCHAR(200) NULL,
    duration        INT NULL,
    duration_units  VARCHAR(200) NULL,
        PRIMARY KEY (id)
) CHARSET = UTF8;

CREATE INDEX
    mamba_fact_medication_orders_client_id_index ON mamba_fact_medication_orders (client_id);
CREATE INDEX
    mamba_fact_medication_orders_order_id_index ON mamba_fact_medication_orders (order_id);


-- $END

