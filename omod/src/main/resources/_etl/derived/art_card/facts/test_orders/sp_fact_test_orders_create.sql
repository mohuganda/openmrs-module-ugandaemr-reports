-- $BEGIN
CREATE TABLE mamba_fact_test_orders
(
    id        INT AUTO_INCREMENT,
    client_id INT NULL,
    order_id    INT NOT NULL,
    test_concept_id  INT NOT NULL,
    test_name        VARCHAR(255) NULL,
    encounter_id INT  NULL,
    orderer     INT NULL,
    instructions VARCHAR(255) NULL,
    date_activated DATE NULL,
    date_stopped   DATE NULL,
    accession_number VARCHAR(200) NULL,
    order_number     VARCHAR(150) NULL,
    specimen_source VARCHAR(250) NULL,
        PRIMARY KEY (id)
) CHARSET = UTF8;

CREATE INDEX
    mamba_fact_test_orders_client_id_index ON mamba_fact_test_orders (client_id);
CREATE INDEX
    mamba_fact_test_orders_order_id_index ON mamba_fact_test_orders (order_id);


-- $END

