-- $BEGIN
CREATE TABLE mamba_fact_non_suppressed_repeat_vl
(
    id                   INT AUTO_INCREMENT,
    client_id            INT  NULL,
    encounter_id           INT NULL,
    obs_group_id            INT NULL,
    obs_datetime            DATETIME NULL,
    vl_sample_collection      VARCHAR(50) NULL,
    hivdr_sample_Collection     VARCHAR(100) NULL,
    vl_repeat_date                   DATE NULL,
    iac_results                VARCHAR(250) NULL,
    copies                INT NULL,
    date_vl_received                   DATE NULL,
    hivdr_results_received                  DATE NULL,
    hivdr_results                   VARCHAR(250) NULL,
    hivdr_result_date                   DATE NULL,

    PRIMARY KEY (id)
) CHARSET = UTF8;

CREATE INDEX
    mamba_fact_non_suppressed_repeat_vl_client_id_index ON mamba_fact_non_suppressed_repeat_vl (client_id);
CREATE INDEX
    mamba_fact_non_suppressed_repeat_vl_encounter_id_index ON mamba_fact_non_suppressed_repeat_vl (encounter_id);
CREATE INDEX
    mamba_fact_non_suppressed_repeat_vl_obs_group_id_index ON mamba_fact_non_suppressed_repeat_vl (obs_group_id);


-- $END

