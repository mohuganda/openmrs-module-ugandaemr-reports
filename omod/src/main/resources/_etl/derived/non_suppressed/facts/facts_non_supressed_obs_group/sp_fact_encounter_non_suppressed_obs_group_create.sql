-- $BEGIN
CREATE TABLE mamba_fact_non_suppressed_obs_group
(
    id                   INT AUTO_INCREMENT,
    client_id            INT  NULL,
    encounter_id           INT NULL,
    obs_group_id            INT NULL,
    obs_datetime            DATETIME NULL,
    session_date            DATE NULL,
    adherence_code                   VARCHAR(250) NULL,
    score                   INT NULL,
    PRIMARY KEY (id)
) CHARSET = UTF8;

CREATE INDEX
    mamba_fact_non_suppressed_obs_group_client_id_index ON mamba_fact_non_suppressed_obs_group (client_id);
CREATE INDEX
    mamba_fact_non_suppressed_obs_group_encounter_id_index ON mamba_fact_non_suppressed_obs_group (encounter_id);
CREATE INDEX
    mamba_fact_non_suppressed_obs_group_obs_group_id_index ON mamba_fact_non_suppressed_obs_group (obs_group_id);


-- $END

