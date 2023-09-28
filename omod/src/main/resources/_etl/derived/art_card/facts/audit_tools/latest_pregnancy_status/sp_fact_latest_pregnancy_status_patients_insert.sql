-- $BEGIN
INSERT INTO mamba_fact_patients_latest_pregnancy_status(client_id,
                                                       encounter_date,
                                                       status)
SELECT a.client_id,encounter_date,
       IF(pregnant='YES','Pregnant',
           IF(pregnant='NO','Not Pregnant',pregnant)) AS family_planning_status
FROM mamba_fact_encounter_hiv_art_card b
         JOIN
     (SELECT client_id, MAX(encounter_date) AS latest_encounter_date
      FROM mamba_fact_encounter_hiv_art_card
      WHERE pregnant IS NOT NULL
      GROUP BY client_id) a
     ON a.client_id = b.client_id AND encounter_date = latest_encounter_date;
-- $END