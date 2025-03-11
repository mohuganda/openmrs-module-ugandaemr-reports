package org.openmrs.module.ugandaemrreports.definition.dataset.evaluator;

import com.google.common.base.Joiner;
import org.openmrs.annotation.Handler;
import org.openmrs.module.reporting.common.DateUtil;
import org.openmrs.module.reporting.common.ObjectUtil;
import org.openmrs.module.reporting.dataset.DataSet;
import org.openmrs.module.reporting.dataset.DataSetRow;
import org.openmrs.module.reporting.dataset.SimpleDataSet;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.evaluator.DataSetEvaluator;
import org.openmrs.module.reporting.evaluation.EvaluationContext;
import org.openmrs.module.reporting.evaluation.EvaluationException;
import org.openmrs.module.reporting.evaluation.querybuilder.SqlQueryBuilder;
import org.openmrs.module.reporting.evaluation.service.EvaluationService;
import org.openmrs.module.ugandaemrreports.common.PatientDataHelper;
import org.openmrs.module.ugandaemrreports.common.PersonDemographics;
import org.openmrs.module.ugandaemrreports.definition.dataset.definition.VLExchangeDatasetDefinition;
import org.springframework.beans.factory.annotation.Autowired;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.openmrs.module.ugandaemrreports.reports.Helper.*;


/**
 */
@Handler(supports = {VLExchangeDatasetDefinition.class})
public class VLExchangeDatasetDefinitionEvaluator implements DataSetEvaluator {

    @Autowired
    EvaluationService evaluationService;


    Map<Integer,String> drugNames = new HashMap<>();
    @Override
    public DataSet evaluate(DataSetDefinition dataSetDefinition, EvaluationContext context) throws EvaluationException {

        VLExchangeDatasetDefinition definition = (VLExchangeDatasetDefinition) dataSetDefinition;


        SimpleDataSet dataSet = new SimpleDataSet(dataSetDefinition, context);
        PatientDataHelper pdh = new PatientDataHelper();

        String startDate = DateUtil.formatDate(definition.getStartDate(), "yyyy-MM-dd");
        String endDate = DateUtil.formatDate(definition.getEndDate(), "yyyy-MM-dd");

        startDate = startDate+" 00:00:00";
        endDate = endDate+" 23:59:59";
        context = ObjectUtil.nvl(context, new EvaluationContext());

        String dataQuery = String.format("SELECT pi.identifier                                  AS hiv_clinic_no,\n" +
                "       p.birthdate,\n" +
                "       TIMESTAMPDIFF(YEAR, p.birthdate, CURRENT_DATE) AS age,\n" +
                "       p.gender,\n" +
                "       CAST(accession_number as CHAR )                               AS specimen_id,\n" +
                "       specimen.name as specimen_source,\n" +
                "       DATE(date_activated)                           AS vl_collection_date,\n" +
                "       DATE(send_request_sync_task.date_sent)         AS send_request_date_sent,\n" +
                "       send_request_sync_task.status                  AS send_request_status,\n" +
                "       send_request_sync_task.status_code             AS send_request_status_code,\n" +
                "       program_data_task.status                       AS program_data_status,\n" +
                "       program_data_task.status_code                  AS program_data_status_code,\n" +
                "       DATE(program_data_task.date_created)           AS program_data_date,\n" +
                "       DATE(request_result_task.date_created)         AS request_results_date,\n" +
                "       request_result_task.status                     AS request_results,\n" +
                "       request_result_task.status_code                AS request_status_code,\n" +
                "audittool.baseline_regimen_start_date,\n" +
                "nin.identifier as NIN,\n" +
                "audittool.who_stage,\n" +
                "preg.status,\n" +
                "audittool.tuberculosis_status,\n" +
                "audittool.adherence,\n" +
                "dsd.model as dsd_model,\n" +
                "vl_indication.value,\n" +
                "audittool.current_regimen,\n" +
                "audittool.regimen_line,\n" +
                "(SELECT property_value from global_property where property ='ugandaemr.dhis2.organizationuuid') as facility_id,\n" +
                "(SELECT property_value from global_property where property ='ugandaemr.healthCenterName') as facility_name,\n" +
                "audittool.arv_regimen_start_date as current_regimen_duration\n" +
                "FROM (SELECT orders.patient_id, orders.date_activated, orders.accession_number, test_order.specimen_source\n" +
                "      FROM orders\n" +
                "               INNER JOIN test_order ON (test_order.order_id = orders.order_id)\n" +
                "      WHERE accession_number IS NOT NULL\n" +
                "        AND specimen_source IS NOT NULL\n" +
                "        AND orders.instructions = 'REFER TO cphl'\n" +
                "        AND orders.concept_id = 165412\n" +
                "        AND orders.voided = 0\n" +
                "        AND orders.date_activated >= '%s'\n" +
                "        AND orders.date_activated <= '%s') cohort\n" +
                "         LEFT JOIN person p ON p.person_id = cohort.patient_id\n" +
                "         LEFT JOIN concept_name specimen\n" +
                "                   ON cohort.specimen_source = specimen.concept_id AND specimen.concept_name_type = 'FULLY_SPECIFIED' AND\n" +
                "                      specimen.locale = 'en' and specimen.locale_preferred=1\n" +
                "         LEFT JOIN sync_task send_request_sync_task ON send_request_sync_task.sync_task = accession_number AND\n" +
                "                                                       send_request_sync_task.sync_task_type = (SELECT sync_task_type_id\n" +
                "                                                                                                FROM sync_task_type\n" +
                "                                                                                                WHERE uuid = '3551ca84-06c0-432b-9064-fcfeefd6f4ec')\n" +
                "         LEFT JOIN sync_task request_result_task ON request_result_task.sync_task = accession_number AND\n" +
                "                                                    request_result_task.sync_task_type = (SELECT sync_task_type_id\n" +
                "                                                                                          FROM sync_task_type\n" +
                "                                                                                          WHERE uuid = '3396dcf0-2106-4e73-9b90-c63978c3a8b4')\n" +
                "         LEFT JOIN sync_task program_data_task ON program_data_task.sync_task = accession_number AND\n" +
                "                                                  program_data_task.sync_task_type = (SELECT sync_task_type_id\n" +
                "                                                                                      FROM sync_task_type\n" +
                "                                                                                      WHERE uuid = 'f9b2fa5d-5d37-4fd9-b20a-a0cab664f520')\n" +
                "         LEFT JOIN patient_identifier pi ON pi.patient_id = cohort.patient_id AND pi.identifier_type =\n" +
                "                                                                                  (SELECT patient_identifier_type_id\n" +
                "                                                                                   FROM patient_identifier_type\n" +
                "                                                                                   WHERE uuid = 'e1731641-30ab-102d-86b0-7a5022ba4115')\n" +
                "        LEFT JOIN patient_identifier nin ON nin.patient_id = cohort.patient_id AND nin.identifier_type =\n" +
                "                                                                                      (SELECT patient_identifier_type_id\n" +
                "                                                                                       FROM patient_identifier_type\n" +
                "                                                                                       WHERE uuid = 'f0c16a6d-dc5f-4118-a803-616d0075d282')\n" +
                "         LEFT JOIN mamba_fact_audit_tool_art_patients audittool ON audittool.client_id = cohort.patient_id\n" +
                "\n" +
                "         LEFT JOIN mamba_fact_patients_latest_pregnancy_status preg ON preg.client_id = cohort.patient_id\n" +
                "         LEFT JOIN (SELECT latest.person_id, cn.name AS model\n" +
                "                    FROM obs o\n" +
                "                             INNER JOIN (SELECT person_id, MAX(obs_datetime) latest\n" +
                "                                         FROM obs o\n" +
                "                                         WHERE concept_id = 165143\n" +
                "                                           AND voided = 0\n" +
                "                                         GROUP BY person_id) latest ON o.person_id = latest.person_id\n" +
                "                             LEFT JOIN concept_name cn\n" +
                "                                       ON o.value_coded = cn.concept_id AND cn.concept_name_type = 'FULLY_SPECIFIED' AND\n" +
                "                                          cn.locale = 'en'\n" +
                "                    WHERE o.concept_id = 165143\n" +
                "                      AND o.obs_datetime = latest.latest) dsd ON dsd.person_id = cohort.patient_id\n" +
                "         LEFT JOIN (SELECT latest.person_id, cn.name AS value\n" +
                "                    FROM obs o\n" +
                "                             INNER JOIN (SELECT person_id, MAX(obs_datetime) latest\n" +
                "                                         FROM obs o\n" +
                "                                         WHERE concept_id = 168689\n" +
                "                                           AND voided = 0\n" +
                "                                         GROUP BY person_id) latest ON o.person_id = latest.person_id\n" +
                "                             LEFT JOIN concept_name cn\n" +
                "                                       ON o.value_coded = cn.concept_id AND cn.concept_name_type = 'FULLY_SPECIFIED' AND\n" +
                "                                          cn.locale = 'en'\n" +
                "                    WHERE o.concept_id = 168689\n" +
                "                      AND o.obs_datetime = latest.latest) vl_indication ON vl_indication.person_id = cohort.patient_id;\n" ,startDate,endDate);
        SqlQueryBuilder q = new SqlQueryBuilder();
        q.append(dataQuery);

        List<Object[]> results = evaluationService.evaluateToList(q, context);

        if(!results.isEmpty()) {
            for (Object[] e : results) {
                DataSetRow row = new DataSetRow();
                pdh.addCol(row, "HIV Clinic No", e[0]);
                pdh.addCol(row, "Birthdate", e[1]);
                pdh.addCol(row, "Age", e[2]);
                pdh.addCol(row, "Sex", e[3]);
                pdh.addCol(row, "Specimen ID", String.valueOf(e[4]));
                pdh.addCol(row, "specimen_source", e[5]);
                pdh.addCol(row, "Date Ordered", e[6]);
                pdh.addCol(row, "send_request_date_sent", e[7]);
                pdh.addCol(row, "send_request_status", e[8]);
                pdh.addCol(row, "send_request_status_code", e[9]);
                pdh.addCol(row, "program_data_status", e[10]);
                pdh.addCol(row, "program_data_status_code", e[11]);
                pdh.addCol(row, "program_data_date", e[12]);
                pdh.addCol(row, "request_results_date", e[13]);
                pdh.addCol(row, "request_results", e[14]);
                pdh.addCol(row, "request_status_code", e[15]);
                pdh.addCol(row, "NIN", e[17]);
                pdh.addCol(row, "Art_start_date", e[16]);
                pdh.addCol(row, "who", e[18]);
                pdh.addCol(row, "duration", e[28]);
                pdh.addCol(row, "preg", e[19]);
                pdh.addCol(row, "tb_status", e[20]);
                pdh.addCol(row, "adherence", e[21]);
                pdh.addCol(row, "dsd", e[22]);
                pdh.addCol(row, "indication", e[23]);
                pdh.addCol(row, "regimen", e[24]);
                pdh.addCol(row, "line", e[25]);
                pdh.addCol(row, "facility_id", e[26]);
                pdh.addCol(row, "facility_name", e[27]);

                dataSet.addRow(row);
            }

        }
        return dataSet;
    }

}
