package org.openmrs.module.ugandaemrreports.reports2019;

import org.openmrs.module.reporting.dataset.definition.CohortIndicatorDataSetDefinition;
import org.openmrs.module.reporting.dataset.definition.DataSetDefinition;
import org.openmrs.module.reporting.evaluation.parameter.Mapped;
import org.openmrs.module.reporting.evaluation.parameter.Parameter;
import org.openmrs.module.reporting.indicator.CohortIndicator;
import org.openmrs.module.reporting.report.ReportDesign;
import org.openmrs.module.reporting.report.definition.ReportDefinition;
import org.openmrs.module.ugandaemrreports.library.DataFactory;
import org.openmrs.module.ugandaemrreports.library.Moh105IndicatorLibrary;
import org.openmrs.module.ugandaemrreports.reporting.library.dimension.CommonReportDimensionLibrary;
import org.openmrs.module.ugandaemrreports.reporting.utils.ReportUtils;
import org.openmrs.module.ugandaemrreports.reports.UgandaEMRDataExportManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.openmrs.module.ugandaemrreports.library.CommonDatasetLibrary.period;
import static org.openmrs.module.ugandaemrreports.library.CommonDatasetLibrary.settings;


@Component

public class SetupMOH105SMC2019Report extends UgandaEMRDataExportManager {

    @Autowired
    private DataFactory df;


    @Autowired
    private CommonReportDimensionLibrary dimensionLibrary;

    @Autowired
    private Moh105IndicatorLibrary indicatorLibrary;

    private static final String PARAMS = "startDate=${startDate},endDate=${endDate}";


    /**
     * @return the uuid for the report design for exporting to Excel
     */
    @Override
    public String getExcelDesignUuid() {
        return "0b095617-de87-4f7c-a15b-a53b9a2e43b4";
    }


    @Override
    public String getUuid() {
        return "d7c9aca3-1640-4761-ba9c-32dbbf109ecf";
    }

    @Override
    public String getName() {
        return "HMIS 105 Section 2: SMC ";
    }

    @Override
    public String getDescription() {
        return "HMIS 105 Section 2: SMC";
    }

    @Override
    public List<Parameter> getParameters() {
        List<Parameter> l = new ArrayList<Parameter>();
        l.add(df.getStartDateParameter());
        l.add(df.getEndDateParameter());
        return l;
    }

    @Override
    public List<ReportDesign> constructReportDesigns(ReportDefinition reportDefinition) {
        List<ReportDesign> l = new ArrayList<ReportDesign>();
        l.add(buildReportDesign(reportDefinition));

        return l;
    }

    /**
     * Build the report design for the specified report, this allows a user to override the report design by adding
     * properties and other metadata to the report design
     *
     * @param reportDefinition
     * @return The report design
     */
    @Override

    public ReportDesign buildReportDesign(ReportDefinition reportDefinition) {
        ReportDesign rd = createExcelTemplateDesign(getExcelDesignUuid(), reportDefinition, "SMC_105_Section.xls");
        return rd;
    }




    @Override
    public ReportDefinition constructReportDefinition() {

        ReportDefinition rd = new ReportDefinition();

        rd.setUuid(getUuid());
        rd.setName(getName());
        rd.setDescription(getDescription());
        rd.setParameters(getParameters());

        rd.addDataSetDefinition("S", Mapped.mapStraightThrough(settings()));
        rd.addDataSetDefinition("105", Mapped.mapStraightThrough(antentalDataSetDefinition()));
        rd.addDataSetDefinition("P", Mapped.mapStraightThrough(period()));
        return rd;

    }

    protected DataSetDefinition antentalDataSetDefinition() {
        CohortIndicatorDataSetDefinition dsd = new CohortIndicatorDataSetDefinition();
        dsd.setParameters(getParameters());
        dsd.addDimension("age", ReportUtils.map(dimensionLibrary.SMCAgeGroups(), "effectiveDate=${endDate}"));
        dsd.addDimension("gender", ReportUtils.map(dimensionLibrary.gender()));

        dsd.addParameter(new Parameter("startDate", "Start Date", Date.class));
        dsd.addParameter(new Parameter("endDate", "End Date", Date.class));


        addRowWithColumns(dsd, "SMC01","Number of females with a first ANC Visit ",indicatorLibrary.ANCEighthVisit() );
        addRowWithColumns(dsd, "SMC02","Number of females with a fourth contact  ANC Visit ",indicatorLibrary.ANCFourthVisit());


        return dsd;
    }

    public void addRowWithColumns(CohortIndicatorDataSetDefinition dsd, String key, String label, CohortIndicator cohortIndicator) {

        addIndicator(dsd, key + "aM", label + " (Between 0 and 60 days) Male", cohortIndicator, "age=Below2Months");
        addIndicator(dsd, key + "bM", label + " (Between 2 Months and a Year) Male", cohortIndicator, "age=Between2MonthsAnd1Year");
        addIndicator(dsd, key + "cM", label + " (Between 1 and 9 Year) Male", cohortIndicator, "age=Between1And9Year");
        addIndicator(dsd, key + "dM", label + " (Between 10 and 14 Year) Male", cohortIndicator, "age=Between10And14Year");
        addIndicator(dsd, key + "eM", label + " (Between 15 and 19 Year) Male", cohortIndicator, "age=Between15And19Year");
        addIndicator(dsd, key + "fM", label + " (Between 20 and 24 Year) Male", cohortIndicator, "age=Between20And24Year");
        addIndicator(dsd, key + "gM", label + " (Between 25 and 29 Year) Male", cohortIndicator, "age=Between25And29Year");
        addIndicator(dsd, key + "hM", label + " (Between 30 and 34 Year) Male", cohortIndicator, "age=Between30And34Year");
        addIndicator(dsd, key + "iM", label + " (Between 35 and 39 Year) Male", cohortIndicator, "age=Between35And39Year");
        addIndicator(dsd, key + "jM", label + " (Between 40 and 44 Year) Male", cohortIndicator, "age=Between40And44yrs");
        addIndicator(dsd, key + "kM", label + " (Between 45 and 49 Year) Male", cohortIndicator, "age=Between45And49yrs");
        addIndicator(dsd, key + "lM", label + " (Greater than 50) Male", cohortIndicator, "age=GreaterThan50yrs");
        addIndicator(dsd, key + "mM", label + " (Between 45 and 49 Year) Male", cohortIndicator, "age=Between45And49yrs");




    }

    public void addIndicator(CohortIndicatorDataSetDefinition dsd, String key, String label, CohortIndicator cohortIndicator, String dimensionOptions) {
        dsd.addColumn(key, label, ReportUtils.map(cohortIndicator, PARAMS), dimensionOptions);

    }

    @Override
    public String getVersion() {
        return "2.1.0.3";
    }
}
