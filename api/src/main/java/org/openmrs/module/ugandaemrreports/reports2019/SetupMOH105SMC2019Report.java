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
        dsd.addDimension("age", ReportUtils.map(dimensionLibrary.ANCAgeGroups(), "effectiveDate=${endDate}"));
        dsd.addDimension("gender", ReportUtils.map(dimensionLibrary.gender()));

        dsd.addParameter(new Parameter("startDate", "Start Date", Date.class));
        dsd.addParameter(new Parameter("endDate", "End Date", Date.class));


        addRowWithColumns(dsd, "SMC01","Number of females with a first ANC Visit ",indicatorLibrary.ANCFirstContact() );
        addRowWithColumns(dsd, "SMC02","Number of females with a fourth contact  ANC Visit ",indicatorLibrary.ANCFourthVisit());


        return dsd;
    }

    public void addRowWithColumns(CohortIndicatorDataSetDefinition dsd, String key, String label, CohortIndicator cohortIndicator) {

        addIndicator(dsd, key + "aF", label + " (Below 15 Years) Female", cohortIndicator, "age=Below15");
        addIndicator(dsd, key + "bF", label + " (Between 15 and 19 Years) Female", cohortIndicator, "age=Between15And19yrs");
        addIndicator(dsd, key + "cF", label + " (Between 20 and 24 Years) Female", cohortIndicator, "age=Between20And24yrs");
        addIndicator(dsd, key + "dF", label + " (Between 25 and 49 Years) Female", cohortIndicator, "age=Between25And49yrs");
        addIndicator(dsd, key + "eF", label + " (>50) Female", cohortIndicator, "age=GreaterThan50yrs");
        addIndicator(dsd, key + "fF", label + " (Total) ", cohortIndicator, "");
    }

    public void addIndicator(CohortIndicatorDataSetDefinition dsd, String key, String label, CohortIndicator cohortIndicator, String dimensionOptions) {
        dsd.addColumn(key, label, ReportUtils.map(cohortIndicator, PARAMS), dimensionOptions);

    }

    @Override
    public String getVersion() {
        return "2.1.0.3";
    }
}
