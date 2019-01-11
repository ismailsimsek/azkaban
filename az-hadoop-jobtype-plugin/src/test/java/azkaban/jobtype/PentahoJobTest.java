package azkaban.jobtype;

import static org.junit.Assert.assertEquals;

import azkaban.utils.Props;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import azkaban.test;

public class PentahoJobTest {

  Props testSysProps = AllJobExecutorTests.setUpCommonProps();
  Props testJobProps = AllJobExecutorTests.setUpCommonProps();

  private Props testProps;
  Logger logger = Logger.getRootLogger();

  @Before
  public void beforeMethod() throws Exception {
    azkaban.test.Utils.initServiceProvider();
  }


  @Test
  public void getJavaClass() {
  }

  @Test
  public void createCommandLine() throws Exception {
    String expectedCMD = "";

    testJobProps.put("Xms", "2G");
    testJobProps.put("Xmx", "4G");
    testJobProps.put("pentaho.data_integration_root_dir", "src/test/resources/pentaho/data-integration");
    testJobProps.put("pentaho.file", "src/test/resources/pentaho/etl/parameterized_job.kjb");
    testJobProps.put("pentaho.param.level", "Detailed");
    testJobProps.put("pentaho.param.DEMO_PARAMETER_1", "Value of Pentaho ETL DEMO_PARAMETER_1!");

    PentahoJob job = new PentahoJob("-1",testSysProps,testJobProps,logger);
    logger.info(job.createCommandLine());
    assertEquals(expectedCMD,job.createCommandLine());

  }
}