package azkaban.jobtype;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import azkaban.utils.Props;

public class PentahoJobTest {

  Props testSysProps = new Props();
  Props testJobProps = new Props();

  private Props testProps;
  Logger logger = Logger.getRootLogger();

  @Before
  public void beforeMethod() throws Exception {
    KettleEnvironment.init(false);
    String expectedCMD = "";

    testJobProps.put("Xms", "2G");
    testJobProps.put("Xmx", "4G");
    testJobProps.put("pentaho.data_integration_root_dir", "src/test/resources/pentaho/data-integration");
    testJobProps.put("pentaho.file", "src/test/resources/pentaho/etl/parameterized_job.kjb");
    testJobProps.put("pentaho.param.level", "Detailed");
    testProps.put("pentaho.param.DEMO_PARAMETER_1", "Value of Pentaho ETL DEMO_PARAMETER_1!");

    PentahoJob job = new PentahoJob("-1",testSysProps,testJobProps,logger);
    logger.info(job.createCommandLine());
    assertEquals(expectedCMD,job.createCommandLine());
  }


  @Test
  public void getJavaClass() {
  }

  @Test
  public void createCommandLine() {


  }
}