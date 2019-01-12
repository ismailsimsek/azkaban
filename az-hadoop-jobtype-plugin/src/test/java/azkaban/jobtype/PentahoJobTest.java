package azkaban.jobtype;

import static org.junit.Assert.assertEquals;

import azkaban.jobExecutor.AllJobExecutorTests;
import azkaban.utils.Props;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class PentahoJobTest {

  private final Props testSysProps = AllJobExecutorTests.setUpCommonProps();
  private final Props testJobProps = AllJobExecutorTests.setUpCommonProps();

  private Props testProps;
  private final Logger logger = Logger.getRootLogger();

  @Before
  public void beforeMethod() {
    azkaban.test.Utils.initServiceProvider();

    testJobProps.put("Xms", "2G");
    testJobProps.put("Xmx", "4G");
    testJobProps
        .put("pentaho.data_integration_root_dir",
            "src/test/resources/plugins/jobtypes/pentaho/data-integration");
    testJobProps.put("pentaho.file",
        "src/test/resources/plugins/jobtypes/pentaho/etl/parameterized_job.kjb");
    testJobProps.put("pentaho.param.level", "Detailed");
    testJobProps.put("pentaho.param.DEMO_PARAMETER_1", "Value of Pentaho ETL DEMO_PARAMETER_1");
    testJobProps.put("jvm.args", "-Dhttps.protocols=TLSv1,TLSv1.1,TLSv1.2");
    testJobProps.put("jvm.args.1", "-DKETTLE_HOME=");
    testJobProps.put("jvm.args.2", "-DKETTLE_USER=");
    testJobProps.put("jvm.args.3", "-DKETTLE_PASSWORD=");
  }


  @Test
  public void getJavaClass() throws Exception {
    PentahoJob job = new PentahoJob("-1", testSysProps, testJobProps, logger);
    assertEquals("org.pentaho.di.kitchen.Kitchen", job.getJavaClass("parameterized_job.kjb"));

  }

  @Test(expected = Exception.class)
  public void getPentahoLauncherJarNoLauncher() throws Exception {
    testJobProps.put("pentaho.data_integration_root_dir",
        "src/test/resources/plugins/jobtypes/pentaho/data-integration_without_launcher");
    PentahoJob job = new PentahoJob("-1", testSysProps, testJobProps, logger);
    job.run();
  }

  @Test
  public void createCommandLine() throws Exception {
    String launcher = "/Users/isimsek/development/azkaban/az-hadoop-jobtype-plugin/src/test"
        + "/resources/plugins/jobtypes/pentaho/data-integration/launcher/pentaho-application"
        + "-launcher-8.1.0.0-12.jar";

    String etlJob = "/Users/isimsek/development/azkaban/az-hadoop-jobtype-plugin/src/test/resources/plugins/jobtypes/pentaho/etl/parameterized_job.kjb";

    String expectedCMD =
        "java -Dhttps.protocols=TLSv1,TLSv1.1,TLSv1.2 -DKETTLE_HOME= -DKETTLE_USER= "
            + "-DKETTLE_PASSWORD= -Xms2G -Xmx4G -jar " + launcher + " -main org.pentaho.di.kitchen"
            + ".Kitchen -initialDir  -file " + etlJob
            + " \"-param:DEMO_PARAMETER_1=Value of Pentaho "
            + "ETL "
            + "DEMO_PARAMETER_1\" \"-param:level=Detailed\"";

    PentahoJob job = new PentahoJob("-1", testSysProps, testJobProps, logger);
    assertEquals(expectedCMD, job.createCommandLine(launcher, etlJob));

  }

  @Test
  public void createCommandLineTransformation() throws Exception {

    String launcher = "/Users/isimsek/development/azkaban/az-hadoop-jobtype-plugin/src/test"
        + "/resources/plugins/jobtypes/pentaho/data-integration/launcher/pentaho-application"
        + "-launcher-8.1.0.0-12.jar";

    String etlJob = "/Users/isimsek/development/azkaban/az-hadoop-jobtype-plugin/src/test/resources/plugins/jobtypes/pentaho/etl/capturing_rows.ktr";

    String expectedCMD =
        "java -Dhttps.protocols=TLSv1,TLSv1.1,TLSv1.2 -DKETTLE_HOME= -DKETTLE_USER= "
            + "-DKETTLE_PASSWORD= -Xms2G -Xmx4G -jar " + launcher + " -main org.pentaho.di.pan.Pan "
            + "-initialDir  -file " + etlJob + " \"-param:DEMO_PARAMETER_1=Value of Pentaho ETL "
            + "DEMO_PARAMETER_1\" \"-param:level=Detailed\"";

    PentahoJob job = new PentahoJob("-1", testSysProps, testJobProps, logger);
    assertEquals(expectedCMD, job.createCommandLine(launcher, etlJob));

  }
}