package azkaban.jobtype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.job.Job;

public class PentahoJobWrapperTest {

  private Properties testProps;
  Logger logger = Logger.getRootLogger();

  @Before
  public void beforeMethod() throws IOException, KettleException {
    KettleEnvironment.init(false);

    testProps = new Properties();
    testProps.put("pentahojob.file", "src/test/resources/etl/parameterized_job.kjb");
    testProps.put("other_namenodes", "xxxxxxx");
    testProps.put("pentahojob.level", "Detailed");
    testProps.put("pentahojob.param.DEMO_PARAMETER_1", "Detailed");
    testProps.put("pentahojob.param.abc_2", "xxxxxxx");
    testProps.put("sgddsg.param.abc_2", "xxxxxxx");
  }

  @Test
  public void testRunningTransformations() throws Exception {

    PentahoJobWrapper pentahoJob = new PentahoJobWrapper();
    // run a transformation from the file system
    Job j = pentahoJob.runJobFromFileSystem(this.testProps);

    for (int i = 0; i < 20; i++) {
      if (j.getStatus().equals("Finished")) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        // Ignore
      }
    }

    // A successfully completed job is in finished state
    assertEquals("Finished", j.getStatus());

    // A successfully completed job has no errors
    assertEquals(0, j.getResult().getNrErrors());

    // And a true grand result
    assertTrue(j.getResult().getResult());

  }

}