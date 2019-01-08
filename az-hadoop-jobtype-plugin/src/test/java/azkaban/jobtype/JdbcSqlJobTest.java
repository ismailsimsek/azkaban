package azkaban.jobtype;

import azkaban.jobExecutor.AbstractProcessJob;
import azkaban.jobExecutor.ProcessJob;
import azkaban.utils.Props;
import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JdbcSqlJobTest {

  private JdbcSqlJob job;
  private final Logger log = Logger.getLogger(ProcessJob.class);
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private Props props = null;

  @Before
  public void setUp() throws IOException {
    final File workingDir = this.temp.newFolder("TestProcess");

    // Initialize job
    this.props = AllJobExecutorTests.setUpCommonProps();
    this.props.put(AbstractProcessJob.WORKING_DIR, workingDir.getCanonicalPath());
    this.props.put("type", "jdbcSql");

    this.props.put("jdbcSql.file", "src/test/resources/plugins/jobtypes/jdbcSql/testSQL.sql");
    this.props.put("jdbcSql.file.1", "src/test/resources/plugins/jobtypes/jdbcSql/testSQL_1.sql");
    this.props.put("jdbcSql.file.2", "src/test/resources/plugins/jobtypes/jdbcSql/testSQL_2.sql");
    this.props.put("jdbcSql.preexecution_file",
        "src/test/resources/plugins/jobtypes/jdbcSql/testpreSQL.sql");
    this.props.put("jdbcSql.postexecution_file",
        "src/test/resources/plugins/jobtypes/jdbcSql/testpostSQL.sql");
    // clean derby db if exists
    FileUtils.deleteDirectory(new File("tempderbydb"));
    this.props.put("jdbcSql.myxyzDB.connectionurl", "jdbc:derby:tempderbydb/mydb;create=true");
    this.props.put("jdbcSql.myxyzDB.username", "root");
    this.props.put("jdbcSql.myxyzDB.password", "");
    this.props.put("jdbcSql.database", "myxyzDB");
    this.props.put("jdbcSql.sqlparam.schema_name", "TEST_SCHEMA");
    this.props.put("jdbcSql.sqlparam.table_name", "PERSONS");
    // log sql statements
    this.props.put("jdbcSql.logsql", "true");
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File("tempderbydb"));
  }

  @Test(expected = AssertionError.class)
  public void testCheckJobParameters() {
    this.props.removeLocal("jdbcSql.file");
    this.job = new JdbcSqlJob("TestProcess", this.props, this.props, this.log);
  }

  @Test(expected = AssertionError.class)
  public void testCheckJobParameters2() {
    this.props.removeLocal("jdbcSql.myxyzDB.password");
    this.job = new JdbcSqlJob("TestProcess", this.props, this.props, this.log);
  }

  @Test
  public void testrun() throws Exception {
    this.job = new JdbcSqlJob("TestProcess", this.props, this.props, this.log);
    this.job.run();
  }

}