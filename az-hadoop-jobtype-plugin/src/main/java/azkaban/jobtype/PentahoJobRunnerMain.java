/*
 * Copyright 2012 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package azkaban.jobtype;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Result;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.logging.KettleLogStore;
import org.pentaho.di.core.logging.LogLevel;
import org.pentaho.di.core.logging.LoggingBuffer;
import org.pentaho.di.core.parameters.UnknownParamException;
import org.pentaho.di.core.plugins.PluginRegistry;
import org.pentaho.di.core.plugins.RepositoryPluginType;
import org.pentaho.di.job.Job;
import org.pentaho.di.job.JobMeta;
import org.pentaho.di.repository.RepositoriesMeta;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.repository.RepositoryDirectoryInterface;
import org.pentaho.di.repository.RepositoryMeta;

public class PentahoJobRunnerMain {

  private static final Logger logger = Logger.getRootLogger();

  /**
   *
   * @param args
   * @throws Exception
   *
   **/

  public static void main(final String[] args) throws Exception {

    Properties jobProps = HadoopSecureWrapperUtils.loadAzkabanProps();
    
    

    Map<String, String> pentahoVarMap = new HashMap<String, String>();
    Enumeration e = jobProps.propertyNames();
    while (e.hasMoreElements()) {
      String key = (String) e.nextElement();
      logger.info(key +": "+jobProps.getProperty(key));
    }
    runJob(args, jobProps);
  }

  public static void runJob(String[] args, Properties jobProps) throws Exception {

    Job job = null;

    // run a job from the file system
    if (jobProps.containsKey("pentahojob.file")) {
      job = runJobFromFileSystem(jobProps);
    }
    // run a job from the repository
    else if (jobProps.containsKey("pentahojob.rep.name")) {
      runJobFromRepository(jobProps);
    } else {
      throw new Exception(
          "No Job parametter given! please set pentahojob.file of pentahojob.rep.* parametters");
    }

    // retrieve logging appender
    LoggingBuffer appender = KettleLogStore.getAppender();
    // retrieve logging lines for job
    String logText = appender.getBuffer(job.getLogChannelId(), false).toString();

    // report on logged lines
    logger.info(logText);
  }

  /**
   * This method executes a job defined in a kjb file
   * <p>
   * It demonstrates the following:
   * <p>
   * - Loading a job definition from a kjb file - Setting named parameters for
   * the job - Setting the log level of the job - Executing the job, waiting
   * for it to finish - Examining the result of the job
   *
   * @param jobProps azkaban job properties
   * @return the job that was executed, or null if there was an error
   */
  public static Job runJobFromFileSystem(Properties jobProps) throws Exception {
    final String filename = jobProps.getProperty("pentahojob.file");
    if (filename == null || filename.isEmpty()) {
      throw new Exception("please set job file location! pentahojob.file = xyz.file ");
    }

    logger.info("Attempting to run job " + filename + " from file system");
    // Loading the job file from file system into the JobMeta object.
    // The JobMeta object is the programmatic representation of a job
    // definition.
    JobMeta jobMeta = new JobMeta(filename, null);
    setPentahoJobParams(jobMeta, jobProps);

    // Creating a Job object which is the programmatic representation of a job
    // A Job object can be executed, report success, etc.
    Job job = new Job(null, jobMeta);
    setLogLevel(job, jobProps);
    logger.info("\nStarting job");

    // starting the job thread, which will execute asynchronously
    job.start();

    // waiting for the job to finish
    job.waitUntilFinished();

    // retrieve the result object, which captures the success of the job
    Result result = job.getResult();

    // report on the outcome of the job
    String outcome = String.format("\nJob %s executed with result: %s and %d errors\n",
        filename, result.getResult(), result.getNrErrors());
    logger.info(outcome);

    return job;
  }

  /**
   * This method executes a job stored in a repository.
   * <p>
   * It demonstrates the following:
   * <p>
   * - Loading a job definition from a repository - Setting named parameters
   * for the job - Setting the log level of the job - Executing the job,
   * waiting for it to finish - Examining the result of the job
   * <p>
   * When calling this method, kettle will look for the given repository name
   * in $KETTLE_HOME/.kettle/repositories.xml
   * <p>
   * If $KETTLE_HOME is not set explicitly, the user's home directory is
   * assumed
   *
   * @param jobProps azkaban job properties
   * @return the job that was executed, or null if there was an error
   */
  public static Job runJobFromRepository(Properties jobProps) throws KettleException {

    final String repositoryName = jobProps.getProperty("pentahojob.rep.name");
    final String username = jobProps.getProperty("pentahojob.rep.user");
    final String password = jobProps.getProperty("pentahojob.rep.pass");
    final String directory = jobProps.getProperty("pentahojob.rep.folder");
    final String jobName = jobProps.getProperty("pentahojob.rep.filename");

    logger.info("Attempting to run job " + directory + "/" + jobName + " from repository: "
        + repositoryName);

    // read the repositories.xml file to determine available
    // repositories
    RepositoriesMeta repositoriesMeta = new RepositoriesMeta();
    repositoriesMeta.readData();

    // find the repository definition using its name
    RepositoryMeta repositoryMeta = repositoriesMeta.findRepository(repositoryName);

    if (repositoryMeta == null) {
      throw new KettleException("Cannot find repository \"" + repositoryName
          + "\". Please make sure it is defined in your " + Const.getKettleUserRepositoriesFile()
          + " file");
    }

    // use the plug-in system to get the correct repository implementation
    // the actual implementation will vary depending on the type of given
    // repository (File-based, DB-based, EE, etc.)
    PluginRegistry registry = PluginRegistry.getInstance();
    Repository repository = registry
        .loadClass(RepositoryPluginType.class, repositoryMeta, Repository.class);

    // connect to the repository using given username and password
    repository.init(repositoryMeta);
    repository.connect(username, password);

    // find the directory we want to load from
    RepositoryDirectoryInterface tree = repository.loadRepositoryDirectoryTree();
    RepositoryDirectoryInterface dir = tree.findDirectory(directory);

    if (dir == null) {
      throw new KettleException("Cannot find directory \"" + directory + "\" in repository.");
    }

    // load latest revision of the job
    // The JobMeta object is the programmatic representation of a job definition.
    JobMeta jobMeta = repository.loadJob(jobName, dir, null, null);
    setPentahoJobParams(jobMeta, jobProps);

    // Creating a Job object which is the programmatic representation of a job
    // A Job object can be executed, report success, etc.
    Job job = new Job(repository, jobMeta);
    setLogLevel(job, jobProps);

    logger.info("\nStarting job");

    // starting the job, which will execute asynchronously
    job.start();

    // waiting for the job to finish
    job.waitUntilFinished();

    // retrieve the result object, which captures the success of the job
    Result result = job.getResult();

    // report on the outcome of the job
    String outcome =
        "\nJob " + directory + "/" + jobName + " executed with result: " + result.getResult()
            + " and " + result.getNrErrors() + " errors";
    logger.info(outcome);

    return job;
  }

  static void setLogLevel(Job job, Properties jobProps) {
    // adjust the log level
    if (jobProps.containsKey("pentahojob.level")) {
      LogLevel logLevel = LogLevel.getLogLevelForCode(jobProps.getProperty("pentahojob.level"));
      job.setLogLevel(logLevel);
    }
  }

  static void setPentahoJobParams(JobMeta jobMeta, Properties props) throws UnknownParamException {

    Map<String, String> pentahoVarMap = new HashMap<String, String>();
    Enumeration e = props.propertyNames();
    while (e.hasMoreElements()) {
      String key = (String) e.nextElement();
      if (key.contains("pentahojob.param.")) {
        String jobpParamValue = props.getProperty(key);
        String jobpParamName = key.replace("pentahojob.param.", "");
        logger
            .info(String.format("Setting parameter %s to \"%s\" ", jobpParamName, jobpParamValue));
        jobMeta.setParameterValue(jobpParamName, jobpParamValue);

      }
    }

    // The next section reports on the declared parameters
    String[] declaredParameters = jobMeta.listParameters();
    for (int i = 0; i < declaredParameters.length; i++) {
      String parameterName = declaredParameters[i];
      // determine the parameter description and default values for display purposes
      String description = jobMeta.getParameterDescription(parameterName);
      String defaultValue = jobMeta.getParameterDefault(parameterName);
      String output = String.format("Job parameter %s [description: \"%s\", default: \"%s\"]",
          parameterName, description, defaultValue);
    }
  }

}
