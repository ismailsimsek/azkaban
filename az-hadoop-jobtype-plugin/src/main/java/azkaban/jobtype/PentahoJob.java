/*
 * Copyright 2012 LinkedIn Corp.
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

import azkaban.jobExecutor.JavaProcessJob;
import azkaban.jobtype.javautils.FileUtils;
import azkaban.utils.Props;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;


class PentahoJob extends JavaProcessJob {

  private static final String DEFAULT_INITIAL_MEMORY_SIZE = "2G";
  private static final String DEFAULT_MAX_MEMORY_SIZE = "4G";
  private static final String DATA_INTEGRATION_ROOT_DIR = "pentaho.data_integration_root_dir";
  private static final String PENTAHO_FILE = "pentaho.file";
  private static final String PENTAHO_PARAM = "pentaho.param";

  private static String pentahoLauncherJar;
  private static String pentahoEtlFile;
  private final List<String> pentahoParams = new ArrayList<>();
  private final List<String> jvmParams = new ArrayList<>();

  public PentahoJob(String jobid, Props sysProps, Props jobProps, Logger log)
      throws Exception {
    super(jobid, sysProps, jobProps, log);
    jobProps.logProperties(log, "");

    pentahoLauncherJar = getPentahoLauncherJar();
    pentahoEtlFile = getPentahoEtlFile();
    setPentahoParams();
    setJvmParams(GLOBAL_JVM_PARAMS);
    setJvmParams(JVM_PARAMS);

  }

  private String getPentahoLauncherJar() {
    assert
        jobProps.containsKey(DATA_INTEGRATION_ROOT_DIR) || jobProps.getString(
            DATA_INTEGRATION_ROOT_DIR).isEmpty() :
        "please set '" + DATA_INTEGRATION_ROOT_DIR + "' parameter in plugin.properties";

    File launcherDir = new File(jobProps.getString(DATA_INTEGRATION_ROOT_DIR), "launcher");
    assert launcherDir.exists() || launcherDir.isDirectory() :
        "Cant find Pentaho launcher directory : " + launcherDir.getAbsolutePath();

    Collection<String> launchers = FileUtils
        .listFiles(launcherDir.getAbsolutePath(), "*pentaho*launcher*.jar");
    assert launchers.isEmpty() :
        "Cant find launcher jar file! under " + launcherDir.getAbsolutePath();

    return launcherDir.getAbsolutePath() + "/" + Collections.max(launchers);
  }


  protected String getJavaClass() {
    if (pentahoEtlFile.endsWith(".ktr")) {
      return "org.pentaho.di.pan.Pan";
    } else {
      return "org.pentaho.di.kitchen.Kitchen";
    }
  }

  private String getPentahoEtlFile() throws Exception {
    assert
        jobProps.containsKey(PENTAHO_FILE) :
        "please set " + PENTAHO_FILE + " parameter in job properties";

    // assuming sql file given as absolute path
    File etlFile = new File(jobProps.getString(PENTAHO_FILE));
    // if not found load it from relative path
    if (!etlFile.exists()) {
      etlFile = new File(this.getWorkingDirectory(), jobProps.getString(PENTAHO_FILE));
    }
    // file not found both at relative and absolute path locations
    if (!etlFile.exists()) {
      throw new Exception("Could not find Etl file at: " + jobProps.getString(PENTAHO_FILE)
          + " or at: " + this.getWorkingDirectory() + "/" + jobProps.getString(PENTAHO_FILE)
          + "! please check Job Parameter " + PENTAHO_FILE + "!");
    }

    return etlFile.getAbsolutePath();
  }


  private void setPentahoParams() {
    for (final String key : this.jobProps.getKeySet()) {
      if (key.contains(PENTAHO_PARAM + ".")) {
        String jobpParamValue = this.jobProps.getString(key, "");
        String jobpParamName = key.replace(PENTAHO_PARAM + ".", "");
        this.pentahoParams.add("-param:" + jobpParamName + "=" + jobpParamValue);
      }
    }
  }

  private void setJvmParams(String jvmParamsKey) {
    if (!this.jobProps.getString(jvmParamsKey, "").isEmpty()) {
      this.jvmParams.add(this.jobProps.getString(jvmParamsKey));
    }
    for (int i = 1; this.jobProps.containsKey(jvmParamsKey + "." + i); i++) {
      if (!this.jobProps.getString(jvmParamsKey + "." + i, "").isEmpty()) {
        this.jvmParams.add(this.jobProps.getString(jvmParamsKey + "." + i));
      }
    }
  }

  @Override
  protected String getInitialMemorySize() {
    return getJobProps().getString(INITIAL_MEMORY_SIZE,
        DEFAULT_INITIAL_MEMORY_SIZE);
  }

  @Override
  protected String getMaxMemorySize() {
    return getJobProps().getString(MAX_MEMORY_SIZE, DEFAULT_MAX_MEMORY_SIZE);
  }

  @Override
  protected String createCommandLine() {
    String command = JAVA_COMMAND + " ";
    // add GLOBAL_JVM_PARAMS and JVM_PARAMS
    for (String param : this.jvmParams) {
      command += param + " ";
    }

    command += "-Xms" + getInitialMemorySize() + " ";
    command += "-Xmx" + getMaxMemorySize() + " ";
    command += "-cp " + createArguments(getClassPaths(), ":") + " ";

    command += "-jar" + pentahoLauncherJar + " ";
    // not needed for command line executions. example: -lib ./../libswt/linux/x86_64/
    // command += "-lib ./../libswt/" + this.sysname + "/" + this.sysnameArc + " ";
    command += "-main " + this.getJavaClass() + " ";
    command += "-initialDir " + this.getWorkingDirectory() + " ";
    command += "-file " + pentahoEtlFile + " ";

    // add pentaho params example:-param:file_wildcard_regexp=tx-.*csv
    for (String param : this.pentahoParams) {
      command += param + " ";
    }
    command += getMainArguments();

    return command;
  }

/*
/usr/local/openjdk8/bin/java
-Djavax.net.ssl.keyStore=/usr/home/pentaho_etl/etc/mysql/keystore
-Djavax.net.ssl.keyStorePassword=aBW9Nvvi108V0KDny1Mepe9ENScfQf
-Djavax.net.ssl.trustStore=/usr/home/pentaho_etl/etc/mysql/truststore
-Djavax.net.ssl.trustStorePassword=aBW9Nvvi108V0KDny1Mepe9ENScfQf
-Xms1g
-Xmx16g
-Dhttps.protocols=TLSv1,TLSv1.1,TLSv1.2
-Djava.library.path=./../libswt/linux/x86_64/
-DKETTLE_HOME=
-DKETTLE_REPOSITORY=
-DKETTLE_USER=
-DKETTLE_PASSWORD=
-DKETTLE_PLUGIN_PACKAGES=
-DKETTLE_LOG_SIZE_LIMIT=
-DKETTLE_JNDI_ROOT=
-jar /home/pentaho_etl/data-integration/launcher/pentaho-application-launcher-7.1.0.0-12.jar
-lib ./../libswt/linux/x86_64/
-main org.pentaho.di.kitchen.Kitchen
-initialDir /usr/home/pentaho_etl/azkaban/azkaban-solo-server-3.65.0-12-g9cb71555/executions/9660/home/pentaho_etl/bi-pentaho-etl/workflows/DWH/DWH_daily/imp_girogate/
-file /usr/home/pentaho_etl/bi-pentaho-etl/etl/imp_girogate/imp_girogate.csv_transaction_payment_gg.kjb
-param:ggbiexport_file_wildcard_regexp=tx-.*csv
 */


/*
09-01-2019 15:52:29 PST pentaho INFO - --
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.job.id value=pentaho
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.job.attempt value=0
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.start.year value=2019
09-01-2019 15:52:29 PST pentaho INFO -   key=pentaho.other_namenodes value=xxxxxxx
09-01-2019 15:52:29 PST pentaho INFO -   key=type value=pentaho
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.projectversion value=10
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.start.minute value=52
09-01-2019 15:52:29 PST pentaho INFO -   key=plugin.dir value=/opt/azkaban-solo-server-3.66.0-13-gcee80381/plugins/jobtypes/pentaho
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.execid value=44
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.job.attachment.file value=/opt/azkaban-solo-server-3.66.0-13-gcee80381/executions/44/_job.44.pentaho.attach
09-01-2019 15:52:29 PST pentaho INFO -   key=pentahojob.data_integration_root_dir value=/Users/isimsek/pentaho/data-integration
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.start.hour value=15
09-01-2019 15:52:29 PST pentaho INFO -   key=pentahojob.param.DEMO_PARAMETER_3 value=testvalupepassedto_pentaho3
09-01-2019 15:52:29 PST pentaho INFO -   key=jvm.args value=-Dazkaban.flowid=pentaho -Dazkaban.execid=44 -Dazkaban.jobid=pentaho
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.uuid value=f8f62825-59a2-4570-9f20-d9c3cce60522
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.job.log.file value=/opt/azkaban-solo-server-3.66.0-13-gcee80381/executions/44/_job.44.pentaho.log
09-01-2019 15:52:29 PST pentaho INFO -   key=user.to.proxy value=azkaban
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.flowid value=pentaho
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.start.day value=09
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.job.outnodes value=
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.memory.check value=true
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.projectlastchangedby value=azkaban
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.start.timestamp value=2019-01-09T15:52:29.670-08:00
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.job.innodes value=
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.start.milliseconds value=670
09-01-2019 15:52:29 PST pentaho INFO -   key=classpath value=./lib/*,./*
09-01-2019 15:52:29 PST pentaho INFO -   key=pentahojob.param.DEMO_PARAMETER_1 value=Detailed
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.start.timezone value=America/Los_Angeles
09-01-2019 15:52:29 PST pentaho INFO -   key=pentahojob.param.DEMO_PARAMETER_2 value=testvalupepassedto_pentaho
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.projectid value=3
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.start.second value=29
09-01-2019 15:52:29 PST pentaho INFO -   key=pentahojob.file value=pentaho/parameterized_job.kjb
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.submituser value=azkaban
09-01-2019 15:52:29 PST pentaho INFO -   key=dependencies value=
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.projectname value=ccccc
09-01-2019 15:52:29 PST pentaho INFO -   key=working.dir value=/opt/azkaban-solo-server-3.66.0-13-gcee80381/executions/44
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.start.month value=01
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.job.metadata.file value=_job.44.pentaho.meta
09-01-2019 15:52:29 PST pentaho INFO -   key=azkaban.flow.projectlastchangeddate value=1547077313573
09-01-2019 15:52:29 PST pentaho INFO -   key=pentahojob.level value=Detailed
09-01-2019 15:52:29 PST pentaho INFO - Memory granted for job pentaho
09-01-2019 15:52:29 PST pentaho INFO - 1 commands to execute.
09-01-2019 15:52:29 PST pentaho INFO - cwd=/opt/azkaban-solo-server-3.66.0-13-gcee80381/executions/44
09-01-2019 15:52:29 PST pentaho INFO - effective user is: azkaban
09-01-2019 15:52:29 PST pentaho INFO - Command: java -Dazkaban.flowid=pentaho -Dazkaban.execid=44 -Dazkaban.jobid=pentaho -Xms64M -Xmx256M -cp ./lib/*:./*:/opt/azkaban-solo-server-3.66.0-13-gcee80381/plugins/jobtypes/pentaho/az-hadoop-jobtype-plugin-3.66.0-22-g8ce7c679.jar:/Users/isimsek/pentaho/data-integration/lib/*:/Users/isimsek/pentaho/data-integration/classes:/Users/isimsek/pentaho/data-integration/libext azkaban.jobtype.PentahoJobRunnerMain
09-01-2019 15:52:29 PST pentaho INFO - Environment variables: {JOB_OUTPUT_PROP_FILE=/opt/azkaban-solo-server-3.66.0-13-gcee80381/executions/44/pentaho_output_5104126237603188634_tmp, JOB_PROP_FILE=/opt/azkaban-solo-server-3.66.0-13-gcee80381/executions/44/pentaho_props_7166749732603049247_tmp, KRB5CCNAME=/tmp/krb5cc__ccccc__pentaho__pentaho__44__azkaban, JOB_NAME=pentaho}
09-01-2019 15:52:29 PST pentaho INFO - Working directory: /opt/azkaban-solo-server-3.66.0-13-gcee80381/executions/44
 */


}
