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
import azkaban.utils.Props;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;


public class PentahoJob extends JavaProcessJob {

  public static final String DATA_INTEGRATION_ROOT_DIR = "pentaho.data_integration_root_dir";
  public static final String PENTAHO_FILE = "pentaho.file";
  public static final String PENTAHO_PARAM = "pentaho.param";

  public PentahoJob(String jobid, Props sysProps, Props jobProps, Logger log) {
    super(jobid, sysProps, jobProps, log);
    // test code
    jobProps.logProperties(log, "");

  }

  private String getPentahoLauncherJar() {
    if (jobProps.getString(DATA_INTEGRATION_ROOT_DIR, "").isEmpty()) {
      getLog().error("please set '" + DATA_INTEGRATION_ROOT_DIR + "' parameter in plugin"
          + ".properties");
      return "";
    }

    File launcherDir = new File(jobProps.getString(DATA_INTEGRATION_ROOT_DIR,
        "No/DataIntegration/RootDir/Given"), "launcher");
    if (!launcherDir.isDirectory()) {
      getLog().error(launcherDir.getAbsolutePath());
      getLog().error("Cant find Pentaho launcher directory : " + launcherDir.getAbsolutePath());
      return "";
    }

    getLog().info("Pentaho Launcher Dir:"+launcherDir.getAbsolutePath());
    List<File> launchers = Arrays.asList(launcherDir.listFiles(
        (dir, name) -> name.contains("pentaho") && name.contains("launcher") && name
            .endsWith("jar")));

    //getLog().info(launchers.toString());
    if (launchers.isEmpty()) {
      getLog().error("Cant find launcher jar file! under " + launcherDir.getAbsolutePath());
      return "";
    }

    return Collections.max(launchers).getAbsolutePath();
  }

  public String getJavaClass(String pentahoEtlFile) {
    if (pentahoEtlFile.toLowerCase().endsWith(".ktr")) {
      return "org.pentaho.di.pan.Pan";
    } else {
      return "org.pentaho.di.kitchen.Kitchen";
    }
  }


  private List<String> getPentahoParams() {
    List<String> pentahoParams = new ArrayList<>();

    for (final String key : this.jobProps.getKeySet()) {
      if (key.contains(PENTAHO_PARAM + ".")) {
        String jobpParamValue = this.jobProps.getString(key, "");
        String jobpParamName = key.replace(PENTAHO_PARAM + ".", "");
        pentahoParams.add("-param:" + jobpParamName + "=" + jobpParamValue);
      }
    }

    return pentahoParams;
  }

  private List<String> getJvmParams() {
    List<String> jvmParams = new ArrayList<>();

    if (!jobProps.getString(GLOBAL_JVM_PARAMS, "").isEmpty()) {
      jvmParams.add(this.jobProps.getString(GLOBAL_JVM_PARAMS));
    }
    for (int i = 1; this.jobProps.containsKey(GLOBAL_JVM_PARAMS + "." + i); i++) {
      if (!this.jobProps.getString(GLOBAL_JVM_PARAMS + "." + i, "").isEmpty()) {
        jvmParams.add(this.jobProps.getString(GLOBAL_JVM_PARAMS + "." + i));
      }
    }

    if (!this.jobProps.getString(JVM_PARAMS, "").isEmpty()) {
      jvmParams.add(this.jobProps.getString(JVM_PARAMS));
    }
    for (int i = 1; this.jobProps.containsKey(JVM_PARAMS + "." + i); i++) {
      if (!this.jobProps.getString(JVM_PARAMS + "." + i, "").isEmpty()) {
        jvmParams.add(this.jobProps.getString(JVM_PARAMS + "." + i));
      }
    }

    return jvmParams;
  }

  private String getPentahoEtlFile(String key) {
    if (!jobProps.containsKey(key)) {
      getLog().error("please set " + key + " parameter in job properties");
      return key + " PropertiesKeyNotFound!";
    }

    // assuming sql file given as absolute path
    File etlFile = new File(jobProps.getString(key, key + "_ValueNotSet"));
    // if not found load it from relative path
    if (!etlFile.exists()) {
      etlFile = new File(this.getWorkingDirectory(), jobProps.getString(key));
    }
    // file not found both at relative and absolute path locations
    if (!etlFile.exists()) {
      getLog().error("Etl file not found at: " + jobProps.getString(key)
          + " or at: " + this.getWorkingDirectory() + "/" + jobProps.getString(key)
          + "! please check Job Parameter " + key + "!");
      return key + "_EtlFileNotFound";
    }

    return etlFile.getAbsolutePath();
  }

  @Override
  protected List<String> getCommandList() {

    String pentahoLauncherJar = getPentahoLauncherJar();
    final List<String> commands = new ArrayList<>();
    commands.add(createCommandLine(pentahoLauncherJar, getPentahoEtlFile(PENTAHO_FILE)));
    for (int i = 1; this.jobProps.containsKey(PENTAHO_FILE + "." + i); i++) {
      commands
          .add(createCommandLine(pentahoLauncherJar, getPentahoEtlFile(PENTAHO_FILE + "." + i)));
    }

    return commands;
  }

  public String createCommandLine(String pentahoLauncherJar, String pentahoEtlFile) {
    String command = JAVA_COMMAND + " ";
    // add GLOBAL_JVM_PARAMS and JVM_PARAMS
    for (String param : this.getJvmParams()) {
      command += param + " ";
    }

    command += "-Xms" + getInitialMemorySize() + " ";
    command += "-Xmx" + getMaxMemorySize() + " ";
    // command += "-cp " + createArguments(getClassPaths(), ":") + " ";

    command += "-jar " + pentahoLauncherJar + " ";
    // not needed for command line executions. example: -lib ./../libswt/linux/x86_64/
    // command += "-lib ./../libswt/" + this.sysname + "/" + this.sysnameArc + " ";
    command += "-main " + this.getJavaClass(pentahoEtlFile) + " ";
    command += "-initialDir " + this.getWorkingDirectory() + " ";
    command += "-file \"" + pentahoEtlFile + "\" ";

    // add pentaho params example:-param:file_wildcard_regexp=tx-.*csv
    for (String param : this.getPentahoParams()) {
      command += "\"" + param + "\" ";
    }
    command += getMainArguments();

    return command.trim();
  }

  private void validateJobProps() throws Exception {
    // make sure pentaho launcher is exists
    String launcher = getPentahoLauncherJar();
    if (launcher.isEmpty()) {
      throw new Exception("Cant find Pentaho launcher!");
    }
    // at least one etl job should be given
    if (!jobProps.containsKey(PENTAHO_FILE)) {
      throw new Exception("please set " + PENTAHO_FILE + " parameter in job properties");
    }
  }

  @Override
  public void run() throws Exception {
    validateJobProps();
    super.run();
  }

}
