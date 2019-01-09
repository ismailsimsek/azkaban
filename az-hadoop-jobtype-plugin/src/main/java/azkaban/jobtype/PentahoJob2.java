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
import java.util.List;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;

public class PentahoJob2 extends JavaProcessJob {

  public PentahoJob2(String jobid, Props sysProps, Props jobProps, Logger log)
      throws RuntimeException {
    super(jobid, sysProps, jobProps, log);
    jobProps.logProperties(log,"--");

    checkJobParameters();
  }

  private void checkJobParameters() {
    assert
        jobProps.containsKey("pentahojob.data_integration_root_dir") :
        "please set pentahojob.data_integration_root_dir parameter in plugin.properties";

    assert new File(jobProps.getString("pentahojob.data_integration_root_dir")).isDirectory() :
        "pentahojob.data_integration_root_dir=["+jobProps.getString("pentahojob.data_integration_root_dir")+"] directory found";

    assert (jobProps.containsKey("pentahojob.file")|| jobProps.containsKey("pentahojob.rep.name")) :
        "please set pentahojob.file property or set pentahojob.rep.name in plugin.properties";
  }

  @Override
  protected List<String> getClassPaths() {
	List<String> classPath  = super.getClassPaths();
    classPath.add(getSourcePathFromClass(PentahoJobRunnerMain.class));
    //@TODO add if folder exists ?
    classPath.add(jobProps.getString("pentahojob.data_integration_root_dir")+"/lib/*");
    classPath.add(jobProps.getString("pentahojob.data_integration_root_dir")+"/classes");
    classPath.add(jobProps.getString("pentahojob.data_integration_root_dir")+"/libext");

    return classPath;
  }
  
  @Override
  protected String getJavaClass() {
    return PentahoJobRunnerMain.class.getName();
  }
  

  private static String getSourcePathFromClass(Class<?> containedClass) {
    File file =
        new File(containedClass.getProtectionDomain().getCodeSource()
            .getLocation().getPath());

    if (!file.isDirectory() && file.getName().endsWith(".class")) {
      String name = containedClass.getName();
      StringTokenizer tokenizer = new StringTokenizer(name, ".");
      while (tokenizer.hasMoreTokens()) {
        tokenizer.nextElement();

        file = file.getParentFile();
      }
      return file.getPath();
    } else {
      return containedClass.getProtectionDomain().getCodeSource().getLocation()
          .getPath();
    }
  }
  
}
