/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.logicalclocks.cli;

import io.hops.cli.action.JobStopAction;
import io.hops.cli.config.HopsworksAPIConfig;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpStatus;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Options;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HopsCreateJob {

  public void actionPerformed(String jobType, String simulation, String sparkJarFilePath) throws Exception {

    // jobs config
    Map<Object, Object> flinkJobConfig;
    InputStream finkJobConfigStream;
    if (simulation.equals("yes")){
      finkJobConfigStream = new FileInputStream("bin/flink_simulation_config.json");
    } else {
      finkJobConfigStream = new FileInputStream("bin/flink_job_config.json");
    }
    try {
      flinkJobConfig =
          new ObjectMapper().readValue(finkJobConfigStream, HashMap.class);
      finkJobConfigStream.close();
    } catch (Exception e) {
      throw new Exception(e.toString());
    }

    String hopsworksApiKey = (String) flinkJobConfig.get("hopsworksApiKey");
    String hopsworksUrl = (String) flinkJobConfig.get("hopsworksUrl");
    String projectName = (String) flinkJobConfig.get("projectName");
    String destination = "/Projects/" + projectName + "/Resources";

    Integer flinkJobManagerMemory  = (Integer) flinkJobConfig.get("jobManagerMemory");;
    Integer flinkTaskManagerMemory =  (Integer) flinkJobConfig.get("taskManagerMemory");;
    Integer flinkNumTaskManager = (Integer) flinkJobConfig.get("numTaskManager");;
    Integer flinkNumSlots = (Integer) flinkJobConfig.get("numSlots");;
    Boolean flinkIsAdvanced = (Boolean) flinkJobConfig.get("isAdvanced");;
    String flinkAdvancedProperties = (String) flinkJobConfig.get("advancedProperties");

    String hopsProject = null;
    String jobName = null;
    try {
      HopsworksAPIConfig hopsworksAPIConfig = new HopsworksAPIConfig(hopsworksApiKey, hopsworksUrl, projectName);
      //upload program
      FileUploadAction uploadAction = new FileUploadAction(hopsworksAPIConfig, destination, sparkJarFilePath);
      hopsProject = uploadAction.getProjectId(); //check if valid project,throws null pointer
      // upload program if not flink
      if (!jobType.equals("FLINK"))
        uploadAction.execute();
      //set program configs
      JobCreateAction.Args args = new JobCreateAction.Args();
      args.setJobType(jobType);  // flink
      if ("FLINK".equals(jobType)) {
        jobName = (String) flinkJobConfig.get("jobName");

        args.setJobManagerMemory(flinkJobManagerMemory);
        args.setTaskManagerMemory(flinkTaskManagerMemory);
        args.setNumTaskManager(flinkNumTaskManager);
        args.setNumSlots(flinkNumSlots);
        if (flinkIsAdvanced) {
          args.setAdvanceConfig(true);
          args.setProperties(flinkAdvancedProperties);
        }
      }

      // if running stop
      try {
        io.hops.cli.action.JobStopAction stopJob=new JobStopAction(hopsworksAPIConfig,jobName);
        int status=stopJob.execute();
        if (status == HttpStatus.SC_OK || status == HttpStatus.SC_CREATED || status == HttpStatus.SC_ACCEPTED) {
          System.out.println("Job: Stopped");
        }  else {
          if (stopJob.getJsonResult().containsKey("usrMsg"))
            System.out.println(" Job Stop Failed | "
                +stopJob.getJsonResult().getString("usrMsg"));
        }
      } catch (Exception ex) {
        System.out.println(ex.getMessage());
      }

      // create job
      JobCreateAction createJob = createJob = new JobCreateAction(hopsworksAPIConfig, jobName, args);
      int status = createJob.execute();

      if (status == HttpStatus.SC_OK || status == HttpStatus.SC_CREATED) {
        System.out.println("Job Created: " + jobName);
      }else {
        if(createJob.getJsonResult().containsKey("usrMsg"))
          System.out.println(" Job Create Failed | "+createJob.getJsonResult().getString("usrMsg"));

        else System.out.println("Job Creation Failed: " + jobName);
      }

    } catch (IOException ioException) {
      System.out.println(ioException.getMessage());
      Logger.getLogger(JobCreateAction.class.getName()).log(Level.SEVERE, ioException.getMessage(), ioException);
    } catch (NullPointerException nullPointerException) {
      if (hopsProject == null) {
        System.out.println( "INVALID_PROJECT");
        Logger.getLogger(HopsCreateJob.class.getName()).log(Level.SEVERE, nullPointerException.toString(), nullPointerException);
      } else {
        System.out.println(nullPointerException.toString());
      }
      Logger.getLogger(HopsCreateJob.class.getName()).log(Level.SEVERE, nullPointerException.toString(), nullPointerException);
    } catch (Exception exception) {
      System.out.println(exception.toString());
      Logger.getLogger(HopsCreateJob.class.getName()).log(Level.SEVERE, exception.getMessage(), exception);
    }
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("jobType", "jobType", true, "Job type FLINK");
    options.addOption("jarFilePath", "jarFilePath", true,
        "path to flink program binary");
    options.addOption("simulation", "simulation", true, "path to spark program binary");

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);

    HopsCreateJob hopsCreateJob = new HopsCreateJob();
    hopsCreateJob.actionPerformed(commandLine.getOptionValue("jobType"), commandLine.getOptionValue("simulation"),
        commandLine.getOptionValue("jarFilePath"));
  }
}
