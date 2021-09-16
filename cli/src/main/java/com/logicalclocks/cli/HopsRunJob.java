package com.logicalclocks.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.hops.cli.action.FileUploadAction;
import io.hops.cli.action.JobRunAction;
import io.hops.cli.config.HopsworksAPIConfig;

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

public class HopsRunJob {

  public void actionPerformed(String jobType, String simulation, String jarPath) throws Exception {

    // jobs config
    Map<Object, Object> flinkJobsConfig;
    InputStream finkJobConfigStream;
    if (simulation.equals("yes")){
      finkJobConfigStream = new FileInputStream("bin/flink_simulation_config.json");
    } else {
      finkJobConfigStream = new FileInputStream("bin/flink_job_config.json");
    }
    try {
      flinkJobsConfig =
          new ObjectMapper().readValue(finkJobConfigStream, HashMap.class);
      finkJobConfigStream.close();
    } catch (Exception e) {
      throw new Exception(e.toString());
    }

    String hopsworksApiKey = (String) flinkJobsConfig.get("hopsworksApiKey");
    String hopsworksUrl = (String) flinkJobsConfig.get("hopsworksUrl");
    String projectName = (String) flinkJobsConfig.get("projectName");

    String flinkJobName = (String) flinkJobsConfig.get("jobName");
    String flinkDestination = "/Projects/" + projectName + "/Resources";

    String flinkUserArgs;
    if (simulation.equals("yes")){
      flinkUserArgs = String.format("-topicName %s -batchSize %s",
          flinkJobsConfig.get("topicName"), flinkJobsConfig.get("batchSize"));
    } else {
      flinkUserArgs = String.format("-projectName %s -featureGroupName %s -featureGroupVersion %s " +
              "-stateBackend %s -keyName %s -timestampField %s -sourceTopic %s -windowSize %s -windowTimeUnit %s " +
              "-watermark %s -watermarkTimeUnit %s -windowType %s -slideSize %s -slideTimeUnit %s -gapSize %s -gapTimeUnit %s " +
              "-parallelism %s -withSimulation %s -batchSize %s",
          flinkJobsConfig.get("projectName"),
          flinkJobsConfig.get("featureGroupName"),
          flinkJobsConfig.get("featureGroupVersion"),
          flinkJobsConfig.get("stateBackend"),
          flinkJobsConfig.get("keyName"),
          flinkJobsConfig.get("timestampField"),
          flinkJobsConfig.get("sourceTopic"),
          flinkJobsConfig.get("windowSize"),
          flinkJobsConfig.get("windowTimeUnit"),
          flinkJobsConfig.get("watermark"),
          flinkJobsConfig.get("watermarkTimeUnit"),
          flinkJobsConfig.get("windowType"),
          flinkJobsConfig.get("slideSize"),
          flinkJobsConfig.get("slideTimeUnit"),
          flinkJobsConfig.get("gapSize"),
          flinkJobsConfig.get("gapTimeUnit"),
          flinkJobsConfig.get("parallelism"),
          flinkJobsConfig.get("withSimulation"),
          flinkJobsConfig.get("batchSize")
      );

    }

    String flinkMainClass = (String) flinkJobsConfig.get("mainClass");

    String hopsProject=null;
    Integer userExecutionId= (Integer) flinkJobsConfig.get("userExecutionId");;

    int executionId=0;
    if(!userExecutionId.equals(0)){
      try{
        executionId=userExecutionId;
      } catch (NumberFormatException ex){
        System.out.println("Not a valid number execution id; Skipped");
      }
    }

    try {
      int status;
      JobRunAction runJob;
      HopsworksAPIConfig hopsworksAPIConfig = new HopsworksAPIConfig(hopsworksApiKey, hopsworksUrl, projectName);
      FileUploadAction uploadAction = new FileUploadAction(hopsworksAPIConfig, flinkDestination, jarPath);
      hopsProject = uploadAction.getProjectId(); //check if valid project,throws null pointer
      if (jobType.equals("FLINK")) {
        SubmitFlinkJob submitFlinkJob = new SubmitFlinkJob(hopsworksAPIConfig, flinkJobName);
        submitFlinkJob.setLocal_file_path(jarPath);
        submitFlinkJob.setMainClass(flinkMainClass);
        submitFlinkJob.setUserArgs(flinkUserArgs);
        submitFlinkJob.setUserExecId(executionId);
        status=submitFlinkJob.execute();
        if (status== HttpStatus.SC_OK){
          System.out.println("e.getProject()" +
              "Flink job was submitted successfully, please check Hopsworks UI for progress.");
        }
      }
    } catch (IOException ex) {
      System.out.println("e.getProject()" + ex.getMessage());
      Logger.getLogger(JobRunAction.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
    } catch (NullPointerException nullPointerException) {
      if (hopsProject == null) {
        System.out.println("e.getProject(), HopsPluginUtils.INVALID_PROJECT");
      } else {
        System.out.println("e.getProject()" + nullPointerException.toString());
      }
      Logger.getLogger(HopsCreateJob.class.getName()).log(Level.SEVERE, nullPointerException.toString(), nullPointerException);
    } catch (Exception ex) {
      System.out.println("e.getProject()" + ex.getMessage());
      Logger.getLogger(HopsRunJob.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
    }
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption("jobType", "jobType", true, "Job type SPARK|FLINK");
    options.addOption("jarFilePath", "jarFilePath", true, "path to spark program binary");
    options.addOption("simulation", "simulation", true, "path to spark program binary");

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);

    HopsRunJob hopsRunJob = new HopsRunJob();
    hopsRunJob.actionPerformed(commandLine.getOptionValue("jobType"),
        commandLine.getOptionValue("simulation"),
        commandLine.getOptionValue("jarFilePath"));
  }
}
