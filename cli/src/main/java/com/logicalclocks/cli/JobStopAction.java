package com.logicalclocks.cli;

import io.hops.cli.action.JobAction;
import io.hops.cli.config.HopsworksAPIConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobStopAction extends JobAction {
  private static final Logger logger = LoggerFactory.getLogger(io.hops.cli.action.JobStopAction.class);

  public JobStopAction(HopsworksAPIConfig hopsworksAPIConfig, String jobName) {
    super(hopsworksAPIConfig, jobName);
  }


  @Override
  public int execute() throws Exception {
    int executionId = getLatestExecution();
    CloseableHttpClient getClient = getClient();
    HttpPut request = getJobPut("/executions/" + executionId + "/status");
    CloseableHttpResponse response = getClient.execute(request);
    System.out.println("Stopping job " + this.jobName + " with execution id: " + executionId);
    return readJsonResponse(response);
  }
}
