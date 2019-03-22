package org.apache.zeppelin.notebook;

public class JobPayload {

  private Long id;
  private Long jobId;
  private String payload;

  public Long getId() {
    return id;
  }

  public void setId(final Long id) {
    this.id = id;
  }

  public Long getJobId() {
    return jobId;
  }

  public void setJobId(final Long jobId) {
    this.jobId = jobId;
  }

  public String getPayload() {
    return payload;
  }

  public void setPayload(final String payload) {
    this.payload = payload;
  }
}
