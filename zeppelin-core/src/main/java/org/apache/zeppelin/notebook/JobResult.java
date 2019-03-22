package org.apache.zeppelin.notebook;

import java.time.LocalDateTime;

public class JobResult {

  private Long id;
  private Long jobId;
  private LocalDateTime createdAt;
  private String type;
  private String result;

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

  public LocalDateTime getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(final LocalDateTime createdAt) {
    this.createdAt = createdAt;
  }

  public String getType() {
    return type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public String getResult() {
    return result;
  }

  public void setResult(final String result) {
    this.result = result;
  }
}
