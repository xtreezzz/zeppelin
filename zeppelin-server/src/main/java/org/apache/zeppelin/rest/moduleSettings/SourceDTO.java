package org.apache.zeppelin.rest.moduleSettings;

public class SourceDTO {

  private String interpreterName;
  private String artifact;
  private String path;
  private String status;
  private boolean reinstallOnStart;

  public String getInterpreterName() {
    return interpreterName;
  }

  public void setInterpreterName(final String interpreterName) {
    this.interpreterName = interpreterName;
  }

  public String getArtifact() {
    return artifact;
  }

  public void setArtifact(final String artifact) {
    this.artifact = artifact;
  }

  public String getPath() {
    return path;
  }

  public void setPath(final String path) {
    this.path = path;
  }

  public String getStatus() {
    return status;
  }

  public void setStatus(final String status) {
    this.status = status;
  }

  public boolean isReinstallOnStart() {
    return reinstallOnStart;
  }

  public void setReinstallOnStart(final boolean reinstallOnStart) {
    this.reinstallOnStart = reinstallOnStart;
  }
}
