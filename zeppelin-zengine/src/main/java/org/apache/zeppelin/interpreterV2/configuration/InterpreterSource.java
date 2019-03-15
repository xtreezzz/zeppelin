package org.apache.zeppelin.interpreterV2.configuration;

import java.util.StringJoiner;

public class InterpreterSource {

  private String interpreterName;
  private String artifact;

  public InterpreterSource(String interpreterName, String artifact) {
    this.interpreterName = interpreterName;
    this.artifact = artifact;
  }

  public String getInterpreterName() {
    return interpreterName;
  }

  public void setInterpreterName(String interpreterName) {
    this.interpreterName = interpreterName;
  }

  public String getArtifact() {
    return artifact;
  }

  public void setArtifact(String artifact) {
    this.artifact = artifact;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("interpreterName='" + interpreterName + "'")
        .add("artifact='" + artifact + "'")
        .toString();
  }
}
