package org.apache.zeppelin.interpreter;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.builder.ToStringBuilder;

/**
 * Interpreter runner path
 */
public class InterpreterRunner {

  @SerializedName("linux")
  private String linuxPath;
  @SerializedName("win")
  private String winPath;

  public InterpreterRunner() {

  }

  public InterpreterRunner(String linuxPath, String winPath) {
    this.linuxPath = linuxPath;
    this.winPath = winPath;
  }

  public String getPath() {
    return System.getProperty("os.name").startsWith("Windows") ? winPath : linuxPath;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("linuxPath", linuxPath)
        .append("winPath", winPath)
        .toString();
  }
}
