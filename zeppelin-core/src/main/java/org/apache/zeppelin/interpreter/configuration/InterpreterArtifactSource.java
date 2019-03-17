/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.interpreter.configuration;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import java.io.Serializable;
import java.nio.file.InvalidPathException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class InterpreterArtifactSource implements Serializable {

  /**
   * Types of interpreter processes.
   */
  private enum Status {
    NOT_INSTALLED("not installed"),
    INSTALLED("installed"),
    IN_PROGRESS("in progress");

    private final String value;

    Status(final String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  @Nonnull
  private String interpreterName;
  @Nonnull
  private String artifact;
  @Nullable
  private String path;
  @Nonnull
  private String status;

  public InterpreterArtifactSource(@Nonnull final String interpreterName,
      @Nonnull final String artifact, @Nullable final String path, @Nonnull final String status) {
    Preconditions.checkArgument(getAllStatuses().contains(status), "Wrong status: %s", status);
    Preconditions.checkArgument(isValidAbsolutePathOrNull(path), "Wrong path: %s", path);

    //TODO(egorklimov): add regexp check for artifact
    this.interpreterName = interpreterName;
    this.artifact = artifact;
    this.path = path;
    this.status = status;
  }

  public InterpreterArtifactSource(@Nonnull final String interpreterName,
      @Nonnull final String artifact) {
    //TODO(egorklimov): add regexp check for artifact

    this.interpreterName = interpreterName;
    this.artifact = artifact;
    this.status = Status.NOT_INSTALLED.getValue();
  }

  @Nonnull
  private static List<String> getAllStatuses() {
    return Arrays.stream(Status.values()).map(Status::getValue).collect(Collectors.toList());
  }

  @Nonnull
  public String getInterpreterName() {
    return interpreterName;
  }

  public void setInterpreterName(@Nonnull final String interpreterName) {
    this.interpreterName = interpreterName;
  }

  @Nonnull
  public String getArtifact() {
    return artifact;
  }

  public void setArtifact(@Nonnull final String artifact) {
    //TODO(egorklimov): add regexp check for artifact
    this.artifact = artifact;
  }

  @Nullable
  public String getPath() {
    return path;
  }

  /**
   * Set path to downloaded .jar
   *
   * @param path - Nonnul
   */
  public void setPath(@Nonnull final String path) {
    Preconditions.checkArgument(isValidAbsolutePathOrNull(path), "Wrong path: %s", path);
    this.path = path;
  }

  @Nonnull
  public String getStatus() {
    return status;
  }

  public void setStatus(@Nonnull final String status) {
    Preconditions.checkArgument(getAllStatuses().contains(status), "Wrong status: %s", status);
    this.status = status;
  }

  private static boolean isValidAbsolutePathOrNull(@Nullable final String path) {
    try {
      if (path != null) {
        return Paths.get(path).isAbsolute();
      }
      return true;
    } catch (final InvalidPathException | NullPointerException ex) {
      return false;
    }
  }

  public static InterpreterArtifactSource fromJson(final String json) {
    final InterpreterArtifactSource source = new Gson().fromJson(json, InterpreterArtifactSource.class);
    Preconditions.checkNotNull(source.artifact);
    Preconditions.checkNotNull(source.interpreterName);

    if (source.status == null) {
      source.setStatus(Status.NOT_INSTALLED.getValue());
    }
    Preconditions.checkState(isValidAbsolutePathOrNull(source.getPath()), "Wrong path: %s", source.getPath());
    Preconditions.checkState(getAllStatuses().contains(source.getStatus()), "Wrong status: %s", source.getStatus());
    //TODO(egorklimov): add regexp check for artifact

    return source;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("interpreterName='" + interpreterName + "'")
        .add("artifact='" + artifact + "'")
        .add("path='" + path + "'")
        .add("status='" + status + "'")
        .toString();
  }
}
