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
import java.io.Serializable;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.zeppelin.interpreter.configuration.option.ExistingProcess;
import org.apache.zeppelin.interpreter.configuration.option.Permissions;

/**
 * Full interpreter settings on interpreter page.
 */
public class InterpreterOption implements Serializable {

  /**
   * Interpreter process isolation type.
   */
  private enum ProcessType {
    SHARED("shared"),
    SCOPED("scoped"),
    ISOLATED("isolated");

    private final String value;

    ProcessType(final String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  // Human readable name with description
  @Nonnull
  private String customInterpreterName;
  @Nonnull
  private String interpreterName;
  @Nonnull
  private String shebang;
  @Nonnull
  private String perNote;
  @Nonnull
  private String perUser;
  @Nonnull
  private BaseInterpreterConfig config;

  // additional jvm parameter string
  //TODO(egorklimov): validatie options
  @Nonnull
  private String jvmOptions;

  /**
   * Count of tasks available for concurrent execution
   */
  private int concurrentTasks;
  private final boolean isEnabled;

  @Nonnull
  private ExistingProcess remoteProcess;
  @Nonnull
  private Permissions permissions;


  public InterpreterOption(@Nonnull final String customInterpreterName,
      @Nonnull final String interpreterName,
      @Nonnull final String shebang,
      @Nonnull final String perNote,
      @Nonnull final String perUser,
      @Nonnull final BaseInterpreterConfig config,
      @Nonnull final ExistingProcess remoteProcess,
      @Nonnull final Permissions permissions,
      @Nonnull final String jvmOptions,
      final int concurrentTasks,
      final boolean isEnabled) {
    Preconditions.checkArgument(isValidShebang(shebang), "Wrong shebang: %s", shebang);

    this.customInterpreterName = customInterpreterName;
    this.interpreterName = interpreterName;
    this.shebang = shebang;
    this.perNote = perNote;
    this.perUser = perUser;
    this.config = config;
    this.remoteProcess = remoteProcess;
    this.permissions = permissions;
    this.jvmOptions = jvmOptions;
    this.concurrentTasks = concurrentTasks;
    this.isEnabled = isEnabled;
  }

  @Nonnull
  public String getPerNote() {
    return perNote;
  }

  @Nonnull
  public String getPerUser() {
    return perUser;
  }

  @Nonnull
  public String getJvmOptions() {
    return jvmOptions;
  }

  public void setJvmOptions(@Nonnull final String jvmOptions) {
    this.jvmOptions = jvmOptions;
  }

  public int getConcurrentTasks() {
    return concurrentTasks;
  }

  public void setConcurrentTasks(final int concurrentTasks) {
    this.concurrentTasks = concurrentTasks;
  }

  @Nonnull
  public String getCustomInterpreterName() {
    return customInterpreterName;
  }

  public void setCustomInterpreterName(@Nonnull final String customInterpreterName) {
    this.customInterpreterName = customInterpreterName;
  }

  @Nonnull
  public String getInterpreterName() {
    return interpreterName;
  }

  public void setInterpreterName(@Nonnull final String interpreterName) {
    this.interpreterName = interpreterName;
  }

  @Nonnull
  public String getShebang() {
    return shebang;
  }

  public void setShebang(@Nonnull final String shebang) {
    Preconditions.checkArgument(isValidShebang(shebang), "Wrong shebang: %s", shebang);
    this.shebang = shebang;
  }

  @Nonnull
  public BaseInterpreterConfig getConfig() {
    return config;
  }

  public void setConfig(@Nonnull final BaseInterpreterConfig config) {
    this.config = config;
  }

  @Nonnull
  public ExistingProcess getRemoteProcess() {
    return remoteProcess;
  }

  public void setRemoteProcess(@Nonnull final ExistingProcess remoteProcess) {
    this.remoteProcess = remoteProcess;
  }

  @Nonnull
  public Permissions getPermissions() {
    return permissions;
  }

  public void setPermissions(@Nonnull final Permissions permissions) {
    this.permissions = permissions;
  }

  public boolean perUserShared() {
    return ProcessType.SHARED.getValue().equals(perUser);
  }

  private boolean perUserScoped() {
    return ProcessType.SCOPED.getValue().equals(perUser);
  }

  private boolean perUserIsolated() {
    return ProcessType.ISOLATED.getValue().equals(perUser);
  }

  public boolean perNoteShared() {
    return ProcessType.SHARED.getValue().equals(perNote);
  }

  private boolean perNoteScoped() {
    return ProcessType.SCOPED.getValue().equals(perNote);
  }

  private boolean perNoteIsolated() {
    return ProcessType.ISOLATED.getValue().equals(perNote);
  }

  public boolean isProcess() {
    return perUserIsolated() || perNoteIsolated();
  }

  public boolean isSession() {
    return perUserScoped() || perNoteScoped();
  }

  public void setPerNote(@Nonnull final String perNote) {
    Preconditions.checkArgument(isValidProcessType(perNote), "Wrong process type: %s", perNote);
    this.perNote = perNote;
  }

  public void setPerUser(@Nonnull final String perUser) {
    Preconditions.checkArgument(isValidProcessType(perUser), "Wrong process type: %s", perUser);
    this.perUser = perUser;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  private static boolean isValidProcessType(@Nonnull final String value) {
    return Arrays.stream(ProcessType.values()).map(ProcessType::getValue)
        .collect(Collectors.toList()).contains(value);
  }

  private static boolean isValidShebang(@Nonnull final String shebang) {
    return Pattern.compile("%([\\w.]+)(\\(.*?\\))?").matcher(shebang).matches();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("customInterpreterName: '" + customInterpreterName + "'")
        .add("interpreterName: '" + interpreterName + "'")
        .add("shebang: '" + shebang + "'")
        .add("perNote: '" + perNote + "'")
        .add("perUser: '" + perUser + "'")
        .add("config: " + config)
        .add("remoteProcess: " + remoteProcess)
        .add("permissions: " + permissions)
        .toString();
  }
}
