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
import java.util.StringJoiner;
import java.util.regex.Pattern;
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
  public enum ProcessType {
    SHARED,
    SCOPED,
    ISOLATED
  }

  // Human readable name with description
  @Nonnull
  private String customInterpreterName;

  @Nonnull
  private String interpreterName;

  @Nonnull
  private String shebang;

  @Nonnull
  private ProcessType perNote;

  @Nonnull
  private ProcessType perUser;

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

  private boolean isEnabled;

  @Nonnull
  private ExistingProcess remoteProcess;

  @Nonnull
  private Permissions permissions;


  public InterpreterOption(@Nonnull final String customInterpreterName,
      @Nonnull final String interpreterName,
      @Nonnull final String shebang,
      @Nonnull final ProcessType perNote,
      @Nonnull final ProcessType perUser,
      @Nonnull final BaseInterpreterConfig config,
      @Nonnull final ExistingProcess remoteProcess,
      @Nonnull final Permissions permissions,
      @Nonnull final String jvmOptions,
      final int concurrentTasks,
      final boolean isEnabled) {
    Preconditions.checkNotNull(customInterpreterName);
    Preconditions.checkNotNull(interpreterName);
    Preconditions.checkNotNull(shebang);
    Preconditions.checkNotNull(perNote);
    Preconditions.checkNotNull(perUser);
    Preconditions.checkNotNull(config);
    Preconditions.checkNotNull(remoteProcess);
    Preconditions.checkNotNull(permissions);
    Preconditions.checkNotNull(jvmOptions);
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
  public ProcessType getPerNote() {
    Preconditions.checkNotNull(perNote);
    return perNote;
  }

  @Nonnull
  public ProcessType getPerUser() {
    Preconditions.checkNotNull(perUser);
    return perUser;
  }

  @Nonnull
  public String getJvmOptions() {
    Preconditions.checkNotNull(jvmOptions);
    return jvmOptions;
  }

  public void setJvmOptions(@Nonnull final String jvmOptions) {
    Preconditions.checkNotNull(jvmOptions);
    this.jvmOptions = jvmOptions;
  }

  public int getConcurrentTasks() {
    Preconditions.checkState(isValidCountOfConcurrentTasks(concurrentTasks));
    return concurrentTasks;
  }

  public void setConcurrentTasks(final int concurrentTasks) {
    Preconditions.checkArgument(isValidCountOfConcurrentTasks(concurrentTasks));
    this.concurrentTasks = concurrentTasks;
  }

  @Nonnull
  public String getCustomInterpreterName() {
    Preconditions.checkNotNull(customInterpreterName);
    return customInterpreterName;
  }

  public void setCustomInterpreterName(@Nonnull final String customInterpreterName) {
    Preconditions.checkNotNull(customInterpreterName);
    this.customInterpreterName = customInterpreterName;
  }

  @Nonnull
  public String getInterpreterName() {
    Preconditions.checkNotNull(interpreterName);
    return interpreterName;
  }

  public void setInterpreterName(@Nonnull final String interpreterName) {
    Preconditions.checkNotNull(interpreterName);
    this.interpreterName = interpreterName;
  }

  @Nonnull
  public String getShebang() {
    Preconditions.checkNotNull(shebang);
    return shebang;
  }

  public void setShebang(@Nonnull final String shebang) {
    Preconditions.checkArgument(isValidShebang(shebang), "Wrong shebang: %s", shebang);
    this.shebang = shebang;
  }

  @Nonnull
  public BaseInterpreterConfig getConfig() {
    Preconditions.checkNotNull(config);
    return config;
  }

  public void setConfig(@Nonnull final BaseInterpreterConfig config) {
    Preconditions.checkNotNull(config);
    this.config = config;
  }

  @Nonnull
  public ExistingProcess getRemoteProcess() {
    Preconditions.checkNotNull(remoteProcess);
    return remoteProcess;
  }

  public void setRemoteProcess(@Nonnull final ExistingProcess remoteProcess) {
    Preconditions.checkNotNull(remoteProcess);
    this.remoteProcess = remoteProcess;
  }

  @Nonnull
  public Permissions getPermissions() {
    Preconditions.checkNotNull(permissions);
    return permissions;
  }

  public void setPermissions(@Nonnull final Permissions permissions) {
    Preconditions.checkNotNull(permissions);
    this.permissions = permissions;
  }

  public boolean perUserShared() {
    return ProcessType.SHARED.equals(perUser);
  }

  private boolean perUserScoped() {
    return ProcessType.SCOPED.equals(perUser);
  }

  private boolean perUserIsolated() {
    return ProcessType.ISOLATED.equals(perUser);
  }

  public boolean perNoteShared() {
    return ProcessType.SHARED.equals(perNote);
  }

  private boolean perNoteScoped() {
    return ProcessType.SCOPED.equals(perNote);
  }

  private boolean perNoteIsolated() {
    return ProcessType.ISOLATED.equals(perNote);
  }

  public boolean isProcess() {
    return perUserIsolated() || perNoteIsolated();
  }

  public boolean isSession() {
    return perUserScoped() || perNoteScoped();
  }

  public void setPerNote(@Nonnull final ProcessType perNote) {
    Preconditions.checkNotNull(perNote);
    this.perNote = perNote;
  }

  public void setPerUser(@Nonnull final ProcessType perUser) {
    Preconditions.checkNotNull(perUser);
    this.perUser = perUser;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public void setEnabled(final boolean enabled) {
    isEnabled = enabled;
  }

  private static boolean isValidShebang(@Nonnull final String shebang) {
    Preconditions.checkNotNull(shebang);
    return Pattern.compile("%([\\w.]+)(\\(.*?\\))?").matcher(shebang).matches();
  }

  private static boolean isValidCountOfConcurrentTasks(final int concurrentTasks) {
    return concurrentTasks > 0;
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
