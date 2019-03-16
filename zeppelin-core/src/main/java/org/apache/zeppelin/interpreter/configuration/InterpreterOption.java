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

import java.io.Serializable;
import java.util.StringJoiner;
import org.apache.zeppelin.interpreter.configuration.option.ExistingProcess;
import org.apache.zeppelin.interpreter.configuration.option.Permissions;

/**
 * Full interpreter settings on interpreter page.
 */
public class InterpreterOption implements Serializable {
  private static final String SHARED = "shared";
  private static final String SCOPED = "scoped";
  private static final String ISOLATED = "isolated";

  // Human readable name with description
  private String customInterpreterName;

  private String interpreterName;
  private String shebang;

  private String perNote;
  private String perUser;

  private BaseInterpreterConfig config;

  // additional jvm parameter string
  private String jvmOptions;

  /**
   *  Count of tasks available for concurrent execution
   */
  private int concurrentTasks;

  private ExistingProcess remoteProcess;
  private Permissions permissions;


  public InterpreterOption(String customInterpreterName, String interpreterName,
      String shebang, String perNote, String perUser,
      BaseInterpreterConfig config,
      ExistingProcess remoteProcess,
      Permissions permissions,
      String jvmOptions,
      int concurrentTasks) {
    if (perUser == null) {
      throw new NullPointerException("perUser can not be null.");
    }
    if (perNote == null) {
      throw new NullPointerException("perNote can not be null.");
    }

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
  }

  public String getPerNote() {
    return perNote;
  }

  public String getPerUser() {
    return perUser;
  }

  public String getJvmOptions() {
    return jvmOptions;
  }

  public void setJvmOptions(String jvmOptions) {
    this.jvmOptions = jvmOptions;
  }

  public int getConcurrentTasks() {
    return concurrentTasks;
  }

  public void setConcurrentTasks(int concurrentTasks) {
    this.concurrentTasks = concurrentTasks;
  }

  public String getCustomInterpreterName() {
    return customInterpreterName;
  }

  public void setCustomInterpreterName(String customInterpreterName) {
    this.customInterpreterName = customInterpreterName;
  }

  public String getInterpreterName() {
    return interpreterName;
  }

  public void setInterpreterName(String interpreterName) {
    this.interpreterName = interpreterName;
  }

  public String getShebang() {
    return shebang;
  }

  public void setShebang(String shebang) {
    this.shebang = shebang;
  }

  public BaseInterpreterConfig getConfig() {
    return config;
  }

  public void setConfig(BaseInterpreterConfig config) {
    this.config = config;
  }

  public ExistingProcess getRemoteProcess() {
    return remoteProcess;
  }

  public void setRemoteProcess(ExistingProcess remoteProcess) {
    this.remoteProcess = remoteProcess;
  }

  public Permissions getPermissions() {
    return permissions;
  }

  public void setPermissions(Permissions permissions) {
    this.permissions = permissions;
  }

  public boolean perUserShared() {
    return SHARED.equals(perUser);
  }

  public boolean perUserScoped() {
    return SCOPED.equals(perUser);
  }

  public boolean perUserIsolated() {
    return ISOLATED.equals(perUser);
  }

  public boolean perNoteShared() {
    return SHARED.equals(perNote);
  }

  public boolean perNoteScoped() {
    return SCOPED.equals(perNote);
  }

  public boolean perNoteIsolated() {
    return ISOLATED.equals(perNote);
  }

  public boolean isProcess() {
    return perUserIsolated() || perNoteIsolated();
  }

  public boolean isSession() {
    return perUserScoped() || perNoteScoped();
  }

  public void setPerNote(String perNote) {
    this.perNote = perNote;
  }

  public void setPerUser(String perUser) {
    this.perUser = perUser;
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
