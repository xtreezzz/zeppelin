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

package org.apache.zeppelin.interpreterV2.configuration;

import org.apache.zeppelin.interpreterV2.configuration.option.ExistingProcess;
import org.apache.zeppelin.interpreterV2.configuration.option.Permissions;

/**
 * Full interpreter settings on interpreter page.
 */
public class InterpreterOption {
  private static final String SHARED = "shared";
  private static final String SCOPED = "scoped";
  private static final String ISOLATED = "isolated";

  private String perNote;
  private String perUser;

  private BaseInterpreterConfig config;

  private ExistingProcess remoteProcess;
  private Permissions permissions;

  public InterpreterOption(String perNote, String perUser,
      ExistingProcess remoteProcess,
      Permissions permissions,
      BaseInterpreterConfig config) {
    if (perUser == null) {
      throw new NullPointerException("perUser can not be null.");
    }
    if (perNote == null) {
      throw new NullPointerException("perNote can not be null.");
    }

    this.perNote = perNote;
    this.perUser = perUser;
    this.remoteProcess = remoteProcess;
    this.permissions = permissions;
    this.config = config;
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
}
