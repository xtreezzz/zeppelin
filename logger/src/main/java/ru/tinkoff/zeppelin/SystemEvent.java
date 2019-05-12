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
package ru.tinkoff.zeppelin;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SystemEvent implements Serializable {

  /**
   * Type of system event.
   */
  public enum ET {
    UI_EVENT,
    INTERPRETER_PROCESS_NOT_FOUND,
    JOB_CANCEL_FAILED,
    JOB_CANCEL_ACCEPTED,
    JOB_CANCEL_NOT_FOUND,
    JOB_CANCEL_ERRORED,
    JOB_CANCEL_ALREADY_RUNNING,
    JOB_SUBMITTED_FOR_EXECUTION,
    GOT_JOB,
    GOT_ABORTED_JOB,
    INTERPRETER_INSTALL,
    INTERPRETER_ALREADY_INSTALLED,
    INTERPRETER_SUCCESSFULLY_INSTALLED,
    INTERPRETER_INSTALLATION_FAILED,
    INTERPRETER_SUCCESSFULLY_UNINSTALLED,
    INTERPRETER_DELETION_FAILED,
    INTERPRETER_CONFIGURATION_REQUESTED,
    INTERPRETER_CONFIGURAION_FOUND,
    INTERPRETER_CONFIGURATION_PROCESSING_FAILED,
    PROCESS_STARTED,
    REMOTE_CONNECTION_REGISTERED,
    BAD_REMOTE_CONNECTION,
    COMPLETED_PROCESS_NOT_FOUND,
    PROCESS_COMPLETED,
    CONNECTION_FAILED,
    FAILED_TO_RELEASE_CONNECTION,
    FORCE_KILL_REQUESTED,
    PUSH_FAILED_CLIENT_NOT_FOUND,
    PUSH_FAILED,
    PING_FAILED_CLIENT_NOT_FOUND,
    PING_FAILED,
    FORCE_KILL_FAILED_CLIENT_NOT_FOUND,
    FORCE_KILL_FAILED,
    INTERPRETER_EVENT_SERVER_STARTING,
    INTERPRETER_EVENT_SERVER_START_FAILED,
    INTERPRETER_EVENT_SERVER_STARTED,
    INTERPRETER_EVENT_SERVER_STOPPED,
    INTERPRETER_PROCESS_START_REQUESTED,
    INTERPRETER_PROCESS_FINISHED,
    INTERPRETER_PROCESS_FAILED,
    JOB_READY_FOR_EXECUTION,
    JOB_REQUEST_IS_EMPTY,
    JOB_ACCEPTED,
    JOB_DECLINED,
    JOB_REQUEST_ERRORED,
    JOB_UNDEFINED,
    JOB_READY_FOR_EXECUTION_BY_SCHEDULER,
    SCHEDULED_JOB_ERRORED,
    JOB_SCHEDULED,
    JOB_NOT_FOUND,
    INTERPRETER_RESULT_NOT_FOUND,
    SUCCESSFUL_RESULT,
    ABORTED_RESULT,
    ERRORED_RESULT,
    ACCESS_ERROR
  }

  private long databaseId;

  @Nonnull
  private final String username;

  @Nonnull
  private final ET type;

  @Nonnull
  private final String message;

  @Nullable
  private final String description;

  @Nonnull
  private final LocalDateTime actionTime;


  public SystemEvent(final long databaseId,
                     @Nonnull final ET type,
                     @Nonnull final String username,
                     @Nonnull final String message,
                     @Nullable final String description,
                     @Nonnull final LocalDateTime actionTime) {
    this.databaseId = databaseId;
    this.username = username;
    this.type = type;
    this.message = message;
    this.description = description;
    this.actionTime = actionTime;
  }

  public SystemEvent(@Nonnull final ET type,
                     @Nonnull final String username,
                     @Nonnull final String message,
                     @Nullable final String description) {
    this.username = username;
    this.type = type;
    this.message = message;
    this.description = description;
    this.actionTime = LocalDateTime.now();
  }

  public long getDatabaseId() {
    return databaseId;
  }

  public void setDatabaseId(final long databaseId) {
    this.databaseId = databaseId;
  }

  @Nonnull
  public String getUsername() {
    return username;
  }

  @Nonnull
  public ET getType() {
    return type;
  }

  @Nonnull
  public String getMessage() {
    return message;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  @Nonnull
  public LocalDateTime getActionTime() {
    return actionTime;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("databaseId=" + databaseId)
        .add("username='" + username + "'")
        .add("type=" + type)
        .add("message='" + message + "'")
        .add("description='" + description + "'")
        .add("actionTime=" + actionTime)
        .toString();
  }
}
