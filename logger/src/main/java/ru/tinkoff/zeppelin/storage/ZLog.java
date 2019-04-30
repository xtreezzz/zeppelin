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
package ru.tinkoff.zeppelin.storage;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import org.apache.zeppelin.SystemEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;


/**
 * Simple event logger.
 */
@Component
public class ZLog {

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

  private static final Logger logger = LoggerFactory.getLogger(ZLog.class);

  private final ApplicationContext applicationContext;

  private static ZLog instance;

  @Nonnull
  private final EventLogDAO storage;

  public ZLog(@Nonnull final EventLogDAO storage, final ApplicationContext applicationContext) {
    this.storage = storage;
    this.applicationContext = applicationContext;
  }

  @PostConstruct
  public void init() {
    instance = applicationContext.getBean(ZLog.class);
  }


  private void enqueue(@Nonnull final ET eventType,
                       @Nonnull final String message,
                       @Nonnull final String description,
                       @Nonnull final String username) {
    try {
      storage.log(new SystemEvent(eventType, username, message, description));
    } catch (final Exception e) {
      //skip
      logger.error("Failed to log event", e);
    }
  }

  /**
   * Все сообщения оборачиваются в System Event и добавляются в очередь
   * Отдельным тредом процесс достает из очереди и кидает
   * см. Логику с батчами.
   */
  public static void log(@Nonnull final ET eventType,
                         @Nonnull final String message,
                         @Nonnull final String description,
                         @Nonnull final String username) {
    try {
      instance.enqueue(eventType, message, description, username);
    } catch (final Exception e) {
      // skip
    }
  }

  /**
   * Record without description (description the same as msg).
   */
  public static void log(@Nonnull final ET eventType,
                         @Nonnull final String message,
                         @Nonnull final String username) {
    try {
      instance.enqueue(eventType, message, message, username);
    } catch (final Exception e) {
      // skip
    }
  }
}
