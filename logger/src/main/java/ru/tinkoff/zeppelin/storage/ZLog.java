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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.SystemEvent.ET;


/**
 * Simple event logger.
 */
@Component
public class ZLog {

  private static final Logger logger = LoggerFactory.getLogger(ZLog.class);

  private final ApplicationContext applicationContext;

  private static ZLog instance;

  @Nonnull
  private final SystemEventDAO storage;

  public ZLog(@Nonnull final SystemEventDAO storage, final ApplicationContext applicationContext) {
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
