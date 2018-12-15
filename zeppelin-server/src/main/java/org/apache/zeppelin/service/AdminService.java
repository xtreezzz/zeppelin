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

package org.apache.zeppelin.service;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.ws.rs.BadRequestException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zeppelin.rest.exception.SchedulerConfigRuntimeException;
import org.apache.zeppelin.rest.message.LoggerRequest;
import org.apache.zeppelin.rest.message.SchedulerConfigRequest;
import org.apache.zeppelin.scheduler.pool.DynamicThreadPool;
import org.quartz.SchedulerException;
import org.quartz.impl.SchedulerRepository;

/**
 * This class handles all of business logic of {@link org.apache.zeppelin.rest.AdminRestApi}.
 */
public class AdminService {

  public List<Logger> getLoggers() {
    Enumeration loggers = LogManager.getCurrentLoggers();
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(
            new Iterator<Logger>() {
              @Override
              public boolean hasNext() {
                return loggers.hasMoreElements();
              }

              @Override
              public Logger next() {
                return Logger.class.cast(loggers.nextElement());
              }
            },
            Spliterator.ORDERED),
        false)
        .collect(Collectors.toList());
  }

  public Logger getLogger(String name) {
    return LogManager.getLogger(name);
  }

  public void setLoggerLevel(LoggerRequest loggerRequest) throws BadRequestException {
    try {
      Class.forName(loggerRequest.getName());
    } catch (Throwable ignore) {
      throw new BadRequestException(
          "The class of '" + loggerRequest.getName() + "' doesn't exists");
    }

    Logger logger = LogManager.getLogger(loggerRequest.getName());
    if (null == logger) {
      throw new BadRequestException("The name of the logger is wrong");
    }

    Level level = Level.toLevel(loggerRequest.getLevel(), null);
    if (null == level) {
      throw new BadRequestException("The level of the logger is wrong");
    }

    logger.setLevel(level);
  }

  public static List<SchedulerConfigRequest> getSchedulersInfoList() {
    return SchedulerRepository.getInstance().lookupAll().stream().map(scheduler -> {
      try {
        return new SchedulerConfigRequest(scheduler.getSchedulerName(),
          scheduler.getSchedulerInstanceId(),
          scheduler.getMetaData().getThreadPoolSize(),
          scheduler.getMetaData().getThreadPoolClass().getName(),
          scheduler.getMetaData().getJobStoreClass().getName());
      } catch (SchedulerException e) {
        throw new SchedulerConfigRuntimeException(e);
      }
    }).collect(Collectors.toList());
  }

  public static void setSchedulerThreadPoolSize(
      String schedulerId, Integer size) throws SchedulerException {
    DynamicThreadPool threadPool =
        DynamicThreadPool.getInstance(schedulerId);
    if (threadPool == null) {
      throw new SchedulerException("Dynamic pool is not configured or scheduler is wrong, "
          + "check setting for scheduler: " + schedulerId);
    }
    threadPool.setThreadCount(size);
  }
}
