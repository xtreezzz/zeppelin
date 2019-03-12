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

import org.apache.log4j.LogManager;
import org.apache.zeppelin.rest.message.LoggerRequest;
import org.apache.zeppelin.rest.message.SchedulerConfigRequest;
import org.quartz.SchedulerException;
import org.quartz.impl.SchedulerRepository;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * This class handles all of business logic of {@link org.apache.zeppelin.rest.AdminRestApi}.
 */
@Component
public class AdminService {

  public List<org.apache.log4j.Logger> getLoggers() {
    final Enumeration loggers = LogManager.getCurrentLoggers();
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                new Iterator<org.apache.log4j.Logger>() {
                  @Override
                  public boolean hasNext() {
                    return loggers.hasMoreElements();
                  }

                  @Override
                  public org.apache.log4j.Logger next() {
                    return (org.apache.log4j.Logger) loggers.nextElement();
                  }
                },
                Spliterator.ORDERED),
            false)
        .collect(Collectors.toList());
  }

  public org.apache.log4j.Logger getLogger(final String name) {
    return LogManager.getLogger(name);
  }

  public void setLoggerLevel(final LoggerRequest loggerRequest) {
    try {
      Class.forName(loggerRequest.getName());
    } catch (final Throwable ignore) {
      throw new IllegalArgumentException(
          "The class of '" + loggerRequest.getName() + "' doesn't exists");
    }

    final org.apache.log4j.Logger logger = LogManager.getLogger(loggerRequest.getName());
    if (null == logger) {
      throw new IllegalArgumentException("The name of the logger is wrong");
    }

    final org.apache.log4j.Level level = org.apache.log4j.Level.toLevel(loggerRequest.getLevel(), null);
    if (null == level) {
      throw new IllegalArgumentException("The level of the logger is wrong");
    }

    logger.setLevel(level);
  }


  public List<SchedulerConfigRequest> getSchedulersInfoList() {
    return SchedulerRepository.getInstance().lookupAll().stream().map(scheduler -> {
      try {
        return new SchedulerConfigRequest(
                scheduler.getSchedulerName(),
                scheduler.getSchedulerInstanceId(),
                scheduler.getMetaData().getThreadPoolSize(),
                scheduler.getMetaData().getThreadPoolClass().getName(),
                scheduler.getMetaData().getJobStoreClass().getName()
        );
      } catch (SchedulerException e) {
        throw new IllegalStateException(e);
      }
    }).collect(Collectors.toList());
  }

  public void setSchedulerThreadPoolSize(final String schedulerId, final Integer size) {
//    try {
//      final DynamicThreadPool threadPool = DynamicThreadPool.getInstance(schedulerId);
//      if (threadPool == null) {
//        throw new IllegalArgumentException("Dynamic pool is not configured or scheduler is wrong, "
//                + "check setting for scheduler: " + schedulerId);
//      }
//      threadPool.setThreadCount(size);
//    } catch (final SchedulerConfigException e) {
//      throw new IllegalStateException(e);
//    }
  }
}
