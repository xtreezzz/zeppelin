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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.ws.rs.BadRequestException;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zeppelin.rest.message.LoggerRequest;
import org.apache.zeppelin.scheduler.pool.DynamicThreadPool;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.SchedulerRepository;

/**
 * This class handles all of business logic of {@link org.apache.zeppelin.rest.AdminRestApi}.
 */
public class AdminService {

  private static Scheduler scheduler;

  public static Scheduler getScheduler() throws SchedulerException {
    if (scheduler != null) {
      return scheduler;
    }
    ArrayList<Scheduler> allSchedulers =
        new ArrayList<>(SchedulerRepository.getInstance().lookupAll());

    if (allSchedulers.size() > 0) {
      scheduler = allSchedulers.get(0);
      return scheduler;
    }
    throw new SchedulerException("Scheduler isn't configured");
  }

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

  public static Map<String, String> getSchedulerInfo() throws SchedulerException {
    Map<String, String> info = new HashMap<>();
    info.put("name", getSchedulerName());
    info.put("id", getSchedulerId());
    info.put("poolSize", String.valueOf(getSchedulerPoolSize()));
    info.put("poolClass", getSchedulerThreadPoolClass());
    info.put("storeClass", getSchedulerJobStoreClass());
    return info;
  }

  public static void setSchedulerThreadPoolSize(
      String schedulerId, Integer size) throws SchedulerException {
    DynamicThreadPool threadPool =
        DynamicThreadPool.getInstance(schedulerId);
    if (threadPool == null) {
      throw new SchedulerException("Wrong schedulerId - " + schedulerId);
    }
    threadPool.setThreadCount(size);
  }

  public static String getSchedulerName() throws SchedulerException {
    return getScheduler().getSchedulerName();
  }

  public static String getSchedulerId() throws SchedulerException {
    return getScheduler().getSchedulerInstanceId();
  }

  public static Integer getSchedulerPoolSize() throws SchedulerException {
    return getScheduler().getMetaData().getThreadPoolSize();
  }

  public static String getSchedulerThreadPoolClass() throws SchedulerException {
    return getScheduler().getMetaData().getThreadPoolClass().getName();
  }

  public static String getSchedulerJobStoreClass() throws SchedulerException {
    return getScheduler().getMetaData().getJobStoreClass().getName();
  }
}
