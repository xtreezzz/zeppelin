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

package org.apache.zeppelin.scheduler.pool;

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicThreadPoolTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicThreadPoolTest.class);

  private DynamicThreadPool threadPool;

  /**
   * MAX_POOL_SIZE - maximum pool size in test in [1; MAX_POOL_SIZE]. COUNT_OF_JOBS - count of jobs
   * on each test iteration.
   *
   * @see DynamicThreadPoolTest#testConcurrentExecution()
   */
  private static final int MAX_POOL_SIZE = 2;
  private static final int COUNT_OF_JOBS = 2;

  /**
   * Setup {@link DynamicThreadPool}.
   */
  @Before
  public void beforeTests() throws SchedulerException {
    threadPool = new DynamicThreadPool();
    threadPool.setInstanceId("Test");
    threadPool.setThreadCount(1);
    threadPool.initialize();
  }

  /**
   * Close and wait for job finish.
   */
  @After
  public void afterTests() {
    threadPool.shutdown(true);
  }

  /**
   * Creates 2 jobs {@link SimpleTask} each is sleeps for 1 second. if threadCount = 1 there is not
   * enough free threads to run jobs concurrently, therefore execution lasts 2 seconds; if
   * threadCount = 2 jobs will be executed concurrently, therefore execution lasts for 1 second.
   */
  @Test
  public void testConcurrentExecution() throws SchedulerConfigException {
    final StopWatch stopwatch = new StopWatch();
    for (int threadCount = 1; threadCount <= MAX_POOL_SIZE; ++threadCount) {
      threadPool.setThreadCount(threadCount);
      LOGGER.info("start: " + threadPool.blockForAvailableThreads());

      Assert.assertEquals(
          "Thread count update failed",
          threadCount,
          threadPool.getPoolSize()
      );
      stopwatch.start();

      for (int jobNo = 0; jobNo < COUNT_OF_JOBS; ++jobNo) {
        if (threadPool.blockForAvailableThreads() > 0) {
          threadPool.runInThread(
              new SimpleTask("job" + ((threadCount - 1) * COUNT_OF_JOBS + jobNo)));
        }
      }

      while (threadPool.blockForAvailableThreads() != threadCount) {
        // Wait for all jobs to finish.
      }
      try {
        TimeUnit.MILLISECONDS.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }

      LOGGER.info("[threadCount="
          + threadCount
          + "] finish: "
          + threadPool.blockForAvailableThreads());

      Assert.assertEquals(
          "Elapsed time is wrong",
          COUNT_OF_JOBS / threadCount,
          stopwatch.getTime(TimeUnit.SECONDS)
      );
      stopwatch.reset();
    }
  }

  /**
   * @see DynamicThreadPoolTest#testConcurrentExecution()
   */
  public static class SimpleTask implements Runnable {

    private String name;

    SimpleTask(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return String.format("SimpleTask-%s", name);
    }

    @Override
    public void run() {
      try {
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
