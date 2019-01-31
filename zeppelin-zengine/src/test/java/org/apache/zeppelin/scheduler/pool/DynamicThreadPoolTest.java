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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.quartz.SchedulerException;

public class DynamicThreadPoolTest {
  private DynamicThreadPool threadPool;

  /**
   * Each <code>{@link SimpleTask}</code> decrements it and main test thread
   * waits for it's zeroing after jobs enqueuing.
   */
  private static CountDownLatch masterLatch;

  /**
   * Main test thread decrements it after masterLatch notification.
   * Each <code>{@link SimpleTask}</code> waits for it's zeroing.
   * So all the jobs is active until slaveLatch zeroing.
   */
  private static CountDownLatch slaveLatch;

  /**
   * Setup {@link DynamicThreadPool}.
   */
  @Before
  public void beforeTests() throws SchedulerException {
    masterLatch = new CountDownLatch(2);
    slaveLatch = new CountDownLatch(1);

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
   * There are two <code>{@link SimpleTask}</code> jobs whereas threadCount is set to 1, therefore
   * threadPool must wait for available thread before second job execution, thus masterLatch.await
   * should return false.
   */
  @Test
  public void testWaitingForFreeThread() throws Exception {
    AtomicBoolean isTasksFinished = new AtomicBoolean(true);
    threadPool.setThreadCount(1);
    Assert.assertEquals(1, threadPool.getPoolSize());

    Thread t = new Thread(() -> {
      Assert.assertTrue(threadPool.runInThread(new SimpleTask()));
      Assert.assertTrue(threadPool.runInThread(new SimpleTask()));
      isTasksFinished.set(true);
    });
    t.setUncaughtExceptionHandler((th, e) -> {
      isTasksFinished.set(false);
    });
    t.start();

    Assert.assertTrue("Task has been rejected", isTasksFinished.get());
    Assert.assertFalse(masterLatch.await(100, TimeUnit.MILLISECONDS));
    slaveLatch.countDown();
  }

  /**
   * There are two <code>{@link SimpleTask}</code> jobs whereas threadCount is set to 2, therefore
   * threadPool jobs will be executed concurrently, thus masterLatch.await should return true.
   */
  @Test
  public void testParallelExecution() throws Exception {
    AtomicBoolean isTasksFinished = new AtomicBoolean(true);
    threadPool.setThreadCount(2);
    Assert.assertEquals(2, threadPool.getPoolSize());

    Thread t = new Thread(() -> {
      Assert.assertTrue(threadPool.runInThread(new SimpleTask()));
      Assert.assertTrue(threadPool.runInThread(new SimpleTask()));
    });
    t.setUncaughtExceptionHandler((th, e) -> {
      isTasksFinished.set(false);
    });
    t.start();

    Assert.assertTrue("Task has been rejected", isTasksFinished.get());
    Assert.assertTrue(masterLatch.await(100, TimeUnit.MILLISECONDS));
    slaveLatch.countDown();
  }

  public static class SimpleTask implements Runnable {
    @Override
    public void run() {
      masterLatch.countDown();
      try {
        slaveLatch.await(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
