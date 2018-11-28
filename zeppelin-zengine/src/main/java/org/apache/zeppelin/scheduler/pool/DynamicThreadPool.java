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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is a simple implementation of a Quartz thread pool.
 *
 * <CODE>Runnable</CODE> objects are sent to the pool by
 * <code>{@link ThreadPoolExecutor}</code> with the
 * <code>{@link #runInThread(Runnable)}</code> method,
 * which blocks until a <code>Thread</code> becomes available.
 *
 * Pool based on <code>{@link SynchronousQueue}</code>.
 *
 * The pool has a dynamic number of <code>Thread</code>s, and grows or shrinks based on demand. It
 * could be changed by calling <code>{@link #setThreadCount(int)}</code>
 *
 * Instance could be obtained using <code>{@link DynamicThreadPool#getInstance(String)}</code>
 */
public class DynamicThreadPool implements ThreadPool {
  private static final Logger LOGGER = LoggerFactory.getLogger(DynamicThreadPool.class);

  private static final Map<String, DynamicThreadPool> INSTANCES = new ConcurrentHashMap<>();
  private final Object poolEvent = new Object();

  private final ThreadPoolExecutor executor =
      new ThreadPoolExecutor(1, 10, 1, TimeUnit.MINUTES, new SynchronousQueue<>());

  private volatile int priority = Thread.NORM_PRIORITY;

  private String threadNamePrefix;
  private String schedulerInstanceId;
  private String schedulerInstanceName;

  public static DynamicThreadPool getInstance(String schedulerInstanceId) {
    return INSTANCES.get(schedulerInstanceId);
  }

  @Override
  public boolean runInThread(Runnable runnable) {
    if (runnable == null) {
      return false;
    }
    try {
      if (blockForAvailableThreads() > 0) {
        executor.execute(new NotifableJob(runnable));
      }
      return true;
    } catch (RejectedExecutionException e) {
      LOGGER.error(e.toString());
      return false;
    }
  }

  @Override
  public int blockForAvailableThreads() {
    int availableThreads = executor.getMaximumPoolSize() - executor.getActiveCount();
    while (availableThreads <= 0) {
      synchronized (poolEvent) {
        try {
          poolEvent.wait(500);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return 0;
        }
      }
      availableThreads = executor.getMaximumPoolSize() - executor.getActiveCount();
    }
    return availableThreads;
  }


  @Override
  public void initialize() throws SchedulerConfigException {
    if (schedulerInstanceId != null && INSTANCES.get(schedulerInstanceId) != this) {
      // already initialized...
      return;
    }

    if (priority <= 0 || priority > 9) {
      throw new SchedulerConfigException(
          "Thread priority must be > 0 and <= 9");
    }

    if (threadNamePrefix == null && schedulerInstanceName != null) {
      threadNamePrefix = schedulerInstanceName;
    }
    executor.setThreadFactory(new DynamicThreadFactory());
  }

  /**
   * Terminate any worker threads in this thread group.
   *
   * Jobs currently in progress will complete.
   *
   * @param waitForJobsToComplete boolean flag for resource release: if true: initiates an orderly
   * shutdown in which previously submitted tasks are executed, but no new tasks will be accepted.
   * @see ExecutorService#shutdown() if false: Attempts to stop all actively executing tasks, halts
   * the processing of waiting tasks.
   * @see ExecutorService#shutdownNow()
   */
  @Override
  public void shutdown(boolean waitForJobsToComplete) {
    LOGGER.info("Shutting down ThreadPool...");
    if (!waitForJobsToComplete) {
      try {
        executor.shutdown();
        final boolean done = executor.awaitTermination(1, TimeUnit.MINUTES);
        LOGGER.info("Have all the cron tasks been completed? {}", done);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      executor.shutdownNow();
      final List<Runnable> rejected = executor.shutdownNow();
      // Should be zero because of SynchronousQueue
      LOGGER.info("Rejected tasks: {}: {}", rejected.size(), rejected);
    }
    INSTANCES.remove(schedulerInstanceId);
  }

  public void setThreadNamePrefix(String prfx) {
    this.threadNamePrefix = prfx;
  }

  /**
   * Set the number of worker threads in the pool. Increases <code>{@link
   * ThreadPoolExecutor#maximumPoolSize}</code>
   */
  public void setThreadCount(int threadCount) throws SchedulerConfigException {
    if (executor.getMaximumPoolSize() == threadCount) {
      return;
    }
    LOGGER.info(
        "Changing ThreadPool size from {} to {}", executor.getMaximumPoolSize(), threadCount);
    try {
      executor.setMaximumPoolSize(threadCount);
    } catch (IllegalArgumentException e) {
      throw new SchedulerConfigException("Thread count must be > 0");
    }
  }

  public void setKeepAliveTimeSec(long time) {
    executor.setKeepAliveTime(time, TimeUnit.SECONDS);
  }

  /**
   * Get the maximum number of worker threads in the pool.
   */
  @Override
  public int getPoolSize() {
    return executor.getMaximumPoolSize();
  }

  @Override
  public void setInstanceId(String schedInstId) {
    if (schedulerInstanceId != null) {
      INSTANCES.remove(schedulerInstanceId, this);
    }
    schedulerInstanceId = schedInstId;
    if (schedulerInstanceId != null) {
      INSTANCES.put(schedulerInstanceId, this);
    }
  }

  @Override
  public void setInstanceName(String schedName) {
    this.schedulerInstanceName = schedName;
  }

  /**
   * Set the thread priority of worker threads in the pool.
   */
  public void setThreadPriority(int priority) {
    this.priority = priority;
  }

  private class DynamicThreadFactory implements ThreadFactory {
    private final AtomicLong count = new AtomicLong(0L);

    @Override
    public Thread newThread(Runnable runnable) {
      Thread newThread = new Thread(runnable);
      newThread.setUncaughtExceptionHandler((th, e) -> LOGGER.error("ThreadPool error", e));
      newThread.setName(String.format("%s-%d", threadNamePrefix, count.getAndIncrement()));
      newThread.setPriority(priority);

      LOGGER.info("Thread - {} created", newThread);
      return newThread;
    }
  }

  private class NotifableJob implements Runnable {

    private Runnable job;

    NotifableJob(Runnable job) {
      this.job = job;
    }

    @Override
    public String toString() {
      return job.toString();
    }

    @Override
    public void run() {
      try {
        job.run();
      } finally {
        synchronized (poolEvent) {
          poolEvent.notifyAll();
        }
      }
    }
  }
}
