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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.io.IOException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.apache.zeppelin.interpreter.InterpreterFactory;
import org.apache.zeppelin.interpreter.InterpreterSettingManager;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.NotebookAuthorization;
import org.apache.zeppelin.notebook.repo.NotebookRepo;
import org.apache.zeppelin.rest.message.SchedulerConfigRequest;
import org.apache.zeppelin.search.SearchService;
import org.apache.zeppelin.user.Credentials;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.SchedulerException;

public class AdminServiceTest {
  private Notebook notebook;

  /**
   * Creates <code>{@link Notebook}</code> instance.
   *
   * @see Notebook#quartzSched
   */
  @Before
  public void setUp() throws IOException, SchedulerException {
    System.setProperty("org.quartz.properties", "./quartz.properties");
    notebook = new Notebook(
        mock(ZeppelinConfiguration.class), mock(NotebookRepo.class),
        mock(InterpreterFactory.class), mock(InterpreterSettingManager.class),
        mock(SearchService.class), mock(NotebookAuthorization.class),
        mock(Credentials.class)
    );
  }

  @After
  public void tearDown() {
    System.clearProperty("org.quartz.properties");
    notebook.close();
  }

  @Test
  public void testSetLoggerLevel() {
    AdminService adminService = new AdminService();
    String testLoggerName = "test";
    Logger logger = adminService.getLogger(testLoggerName);
    Level level = logger.getLevel();
    boolean setInfo = false;
    if (Level.INFO == level) {
      // if a current level is INFO, set DEBUG to check if it's changed or not
      logger.setLevel(Level.DEBUG);
    } else {
      logger.setLevel(Level.INFO);
      setInfo = true;
    }

    logger = adminService.getLogger(testLoggerName);
    assertTrue(
        "Level of logger should be changed",
        (setInfo && Level.INFO == logger.getLevel())
            || (!setInfo && Level.DEBUG == logger.getLevel()));
  }

  @Test
  public void testChangeThreadPoolSize() throws SchedulerException {
    SchedulerConfigRequest request = AdminService.getSchedulersInfoList().iterator().next();
    Integer newPoolSize = request.getPoolSize() + 1;
    AdminService.setSchedulerThreadPoolSize(request.getId(), newPoolSize);

    request = AdminService.getSchedulersInfoList().iterator().next();
    assertEquals(newPoolSize, request.getPoolSize());
    assertEquals(
        "org.apache.zeppelin.scheduler.pool.DynamicThreadPool",
        request.getPoolClass()
    );
    AdminService.setSchedulerThreadPoolSize(request.getId(), newPoolSize - 1);
  }
}
