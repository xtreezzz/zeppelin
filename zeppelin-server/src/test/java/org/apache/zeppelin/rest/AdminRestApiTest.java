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

package org.apache.zeppelin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.zeppelin.rest.message.SchedulerConfigRequest;
import org.apache.zeppelin.service.AdminService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.quartz.SchedulerException;

public class AdminRestApiTest extends AbstractTestRestApi {
  private final Gson gson = new Gson();

  @BeforeClass
  public static void init() throws Exception {
    System.setProperty("org.quartz.properties", "./quartz.properties");
    AbstractTestRestApi.startUp(AdminRestApiTest.class.getSimpleName());
  }

  @AfterClass
  public static void destroy() throws Exception {
    System.clearProperty("org.quartz.properties");
    AbstractTestRestApi.shutDown();
  }

  @Test
  public void testGetSchedulerSettings() throws IOException, SchedulerException {
    GetMethod getSchedulerSettings = httpGet("/admin/cron/pool/");
    assertThat(getSchedulerSettings, isAllowed());
    LOG.info(getSchedulerSettings.getResponseBodyAsString());
    Map<String, Object> resp =
        gson.fromJson(
            getSchedulerSettings.getResponseBodyAsString(),
            new TypeToken<Map<String, Object>>() {
            }.getType());

    ArrayList<SchedulerConfigRequest> parsedResp =
        gson.fromJson(
            resp.get("body").toString(),
            new TypeToken<ArrayList<SchedulerConfigRequest>>() {
            }.getType());

    assertEquals(AdminService.getSchedulersInfoList(), parsedResp);
    getSchedulerSettings.releaseConnection();
  }

  @Test
  public void testUpdateThreadPoolSize() throws SchedulerException, IOException {
    SchedulerConfigRequest request = AdminService.getSchedulersInfoList().iterator().next();

    String postUrl = String.format("/admin/cron/pool/%s", request.getId());
    Integer newPoolSize = request.getPoolSize() - 1;
    String updateRequest = String.format("{\"poolSize\": \"%d\"}", newPoolSize);

    PostMethod post = httpPost(postUrl, updateRequest);
    assertThat("Test update method:", post, isAllowed());
    post.releaseConnection();

    request = AdminService.getSchedulersInfoList().iterator().next();
    assertEquals(newPoolSize, request.getPoolSize());

    // set pool size to start state
    AdminService.setSchedulerThreadPoolSize(request.getId(), newPoolSize + 1);

    // poolSize should be > 0
    updateRequest = "{\"poolSize\": \"0\"}";
    post = httpPost(postUrl, updateRequest);
    assertThat("Test update method:", post, isBadRequest());
    post.releaseConnection();
  }
}
