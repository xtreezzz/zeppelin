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
package org.apache.zeppelin.rest.message;

import com.google.gson.Gson;
import org.apache.zeppelin.rest.AdminRestApi;

/**
 * Scheduler configuration request message for admin rest api.
 * @see AdminRestApi#getQuartzSchedulerPoolInfo()
 * @see AdminRestApi#changeSchedulerPoolSize(String, String)
 */
public class SchedulerConfigRequest {
  private static final Gson gson = new Gson();

  private Integer poolSize;

  public Integer getPoolSize() {
    return poolSize;
  }

  public String toJson() {
    return gson.toJson(this);
  }

  public static SchedulerConfigRequest fromJson(String json) {
    return gson.fromJson(json, SchedulerConfigRequest.class);
  }
}
