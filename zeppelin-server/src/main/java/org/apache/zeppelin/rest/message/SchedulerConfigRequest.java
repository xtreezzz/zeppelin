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
import java.util.Objects;
import org.apache.zeppelin.common.JsonSerializable;
import org.apache.zeppelin.rest.AdminRestApi;

/**
 * Scheduler configuration request message for admin rest api.
 *
 * @see AdminRestApi#getQuartzSchedulerPoolInfo()
 * @see AdminRestApi#changeScheduler(String, String)
 */
public class SchedulerConfigRequest implements JsonSerializable {
  private static final Gson gson = new Gson();

  public SchedulerConfigRequest(String name, String id, Integer poolSize,
      String poolClass, String storeClass) {
    this.name = name;
    this.id = id;
    this.poolSize = poolSize;
    this.poolClass = poolClass;
    this.storeClass = storeClass;
  }

  private String name;
  private String id;
  private Integer poolSize;
  private String poolClass;
  private String storeClass;

  public Integer getPoolSize() {
    return poolSize;
  }

  public String toJson() {
    return gson.toJson(this);
  }

  public static SchedulerConfigRequest fromJson(String json) {
    return gson.fromJson(json, SchedulerConfigRequest.class);
  }

  public String getName() {
    return name;
  }

  public String getId() {
    return id;
  }

  public String getPoolClass() {
    return poolClass;
  }

  public String getStoreClass() {
    return storeClass;
  }

  @Override
  public String toString() {
    return String.join(" ",
        "name:", name,
        "id:", id,
        "storeClass:", storeClass,
        "poolClass:", poolClass,
        "poolSize:", poolSize.toString());
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o == null || o.getClass() != this.getClass()) {
      return false;
    }
    SchedulerConfigRequest other = (SchedulerConfigRequest) o;
    return Objects.equals(this.id, other.id)
        && Objects.equals(this.name, other.name)
        && Objects.equals(this.poolSize, other.poolSize)
        && Objects.equals(this.storeClass, other.storeClass)
        && Objects.equals(this.poolClass, other.poolClass);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, id, poolSize, storeClass, poolClass);
  }
}
