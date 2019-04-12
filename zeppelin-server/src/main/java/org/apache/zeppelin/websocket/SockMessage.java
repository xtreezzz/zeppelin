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

package org.apache.zeppelin.websocket;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.springframework.web.socket.TextMessage;

import java.util.HashMap;
import java.util.Map;

/**
 * Zeppelin websocket massage template class.
 */
public class SockMessage {

  private static final Gson gson = new Gson();

  public Operation op;
  public Map<String, Object> data = new HashMap<>();

  public SockMessage(final Operation op) {
    this.op = op;
  }

  public SockMessage put(final String k, final Object v) {
    data.put(k, v);
    return this;
  }

  public <T> T getNotNull(final String key) {
    final Object value = data.get(key);
    if (value == null) {
      throw new IllegalStateException(key + " is not defined");
    }
    return (T) value;
  }

  public <T> T getOrDefault(final String key, final T defaultValue) {
    final Object value = data.get(key);
    return (T) (value != null ? value : defaultValue);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Message{");
    sb.append("data=").append(data);
    sb.append(", op=").append(op);
    sb.append('}');
    return sb.toString();
  }

  public TextMessage toSend() {
    return new TextMessage(gson.toJson(this));
  }

  public static SockMessage fromJson(final String json) {
    return gson.fromJson(json, SockMessage.class);
  }


}
