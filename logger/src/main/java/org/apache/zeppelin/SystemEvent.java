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
package org.apache.zeppelin;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.StringJoiner;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import ru.tinkoff.zeppelin.storage.ZLog.ET;

public class SystemEvent implements Serializable {

  private long databaseId;

  @Nonnull
  private final String username;

  @Nonnull
  private final ET type;

  @Nonnull
  private final String message;

  @Nullable
  private final String description;

  @Nonnull
  private final LocalDateTime actionTime;


  public SystemEvent(final long databaseId,
                     @Nonnull final ET type,
                     @Nonnull final String username,
                     @Nonnull final String message,
                     @Nullable final String description,
                     @Nonnull final LocalDateTime actionTime) {
    this.databaseId = databaseId;
    this.username = username;
    this.type = type;
    this.message = message;
    this.description = description;
    this.actionTime = actionTime;
  }

  public SystemEvent(@Nonnull final ET type,
                     @Nonnull final String username,
                     @Nonnull final String message,
                     @Nullable final String description) {
    this.username = username;
    this.type = type;
    this.message = message;
    this.description = description;
    this.actionTime = LocalDateTime.now();
  }

  public long getDatabaseId() {
    return databaseId;
  }

  public void setDatabaseId(final long databaseId) {
    this.databaseId = databaseId;
  }

  @Nonnull
  public String getUsername() {
    return username;
  }

  @Nonnull
  public ET getType() {
    return type;
  }

  @Nonnull
  public String getMessage() {
    return message;
  }

  @Nullable
  public String getDescription() {
    return description;
  }

  @Nonnull
  public LocalDateTime getActionTime() {
    return actionTime;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("databaseId=" + databaseId)
        .add("username='" + username + "'")
        .add("type=" + type)
        .add("message='" + message + "'")
        .add("description='" + description + "'")
        .add("actionTime=" + actionTime)
        .toString();
  }
}
