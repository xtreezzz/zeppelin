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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This is an interface for system events logger.
 */
public interface SystemLogger {

  /**
   * Type of system event.
   */
  enum EventType {
    ADD_INTERPRETER;
  }

  /**
   * Adds record about system event to event log.
   *
   * @param eventType - type of event, never {@code null}.
   * @param username - user, who made the change, never {@code null}.
   * @param message - changes msg, never {@code null}.
   * @param description - human-readable change description, may be {@code null}.
   */
  void log(@Nonnull final EventType eventType,
           @Nonnull final String username,
           @Nonnull final String message,
           @Nullable final String description);

}