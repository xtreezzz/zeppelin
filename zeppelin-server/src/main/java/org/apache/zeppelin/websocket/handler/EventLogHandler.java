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
package org.apache.zeppelin.websocket.handler;

import org.apache.zeppelin.Logger;
import org.apache.zeppelin.SystemLogger.EventType;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.storage.DatabaseNoteRepository;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.SockMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class EventLogHandler extends AbstractHandler {

  private final Logger logger;

  @Autowired
  public EventLogHandler(final ConnectionManager connectionManager,
                         final DatabaseNoteRepository noteRepository,
                         final Logger logger) {
    super(connectionManager, noteRepository);
    this.logger = logger;
  }

  public void log(final SockMessage message) {
    final ServiceContext serviceContext = getServiceContext(message);
    final EventType eventType = EventType.valueOf(message.getNotNull("eventType"));
    final String eventMessage = message.getNotNull("message");
    final String description = message.getNotNull("description");

    logger.log(eventType, serviceContext.getAutheInfo().getUser(), eventMessage, description);
  }
}
