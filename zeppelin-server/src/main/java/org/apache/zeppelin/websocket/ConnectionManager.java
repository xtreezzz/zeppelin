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


import org.apache.zeppelin.EventService;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.display.GUI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Manager class for managing websocket connections
 */
@Component
public class ConnectionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionManager.class);

  // noteId -> connection
  private final Map<String, Queue<WebSocketSession>> noteSocketMap = new ConcurrentHashMap<>();


  public String getAssociatedNoteId(final WebSocketSession socket) {
    final Set<String> noteIds = noteSocketMap.keySet();
    for (final String noteId : noteIds) {
      final Queue<WebSocketSession> sockets = noteSocketMap.get(noteId);
      if (sockets.contains(socket)) {
        return noteId;
      }
    }
    return null;
  }

  public void addSubscriberToNode(final String noteId, final WebSocketSession socket) {
    LOGGER.debug("Add connection {} to note: {}", socket, noteId);
    // make sure a socket relates only an single note.
    removeSubscribersFromAllNote(socket);

    final Queue<WebSocketSession> sessions = noteSocketMap.computeIfAbsent(noteId, k -> new ConcurrentLinkedQueue<>());
    if (!sessions.contains(socket)) {
      sessions.add(socket);
    }
  }

  public void removeSubscriberFromNote(final String noteId, final WebSocketSession socket) {
    LOGGER.debug("Remove connection {} from note: {}", socket, noteId);
    final Queue<WebSocketSession> sessions = noteSocketMap.get(noteId);
    if (sessions != null) {
      sessions.remove(socket);
    }
  }

  public void removeNoteSubscribers(final String noteId) {
    noteSocketMap.remove(noteId);
  }

  public void removeSubscribersFromAllNote(final WebSocketSession socket) {
    final Set<String> noteIds = noteSocketMap.keySet();
    for (final String noteId : noteIds) {
      removeSubscriberFromNote(noteId, socket);
    }
  }

  public void broadcast(final String noteId, final SockMessage m) {
    final Queue<WebSocketSession> sessions = noteSocketMap.get(noteId);
    if (sessions == null || sessions.size() == 0) {
      return;
    }
    LOGGER.debug("SEND >> " + m);
    for (final WebSocketSession session : sessions) {
      try {
        session.sendMessage(m.toSend());
      } catch (final Exception e) {
        LOGGER.error("Send error: " + m, e);
      }
    }
  }

  @Scheduled(fixedDelay = 20)
  private void getEvents() {
    EventService.getEvent();
  }
}
