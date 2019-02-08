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

import com.google.common.collect.Lists;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Notebook;
import org.apache.zeppelin.notebook.NotebookAuthorization;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.List;

@Component
public class CompletionHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(CompletionHandler.class);

  @Autowired
  public CompletionHandler(final NotebookAuthorization notebookAuthorization,
                           final Notebook notebook,
                           final ConnectionManager connectionManager) {
    super(notebookAuthorization, notebook, connectionManager);
  }


  public void completion(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    final String buffer = fromMessage.safeGetType("buf", LOG);
    final Integer cursor = fromMessage.safeGetType("cursor", LOG);

    final List<InterpreterCompletion> completions = Lists.newArrayList();
    try {
      completions.addAll(note.completion(p.getId(), buffer, cursor, serviceContext.getAutheInfo()));
    } catch (final RuntimeException e) {
      // SKIP
    }

    final SockMessage message = new SockMessage(Operation.COMPLETION_LIST)
            .put("id", p.getId())
            .put("completions", completions);
    conn.sendMessage(message.toSend());
  }
}
