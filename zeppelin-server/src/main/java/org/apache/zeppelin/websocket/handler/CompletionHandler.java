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

import org.apache.zeppelin.repositories.ZeppelinNoteRepository;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;

@Component
public class CompletionHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(CompletionHandler.class);
  //private InterpreterFactory interpreterFactory;

  @Autowired
  public CompletionHandler(final ConnectionManager connectionManager,
                           //final InterpreterFactory interpreterFactory,
                           final ZeppelinNoteRepository zeppelinNoteRepository) {
    super(connectionManager, zeppelinNoteRepository);
    //this.interpreterFactory = interpreterFactory;
  }


  public void completion(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
//    final ServiceContext serviceContext = getServiceContext(fromMessage);
//
//    final Note note = safeLoadNote("noteId", fromMessage, Permission.WRITER, serviceContext, conn);
//    final Paragraph p = safeLoadParagraph("id", fromMessage, note);
//
//    final String buffer = fromMessage.safeGetType("buf", LOG);
//    final Integer cursor = fromMessage.safeGetType("cursor", LOG);
//
//
//    final List<InterpreterCompletion> completions = Lists.newArrayList();
//    /*
//    try {
//      final Interpreter interpreter =
//              interpreterFactory.getInterpreter(
//                      serviceContext.getAutheInfo().getUser(),
//                      note.getId(),
//                      p.getIntpText(),
//                      note.getDefaultInterpreterGroup()
//              );
//
//      p.setAuthenticationInfo(serviceContext.getAutheInfo());
//      p.setText(buffer);
//
//      final int resultCursor = calculateCursorPosition(p.getScriptText(), buffer, cursor);
//      InterpreterContext interpreterContext = p.getInterpreterContext(null);
//
//      completions.addAll(interpreter.completion(p.getScriptText(), resultCursor, interpreterContext));
//
//    } catch (final InterpreterException e) {
//      // SKIP
//    }*/
//
//    final SockMessage message = new SockMessage(Operation.COMPLETION_LIST)
//            .put("id", p.getId())
//            .put("completions", completions);
//    conn.sendMessage(message.toSend());
  }

  public int calculateCursorPosition(String scriptText, String buffer, int cursor) {
    if (scriptText.isEmpty()) {
      return 0;
    }
    int countCharactersBeforeScript = buffer.indexOf(scriptText);
    if (countCharactersBeforeScript > 0) {
      cursor -= countCharactersBeforeScript;
    }

    return cursor;
  }

}
