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

import org.apache.zeppelin.NoteService;
import org.apache.zeppelin.service.ConfigurationService;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.Map;

@Component
public class SettingsHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SettingsHandler.class);

  private final ConfigurationService configurationService;
  //private final InterpreterFactory interpreterFactory;

  @Autowired
  public SettingsHandler(final NoteService noteService,
                         final ConnectionManager connectionManager,
                         final ConfigurationService configurationService) {
                         //final InterpreterFactory interpreterFactory) {
    super(connectionManager, noteService);
    this.configurationService = configurationService;
    //this.interpreterFactory = interpreterFactory;
  }

  public void sendAllConfigurations(final WebSocketSession conn) throws IOException {
    final Map<String, String> properties = configurationService.getAllProperties();

    final SockMessage message = new SockMessage(Operation.CONFIGURATIONS_INFO)
            .put("configurations", properties);
    conn.sendMessage(message.toSend());
  }

  //FIXME
  public void getEditorSetting(final WebSocketSession conn, final SockMessage fromSockMessage) throws IOException {
    //    final ServiceContext serviceContext = getServiceContext(fromSockMessage);
    //
    //    final Note note = safeLoadNote("noteId", fromSockMessage, Permission.ANY, serviceContext, conn);
    //    final Paragraph p = safeLoadParagraph("paragraphId", fromSockMessage, note);
    //
    //    final String replName = fromSockMessage.safeGetType("magic", LOG);
    //
    //    try {
    //      final Interpreter intp = interpreterFactory.getInterpreter(
    //              serviceContext.getAutheInfo().getUser(),
    //              note.getUuid(),
    //              replName,
    //              note.getDefaultInterpreterGroup());
    //      final Map<String, Object> settings = interpreterSettingRepository.
    //              getEditorSetting(intp, serviceContext.getAutheInfo().getUser(), note.getUuid(), replName);
    //
    //      final SockMessage message = new SockMessage(Operation.EDITOR_SETTING)
    //              .put("paragraphId", p.getUuid())
    //              .put("editor", settings);
    //      conn.sendMessage(message.toSend());
    //
    //    } catch (final InterpreterNotFoundException e) {
    //      LOG.warn(new IOException("Fail to find interpreter", e).getMessage());
    //    }
  }


  //FIXME
  public void getInterpreterSettings(final WebSocketSession conn) throws IOException {
  //    final List<InterpreterSettingV2> availableSettings = interpreterSettingRepository.get();
  //    final SockMessage message = new SockMessage(Operation.INTERPRETER_SETTINGS)
  //            .put("interpreterSettings", availableSettings);
  //    conn.sendMessage(message.toSend());
  }

}
