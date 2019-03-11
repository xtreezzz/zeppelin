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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.zeppelin.repositories.ZeppelinNoteRepository;
import org.apache.zeppelin.interpreter.InterpreterGroup;
import org.apache.zeppelin.interpreter.remote.RemoteAngularObjectRegistry;
import org.apache.zeppelin.interpreterV2.configuration.InterpreterSettingRepository;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.display.AngularObject;
import org.apache.zeppelin.service.ServiceContext;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;

@Component
public class AngularObjectsHandler extends AbstractHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AngularObjectsHandler.class);
  private final InterpreterSettingRepository interpreterSettingRepository;

  public AngularObjectsHandler(final ConnectionManager connectionManager,
                               final ZeppelinNoteRepository zeppelinNoteRepository,
                               final InterpreterSettingRepository interpreterSettingRepository) {
    super(connectionManager, zeppelinNoteRepository);
    this.interpreterSettingRepository = interpreterSettingRepository;
  }

  /**
   * When angular object updated from client.
   */
  //TODO(KOT) check logic
  public void angularObjectUpdated(final WebSocketSession conn, final SockMessage fromMessage) throws IOException {
    //    final ServiceContext serviceContext = getServiceContext(fromMessage);
    //
    //    final String noteId = fromMessage.safeGetType("noteId", LOG);
    //    final String paragraphId = fromMessage.safeGetType("paragraphId", LOG);
    //    final String interpreterGroupId = fromMessage.safeGetType("interpreterGroupId", LOG);
    //    final String varName = fromMessage.safeGetType("name", LOG);
    //    final Object varValue = fromMessage.safeGetType("value", LOG);
    //
    //    final String user = serviceContext.getAutheInfo().getUser();
    //    AngularObject ao = null;
    //    boolean global = false;
    //    // propagate change to (Remote) AngularObjectRegistry
    //    final Note note = zeppelinNoteRepository.getNote(noteId);
    //    if (note != null) {
    //      final List<InterpreterSettingV2> settings = interpreterSettingRepository.getInterpreterSettings(note.getId());
    //      for (final InterpreterSetting setting : settings) {
    //        if (setting.getInterpreterGroup(user, note.getId()) == null) {
    //          continue;
    //        }
    //        if (interpreterGroupId.equals(setting.getInterpreterGroup(user, note.getId()).getId())) {
    //          final AngularObjectRegistry angularObjectRegistry =
    //                  setting.getInterpreterGroup(user, note.getId()).getAngularObjectRegistry();
    //
    //          // first trying to get local registry
    //          ao = angularObjectRegistry.get(varName, noteId, paragraphId);
    //          if (ao == null) {
    //            // then try notebook scope registry
    //            ao = angularObjectRegistry.get(varName, noteId, null);
    //            if (ao == null) {
    //              // then try global scope registry
    //              ao = angularObjectRegistry.get(varName, null, null);
    //              if (ao == null) {
    //                //LOGGER.warn("Object {} is not binded", varName);
    //              } else {
    //                // path from client -> server
    //                ao.set(varValue, false);
    //                global = true;
    //              }
    //            } else {
    //              // path from client -> server
    //              ao.set(varValue, false);
    //              global = false;
    //            }
    //          } else {
    //            ao.set(varValue, false);
    //            global = false;
    //          }
    //          break;
    //        }
    //      }
    //    }

    //    final SockMessage message = new SockMessage(Operation.ANGULAR_OBJECT_UPDATE)
    //            .put("angularObject", ao)
    //            .put("interpreterGroupId", interpreterGroupId)
    //            .put("noteId", noteId)
    //            .put("paragraphId", ao.getParagraphId());
    //    connectionManager.broadcast(noteId, message);
  }


  /**
   * Push the given Angular variable to the target interpreter angular registry given a noteId
   * and a paragraph id.
   */
  public void angularObjectClientBind(final WebSocketSession conn, final SockMessage fromMessage) throws Exception {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.ANY, serviceContext, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    final String varName = fromMessage.safeGetType("name", LOG);
    final Object varValue = fromMessage.safeGetType("value", LOG);


    final InterpreterGroup group = findInterpreterGroupForParagraph(note, p.getId());
    final RemoteAngularObjectRegistry registry = (RemoteAngularObjectRegistry) group.getAngularObjectRegistry();
    pushAngularObjectToRemoteRegistry(note.getId(), p.getId(), varName, varValue, registry, group.getId(), conn);
  }

  /**
   * Remove the given Angular variable to the target interpreter(s) angular registry given a noteId
   * and an optional list of paragraph id(s).
   */
  public void angularObjectClientUnbind(final WebSocketSession conn, final SockMessage fromMessage) throws Exception {
    final ServiceContext serviceContext = getServiceContext(fromMessage);

    final Note note = safeLoadNote("noteId", fromMessage, Permission.ANY, serviceContext, conn);
    final Paragraph p = safeLoadParagraph("id", fromMessage, note);

    final String varName = fromMessage.safeGetType("name", LOG);

    final InterpreterGroup group = findInterpreterGroupForParagraph(note, p.getId());
    final RemoteAngularObjectRegistry registry = (RemoteAngularObjectRegistry) group.getAngularObjectRegistry();
    removeAngularFromRemoteRegistry(note.getId(), p.getId(), varName, registry, group.getId(), conn);
  }

  private InterpreterGroup findInterpreterGroupForParagraph(final Note note, final String paragraphId) throws Exception {
    final Paragraph paragraph = note.getParagraph(paragraphId);
    if (paragraph == null) {
      throw new IllegalArgumentException("Unknown paragraph with id : " + paragraphId);
    }
    //TODO(egorklimov): Интерпретатор был убран из параграфа

    throw new NotImplementedException("Interpreter has been removed from Paragraph");
    //return paragraph.getBindedInterpreter().getInterpreterGroup();
  }

  private void pushAngularObjectToRemoteRegistry(final String noteId,
                                                 final String paragraphId,
                                                 final String varName,
                                                 final Object varValue,
                                                 final RemoteAngularObjectRegistry remoteRegistry,
                                                 final String interpreterGroupId,
                                                 final WebSocketSession conn) {

    final AngularObject ao = remoteRegistry.addAndNotifyRemoteProcess(varName, varValue, noteId, paragraphId);
    final SockMessage message = new SockMessage(Operation.ANGULAR_OBJECT_UPDATE)
            .put("angularObject", ao)
            .put("interpreterGroupId", interpreterGroupId)
            .put("noteId", noteId)
            .put("paragraphId", paragraphId);
    connectionManager.broadcast(noteId, message);
  }

  private void removeAngularFromRemoteRegistry(final String noteId,
                                               final String paragraphId,
                                               final String varName,
                                               final RemoteAngularObjectRegistry remoteRegistry,
                                               final String interpreterGroupId,
                                               final WebSocketSession conn) {

    final AngularObject ao = remoteRegistry.removeAndNotifyRemoteProcess(varName, noteId, paragraphId);
    final SockMessage message = new SockMessage(Operation.ANGULAR_OBJECT_REMOVE)
            .put("angularObject", ao)
            .put("interpreterGroupId", interpreterGroupId).put("noteId", noteId)
            .put("paragraphId", paragraphId);
    connectionManager.broadcast(noteId, message);
  }

  public void sendAllAngularObjects(final Note note, final String user, final WebSocketSession conn) throws IOException {
    //
    //    final List<InterpreterSetting> settings = interpreterSettingRepository.getInterpreterSettings(note.getId());
    //    if (settings == null || settings.size() == 0) {
    //      return;
    //    }
    //
    //    for (final InterpreterSetting intpSetting : settings) {
    //      if (intpSetting.getInterpreterGroup(user, note.getId()) == null) {
    //        continue;
    //      }
    //      final AngularObjectRegistry registry = intpSetting.getInterpreterGroup(user, note.getId()).getAngularObjectRegistry();
    //
    //      for (final AngularObject object : registry.getAllWithGlobal(note.getId())) {
    //        final SockMessage message = new SockMessage(Operation.ANGULAR_OBJECT_UPDATE)
    //                .put("angularObject", object)
    //                .put("interpreterGroupId", intpSetting.getInterpreterGroup(user, note.getId()).getId())
    //                .put("noteId", note.getId())
    //                .put("paragraphId", object.getParagraphId());
    //        conn.sendMessage(message.toSend());
    //      }
    //    }
  }
}
