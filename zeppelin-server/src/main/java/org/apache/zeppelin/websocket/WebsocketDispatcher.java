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

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.rest.exception.ForbiddenException;
import org.apache.zeppelin.ticket.TicketContainer;
import org.apache.zeppelin.websocket.handler.CompletionHandler;
import org.apache.zeppelin.websocket.handler.EventLogHandler;
import org.apache.zeppelin.websocket.handler.NoteFormsHandler;
import org.apache.zeppelin.websocket.handler.NoteHandler;
import org.apache.zeppelin.websocket.handler.NoteRevisionHandler;
import org.apache.zeppelin.websocket.handler.ParagraphHandler;
import org.apache.zeppelin.websocket.handler.RunnerHandler;
import org.apache.zeppelin.websocket.handler.SettingsHandler;
import org.apache.zeppelin.websocket.handler.SpellHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@Component
public class WebsocketDispatcher extends TextWebSocketHandler {

  private static final Logger LOG = LoggerFactory.getLogger(WebsocketDispatcher.class);
  private final SettingsHandler settingsService;
  private final ParagraphHandler paragraphService;
  private final NoteHandler noteService;
  private final NoteRevisionHandler noteRevisionService;
  private final NoteFormsHandler noteFormsService;
  private final SpellHandler spellHandler;
  private final RunnerHandler runnerHandler;
  private final CompletionHandler completionHandler;
  private final ConnectionManager sessionectionManager;
  private final EventLogHandler eventLogHandler;

  @Autowired
  public WebsocketDispatcher(final SettingsHandler settingsService,
                             final ParagraphHandler paragraphService,
                             final NoteHandler noteService,
                             final NoteRevisionHandler noteRevisionService,
                             final NoteFormsHandler noteFormsService,
                             final SpellHandler spellHandler,
                             final RunnerHandler runnerHandler,
                             final CompletionHandler completionHandler,
                             final ConnectionManager sessionectionManager,
                             final EventLogHandler eventLogHandler) {
    this.settingsService = settingsService;
    this.paragraphService = paragraphService;
    this.noteService = noteService;
    this.noteRevisionService = noteRevisionService;
    this.noteFormsService = noteFormsService;
    this.spellHandler = spellHandler;
    this.runnerHandler = runnerHandler;
    this.completionHandler = completionHandler;
    this.sessionectionManager = sessionectionManager;
    this.eventLogHandler = eventLogHandler;
  }

  @Override
  public void afterConnectionEstablished(final WebSocketSession session) throws Exception {
    //sessionectionManager.addSession(session);
  }

  @Override
  public void handleTransportError(final WebSocketSession session, final Throwable exception) throws Exception {
    //sessionectionManager.removeSession(session);
    sessionectionManager.removeSubscribersFromAllNote(session);
  }

  @Override
  public void afterConnectionClosed(final WebSocketSession session, final CloseStatus status) throws Exception {
    //sessionectionManager.removeSession(session);
    sessionectionManager.removeSubscribersFromAllNote(session);
  }

  @Override
  protected void handleTextMessage(final WebSocketSession session, final TextMessage message) throws Exception {
    LOG.info("Start handle message: " + message.getPayload());
    try {
      final SockMessage messagereceived = SockMessage.fromJson(message.getPayload());
      if (messagereceived.op != Operation.PING) {
        LOG.debug("RECEIVE: " + messagereceived.op +
                ", RECEIVE PRINCIPAL: " + messagereceived.principal +
                ", RECEIVE TICKET: " + messagereceived.ticket +
                ", RECEIVE ROLES: " + messagereceived.roles +
                ", RECEIVE DATA: " + messagereceived.data);
      }
      if (LOG.isTraceEnabled()) {
        LOG.trace("RECEIVE MSG = " + messagereceived);
      }

      final String ticket = TicketContainer.instance.getTicket(messagereceived.principal);
      if (ticket != null && (messagereceived.ticket == null || !ticket.equals(messagereceived.ticket))) {
        if (StringUtils.isEmpty(messagereceived.ticket)) {
          LOG.debug("{} message: invalid ticket {} != {}", messagereceived.op, messagereceived.ticket, ticket);
        } else {
          if (!messagereceived.op.equals(Operation.PING)) {
            session.sendMessage(new SockMessage(Operation.SESSION_LOGOUT).put("info", "Your ticket is invalid possibly due to server restart. Please login again.").toSend());
          }
        }
        return;
      }

      //final ZeppelinConfiguration conf = ZeppelinConfiguration.create();
      final boolean allowAnonymous = true;
      if (!allowAnonymous && messagereceived.principal.equals("anonymous")) {
        throw new Exception("Anonymous access not allowed ");
      }

     /* if (SockMessage.isDisabledForRunningNotes(messagereceived.op)) {
        Note note = notebook.getNote((String) messagereceived.get("noteId"));
        if (note != null && note.isRunning()) {
          throw new Exception("Note is now running sequentially. Can not be performed: " +
                  messagereceived.op);
        }
      }
      */

      // Lets be elegant here
      switch (messagereceived.op) {
        case FIRE_EVENT:
          eventLogHandler.log(messagereceived);
          break;
        case LIST_NOTES:
          noteService.listNotesInfo(session, messagereceived);
          break;
        case RELOAD_NOTES_FROM_REPO:
          //noteService.broadcastReloadedNoteList(messagereceived);
          break;
        case GET_HOME_NOTE:
          noteService.getHomeNote(session, messagereceived);
          break;
        case GET_NOTE:
          noteService.getNote(session, messagereceived);
          break;
        case NEW_NOTE:
          noteService.createNote(session, messagereceived);
          break;
        case DEL_NOTE:
          noteService.deleteNote(session, messagereceived);
          break;
        case REMOVE_FOLDER:
          noteService.removeFolder(messagereceived);
          break;
        case MOVE_NOTE_TO_TRASH:
          noteService.moveNoteToTrash(session, messagereceived);
          break;
        case MOVE_FOLDER_TO_TRASH:
          noteService.moveFolderToTrash(messagereceived);
          break;
        case EMPTY_TRASH:
          noteService.emptyTrash(messagereceived);
          break;
        case RESTORE_FOLDER:
          noteService.restoreFolder(messagereceived);
          break;
        case RESTORE_NOTE:
          noteService.restoreNote(session, messagereceived);
          break;
        case RESTORE_ALL:
          noteService.restoreAll(messagereceived);
          break;
        case CLONE_NOTE:
          noteService.cloneNote(session, messagereceived);
          break;
        case IMPORT_NOTE:
          //noteService.importNote(messagereceived);
          break;
        case COMMIT_PARAGRAPH:
          paragraphService.updateParagraph(session, messagereceived);
          break;
        case RUN_PARAGRAPH:
          runnerHandler.runParagraph(session, messagereceived);
          break;
        case PARAGRAPH_EXECUTED_BY_SPELL:
          spellHandler.broadcastSpellExecution(session, messagereceived);
          break;
        case RUN_ALL_PARAGRAPHS:
          runnerHandler.runAllParagraphs(session, messagereceived);
          break;
        case STOP_NOTE_EXECUTION:
          runnerHandler.stopNoteExecution(session, messagereceived);
          break;
        case CANCEL_PARAGRAPH:
          runnerHandler.cancelParagraph(session, messagereceived);
          break;
        case MOVE_PARAGRAPH:
          paragraphService.moveParagraph(session, messagereceived);
          break;
        case INSERT_PARAGRAPH:
          paragraphService.insertParagraph(session, messagereceived);
          break;
        case COPY_PARAGRAPH:
          paragraphService.copyParagraph(session, messagereceived);
          break;
        case PARAGRAPH_REMOVE:
          paragraphService.removeParagraph(session, messagereceived);
          break;
        case PARAGRAPH_CLEAR_OUTPUT:
          paragraphService.clearParagraphOutput(session, messagereceived);
          break;
        case PARAGRAPH_CLEAR_ALL_OUTPUT:
          noteService.clearAllParagraphOutput(session, messagereceived);
          break;
        case NOTE_UPDATE:
          noteService.updateNote(session, messagereceived);
          break;
        case FOLDER_RENAME:
          noteService.renameFolder(messagereceived);
          break;
        case UPDATE_PERSONALIZED_MODE:
          //noteService.updatePersonalizedMode(session, messagereceived);
          break;
        case COMPLETION:
          completionHandler.completion(session, messagereceived);
          break;
        case PING:
          break; //do nothing
        case LIST_CONFIGURATIONS:
          settingsService.sendAllConfigurations(session);
          break;
        case CHECKPOINT_NOTE:
          noteRevisionService.checkpointNote(session, messagereceived);
          break;
        case LIST_REVISION_HISTORY:
          noteRevisionService.listRevisionHistory(session, messagereceived);
          break;
        case SET_NOTE_REVISION:
          noteRevisionService.setNoteRevision(session, messagereceived);
          break;
        case NOTE_REVISION:
          noteRevisionService.getNoteByRevision(session, messagereceived);
          break;
        case NOTE_REVISION_FOR_COMPARE:
          noteRevisionService.getNoteByRevisionForCompare(session, messagereceived);
          break;
        case LIST_NOTE_JOBS:
          //unicastNoteJobInfo(session, messagereceived);
          break;
        case UNSUBSCRIBE_UPDATE_NOTE_JOBS:
          //unsubscribeNoteJobInfo(session);
          break;
        case GET_INTERPRETER_BINDINGS:
          // getInterpreterBindings(session, messagereceived);
          break;
        case EDITOR_SETTING:
          //settingsService.getEditorSetting(session, messagereceived);
          break;
        case GET_INTERPRETER_SETTINGS:
          settingsService.getInterpreterSettings(session);
          break;
        case WATCHER:
          //sessionectionManager.switchConnectionToWatcher(session);
          break;
        case SAVE_NOTE_FORMS:
          noteFormsService.saveNoteForms(session, messagereceived);
          break;
        case REMOVE_NOTE_FORMS:
          noteFormsService.removeNoteForms(session, messagereceived);
          break;
        case PATCH_PARAGRAPH:
          //paragraphService.patchParagraph(session, messagereceived);
          break;
        default:
          break;
      }
    } catch (final ForbiddenException e) {
      try {
        session.sendMessage(new SockMessage(Operation.AUTH_INFO).put("info", e.getMessage()).toSend());
      } catch (final IOException iox) {
        LOG.error("Fail to send error info", iox);
      }
    } catch (final Exception e) {
      LOG.error("Can't handle message: " + message.getPayload(), e);
      try {
        session.sendMessage(new SockMessage(Operation.ERROR_INFO).put("info", e.getMessage()).toSend());
      } catch (final IOException iox) {
        LOG.error("Fail to send error info", iox);
      }
    }
    LOG.info("Done handle message: " + message.getPayload());
  }
}
