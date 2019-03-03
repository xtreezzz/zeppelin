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

package org.apache.zeppelin.listener;

import org.apache.zeppelin.ZeppelinNoteRepository;
import org.apache.zeppelin.interpreter.InterpreterSetting;
import org.apache.zeppelin.interpreter.InterpreterSettingManager;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.display.AngularObject;
import org.apache.zeppelin.notebook.display.AngularObjectRegistryListener;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class AngularObjectRegistryListenerImpl implements AngularObjectRegistryListener {

  private final ConnectionManager connectionManager;
  private final ZeppelinNoteRepository zeppelinNoteRepository;
  private final InterpreterSettingManager interpreterSettingManager;

  @Autowired
  public AngularObjectRegistryListenerImpl(final ConnectionManager connectionManager,
                                           final ZeppelinNoteRepository zeppelinNoteRepository,
                                           final InterpreterSettingManager interpreterSettingManager) {
    this.connectionManager = connectionManager;
    this.zeppelinNoteRepository = zeppelinNoteRepository;
    this.interpreterSettingManager = interpreterSettingManager;
  }

  @Override
  public void onAdd(final String interpreterGroupId,
                    final AngularObject object) {
    onUpdate(interpreterGroupId, object);
  }

  @Override
  public void onUpdate(final String interpreterGroupId,
                       final AngularObject object) {
    final List<Note> notes = zeppelinNoteRepository.getAllNotes();
    for (final Note note : notes) {
      if (object.getNoteId() != null && !note.getId().equals(object.getNoteId())) {
        continue;
      }

      final List<InterpreterSetting> intpSettings = interpreterSettingManager.getInterpreterSettings(note.getId());
      if (intpSettings.isEmpty()) {
        continue;
      }

      final SockMessage message = new SockMessage(Operation.ANGULAR_OBJECT_UPDATE)
              .put("angularObject", object)
              .put("interpreterGroupId", interpreterGroupId)
              .put("noteId", note.getId())
              .put("paragraphId", object.getParagraphId());

      connectionManager.broadcast(note.getId(), message);
    }
  }

  @Override
  public void onRemove(final String interpreterGroupId,
                       final String name,
                       final String noteId,
                       final String paragraphId) {
    for (final Note note : zeppelinNoteRepository.getAllNotes()) {
      if (noteId != null && !note.getId().equals(noteId)) {
        continue;
      }

      final List<String> settingIds = interpreterSettingManager.getSettingIds();
      for (final String id : settingIds) {
        if (interpreterGroupId.contains(id)) {
          final SockMessage message = new SockMessage(Operation.ANGULAR_OBJECT_REMOVE)
                  .put("name", name)
                  .put("noteId", noteId)
                  .put("paragraphId", paragraphId);
          connectionManager.broadcast(note.getId(), message);
          break;
        }
      }
    }
  }
}
