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

import org.apache.zeppelin.repositories.ZeppelinNoteRepository;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.remote.RemoteInterpreterProcessListener;
import org.apache.zeppelin.interpreterV2.configuration.InterpreterSettingRepository;
import org.apache.zeppelin.interpreterV2.configuration.InterpreterSettingV2;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Component
public class RemoteInterpreterProcessListenerImpl implements RemoteInterpreterProcessListener {

  private final ConnectionManager connectionManager;
  private final ZeppelinNoteRepository zeppelinNoteRepository;
  //FIXME
  private final InterpreterSettingRepository interpreterSettingRepository;

  public RemoteInterpreterProcessListenerImpl(final ConnectionManager connectionManager,
                                              final ZeppelinNoteRepository zeppelinNoteRepository,
                                              final InterpreterSettingRepository interpreterSettingRepository) {
    this.connectionManager = connectionManager;
    this.zeppelinNoteRepository = zeppelinNoteRepository;
    this.interpreterSettingRepository = interpreterSettingRepository;
  }

  /**
   * This callback is for the paragraph that runs on ZeppelinServer.
   *
   * @param output output to append
   */
  @Override
  public void onOutputAppend(final String noteId, final String paragraphId, final int index, final String output) {
    final SockMessage msg = new SockMessage(Operation.PARAGRAPH_APPEND_OUTPUT)
            .put("noteId", noteId)
            .put("paragraphId", paragraphId)
            .put("index", index)
            .put("data", output);
    connectionManager.broadcast(noteId, msg);
  }

  /**
   * This callback is for the paragraph that runs on ZeppelinServer.
   *
   * @param output output to update (replace)
   */
  @Override
  public void onOutputUpdated(final String noteId,
                              final String paragraphId,
                              final int index,
                              final InterpreterResult.Type type,
                              final String output) {
    final SockMessage msg = new SockMessage(Operation.PARAGRAPH_UPDATE_OUTPUT)
            .put("noteId", noteId)
            .put("paragraphId", paragraphId)
            .put("index", index)
            .put("type", type)
            .put("data", output);
    final Note note = zeppelinNoteRepository.getNote(noteId);

    connectionManager.broadcast(noteId, msg);
  }

  /**
   * This callback is for the paragraph that runs on ZeppelinServer.
   */
  @Override
  public void onOutputClear(final String noteId,
                            final String paragraphId) {
    final Note note = zeppelinNoteRepository.getNote(noteId);

    //note.clearParagraphOutput(paragraphId);
    final Paragraph paragraph = note.getParagraph(paragraphId);
    final SockMessage msg = new SockMessage(Operation.PARAGRAPH)
            .put("paragraph", paragraph);
    connectionManager.broadcast(note.getId(), msg);
  }

  @Override
  public void onParaInfosReceived(final String noteId,
                                  final String paragraphId,
                                  final String interpreterSettingId,
                                  final Map<String, String> metaInfos) {
    final Note note = zeppelinNoteRepository.getNote(noteId);
    if (note != null) {
      final Paragraph paragraph = note.getParagraph(paragraphId);
      if (paragraph != null) {
        final InterpreterSettingV2 setting = interpreterSettingRepository.get(interpreterSettingId);
        final String label = metaInfos.get("label");
        final String tooltip = metaInfos.get("tooltip");
        final List<String> keysToRemove = Arrays.asList("noteId", "paraId", "label", "tooltip");
        for (final String removeKey : keysToRemove) {
          metaInfos.remove(removeKey);
        }

        //paragraph.updateRuntimeInfos(label, tooltip, metaInfos, setting.getGroup(), setting.getId());

        final SockMessage msg = new SockMessage(Operation.PARAS_INFO)
                .put("id", paragraphId)
                .put("infos", "paragraph.getRuntimeInfos()");
        connectionManager.broadcast(note.getId(), msg);
      }
    }
  }


  //TODO(KOT): REWRITE
  @Override
  public void runParagraphs(final String noteId,
                            final List<Integer> paragraphIndices,
                            final List<String> paragraphIds,
                            final String curParagraphId) throws IOException {
    final Note note = zeppelinNoteRepository.getNote(noteId);
    final List<String> toBeRunParagraphIds = new ArrayList<>();
    if (note == null) {
      throw new IOException("Not existed noteId: " + noteId);
    }
    if (!paragraphIds.isEmpty() && !paragraphIndices.isEmpty()) {
      throw new IOException("Can not specify paragraphIds and paragraphIndices together");
    }
    if (paragraphIds != null && !paragraphIds.isEmpty()) {
      for (final String paragraphId : paragraphIds) {
        if (note.getParagraph(paragraphId) == null) {
          throw new IOException("Not existed paragraphId: " + paragraphId);
        }
        if (!paragraphId.equals(curParagraphId)) {
          toBeRunParagraphIds.add(paragraphId);
        }
      }
    }
    if (paragraphIndices != null && !paragraphIndices.isEmpty()) {
      for (final int paragraphIndex : paragraphIndices) {
        if (note.getParagraphs().get(paragraphIndex) == null) {
          throw new IOException("Not existed paragraphIndex: " + paragraphIndex);
        }
        if (!note.getParagraphs().get(paragraphIndex).getId().equals(curParagraphId)) {
          toBeRunParagraphIds.add(note.getParagraphs().get(paragraphIndex).getId());
        }
      }
    }
    // run the whole note except the current paragraph
    if (paragraphIds.isEmpty() && paragraphIndices.isEmpty()) {
      for (final Paragraph paragraph : note.getParagraphs()) {
        if (!paragraph.getId().equals(curParagraphId)) {
          toBeRunParagraphIds.add(paragraph.getId());
        }
      }
    }
    final Runnable runThread = new Runnable() {
      @Override
      public void run() {
        for (final String paragraphId : toBeRunParagraphIds) {
          //note.run(paragraphId, true);
        }
      }
    };
    //TODO(KOT): FIX
    //executorService.submit(runThread);
  }
}
