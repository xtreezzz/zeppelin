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

import org.apache.commons.lang3.NotImplementedException;
import org.apache.zeppelin.repositories.ZeppelinNoteRepository;
import org.apache.zeppelin.interpreter.InterpreterResultMessage;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.ParagraphJobListener;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ParagraphJobListenerImpl implements ParagraphJobListener {

  private static final Logger LOG = LoggerFactory.getLogger(ParagraphJobListenerImpl.class);

  private final ConnectionManager connectionManager;
  private final ZeppelinNoteRepository zeppelinNoteRepository;


  @Autowired
  public ParagraphJobListenerImpl(final ConnectionManager connectionManager,
                                  final ZeppelinNoteRepository zeppelinNoteRepository) {
    this.connectionManager = connectionManager;
    this.zeppelinNoteRepository = zeppelinNoteRepository;
  }

  /**
   * This callback is for paragraph that runs on RemoteInterpreterProcess.
   */
  @Override
  public void onOutputAppend(final Paragraph paragraph, final int idx, final String output) {
    final SockMessage msg = new SockMessage(Operation.PARAGRAPH_APPEND_OUTPUT)
            .put("noteId", paragraph.getNote().getId())
            .put("paragraphId", paragraph.getId())
            .put("data", output);
    connectionManager.broadcast(paragraph.getNote().getId(), msg);
  }

  /**
   * This callback is for paragraph that runs on RemoteInterpreterProcess.
   */
  @Override
  public void onOutputUpdate(final Paragraph paragraph, final int idx, final InterpreterResultMessage result) {
    final SockMessage msg = new SockMessage(Operation.PARAGRAPH_UPDATE_OUTPUT)
            .put("noteId", paragraph.getNote().getId())
            .put("paragraphId", paragraph.getId())
            .put("data", result.getData());
    connectionManager.broadcast(paragraph.getNote().getId(), msg);
  }

  @Override
  public void onOutputUpdateAll(final Paragraph paragraph, final List<InterpreterResultMessage> msgs) {
    // TODO
  }

  @Override
  public void noteRunningStatusChange(final String noteId, final boolean newStatus) {
    final SockMessage message = new SockMessage(Operation.NOTE_RUNNING_STATUS)
            .put("status", newStatus);
    connectionManager.broadcast(noteId, message);
  }

  @Override
  //TODO(egorklimov): Replace Job with corresponding Paragraph class
  public void onProgressUpdate(final Job p, final int progress) {
    final SockMessage message = new SockMessage(Operation.PROGRESS)
            .put("id", p.getId())
            .put("progress", progress);
    //TODO(egorklimov): parent note has been removed from paragraph
    throw new NotImplementedException("Parent note has been removed from paragraph");
    //connectionManager.broadcast(p.getNote().getId(), message);
  }

  @Override
  public void onStatusChange(final Job p, final Job.Status before, final Job.Status after) {
    //TODO(egorklimov): result has been removed from paragraph:
    throw new NotImplementedException("Paragraph status should be loaded by not implemented "
        + "InterpreterResultService");
    //    if (after == Job.Status.ERROR) {
    //      if (p.getException() != null) {
    //        LOG.error("Error", p.getException());
    //      }
    //    }
    //
    //    if (p.isTerminated()) {
    //      if (p.getStatus() == Job.Status.FINISHED) {
    //        LOG.info("Note {}, job {} is finished successfully, status: {}",
    //                p.getNote().getId(), p.getId(), p.getStatus());
    //      } else {
    //        LOG.warn("Note {}. job {} is finished, status: {}, exception: {}, "
    //                        + "result\n@@@@@ Result start @@@@@\n{}\n@@@@@ Result end @@@@@",
    //                p.getNote().getId(), p.getId(), p.getStatus(), p.getException(), p.getReturn());
    //      }
    //
    //      try {
    //        zeppelinRepository.saveNote(p.getNote());
    //      } catch (final IOException e) {
    //        LOG.error(e.toString(), e);
    //      }
    //    }
    //
    //    p.setStatusToUserParagraph(p.getStatus());
    //    connectionManager.broadcast(p.getNote().getId(), new SockMessage(Operation.PARAGRAPH).put("paragraph", p));

    //    for (NoteEventListener listener : zeppelinRepository.getNoteEventListeners()) {
    //      listener.onParagraphStatusChange(p, after);
    //    }

    //TODO(KOT): FIX THIS
    //try {
    //  broadcastUpdateNoteJobInfo(System.currentTimeMillis() - 5000);
    //} catch (IOException e) {
    //  LOG.error("can not broadcast for job manager {}", e);
    //}
  }
}