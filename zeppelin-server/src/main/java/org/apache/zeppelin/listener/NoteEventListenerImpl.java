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

import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.NoteEventListener;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.service.JobManagerService;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class NoteEventListenerImpl implements NoteEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(NoteEventListenerImpl.class);

  private final JobManagerService jobManagerService;
  private final ConnectionManager connectionManager;

  @Autowired
  public NoteEventListenerImpl(final JobManagerService jobManagerService,
                               final ConnectionManager connectionManager) {
    this.jobManagerService = jobManagerService;
    this.connectionManager = connectionManager;
  }

  @Override
  public void onParagraphRemove(final Paragraph p) {
    final List<JobManagerService.NoteJobInfo> notesJobInfo
            = jobManagerService.getNoteJobInfoByUnixTime(System.currentTimeMillis() - 5000);
    broadcastSuccessResponse(notesJobInfo);
  }

  @Override
  public void onNoteRemove(final Note note) {
    List<JobManagerService.NoteJobInfo> notesJobInfo;

    notesJobInfo = jobManagerService.getNoteJobInfoByUnixTime(System.currentTimeMillis() - 5000);
    broadcastSuccessResponse(notesJobInfo);

    notesJobInfo = jobManagerService.removeNoteJobInfo(note.getId());
    broadcastSuccessResponse(notesJobInfo);
  }

  @Override
  public void onParagraphCreate(final Paragraph p) {
    final List<JobManagerService.NoteJobInfo> notesJobInfo = jobManagerService.getNoteJobInfo(p.getNote().getId());
    broadcastSuccessResponse(notesJobInfo);
  }

  @Override
  public void onParagraphUpdate(final Paragraph p) {

  }

  @Override
  public void onNoteCreate(final Note note) {
    final List<JobManagerService.NoteJobInfo> notesJobInfo = jobManagerService.getNoteJobInfo(note.getId());
    broadcastSuccessResponse(notesJobInfo);
  }

  @Override
  public void onNoteUpdate(final Note note) {

  }

  @Override
  public void onParagraphStatusChange(final Paragraph p, final Job.Status status) {
    final List<JobManagerService.NoteJobInfo> notesJobInfo = jobManagerService.getNoteJobInfo(p.getNote().getId());
    broadcastSuccessResponse(notesJobInfo);
  }

  private void broadcastSuccessResponse(final List<JobManagerService.NoteJobInfo> notesJobInfo) {
    final Map<String, Object> response = new HashMap<>();
    response.put("lastResponseUnixTime", System.currentTimeMillis());
    response.put("jobs", notesJobInfo);
    //TODO(KOT): FIX THIS
    //connectionManager.broadcast(NotebookServer.JobManagerServiceType.JOB_MANAGER_PAGE.getKey(),
    //        new Message(Message.OP.LIST_UPDATE_NOTE_JOBS).put("noteRunningJobs", response));
  }
}
