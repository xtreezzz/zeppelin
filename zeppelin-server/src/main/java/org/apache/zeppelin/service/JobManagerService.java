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

package org.apache.zeppelin.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.repositories.ZeppelinNoteRepository;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Service class for JobManager Page
 */
@Component
public class JobManagerService {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobManagerService.class);

  private final ZeppelinNoteRepository zeppelinNoteRepository;

  @Autowired
  public JobManagerService(final ZeppelinNoteRepository zeppelinNoteRepository) {
    this.zeppelinNoteRepository = zeppelinNoteRepository;
  }

  public List<NoteJobInfo> getNoteJobInfo(final String noteId) {
    final List<NoteJobInfo> notesJobInfo = new ArrayList<>();
    final Note jobNote = zeppelinNoteRepository.getNote(noteId);
    notesJobInfo.add(new NoteJobInfo(jobNote));
    return notesJobInfo;
  }

  /**
   * Get all NoteJobInfo after lastUpdateServerUnixTime
   */
  public List<NoteJobInfo> getNoteJobInfoByUnixTime(final long lastUpdateServerUnixTime) {
    final List<Note> notes = zeppelinNoteRepository.getAllNotes();
    final List<NoteJobInfo> notesJobInfo = new ArrayList<>();
    for (final Note note : notes) {
      final NoteJobInfo noteJobInfo = new NoteJobInfo(note);
      if (noteJobInfo.unixTimeLastRun > lastUpdateServerUnixTime) {
        notesJobInfo.add(noteJobInfo);
      }
    }
    return notesJobInfo;
  }

  public List<NoteJobInfo> removeNoteJobInfo(final String noteId) {
    final List<NoteJobInfo> notesJobInfo = new ArrayList<>();
    notesJobInfo.add(new NoteJobInfo(noteId, true));
    return notesJobInfo;
  }

  private static long getUnixTimeLastRunParagraph(final Paragraph paragraph) {
    /*if (paragraph.isTerminated() && paragraph.getDateFinished() != null) {
      return paragraph.getDateFinished().getTime();
    } else if (paragraph.isRunning()) {
      return new Date().getTime();
    } else {
      return paragraph.getDateCreated().getTime();
    } */
    return new Date().getTime();
  }


  public static class ParagraphJobInfo {
    private final String id;
    private final String name;
    //private final Job.Status status;

    public ParagraphJobInfo(final Paragraph p) {
      this.id = p.getId();
      if (StringUtils.isBlank(p.getTitle())) {
        this.name = p.getId();
      } else {
        this.name = p.getTitle();
      }
      //this.status = p.getStatus();
    }
  }

  public static class NoteJobInfo {
    private final String noteId;
    private String noteName;
    private String noteType;
    private String interpreter;
    private boolean isRunningJob;
    private boolean isRemoved = false;
    private long unixTimeLastRun;
    private List<ParagraphJobInfo> paragraphs;

    public NoteJobInfo(final Note note) {
      boolean isNoteRunning = false;
      long lastRunningUnixTime = 0;
      this.noteId = note.getId();
      this.noteName = note.getName();
      // set note type ( cron or normal )
      if (isCron(note)) {
        this.noteType = "cron";
      } else {
        this.noteType = "normal";
      }
      this.interpreter = note.getDefaultInterpreterGroup();

      // set paragraphs
      this.paragraphs = new ArrayList<>();
      for (final Paragraph paragraph : note.getParagraphs()) {
        // check paragraph's status.
        //if (paragraph.getStatus().isRunning()) {
        //  isNoteRunning = true;
        //}
        // get data for the job manager.
        final ParagraphJobInfo paragraphItem = new ParagraphJobInfo(paragraph);
        lastRunningUnixTime = getUnixTimeLastRunParagraph(paragraph);
        paragraphs.add(paragraphItem);
      }

      this.isRunningJob = isNoteRunning;
      this.unixTimeLastRun = lastRunningUnixTime;
    }

    private boolean isCron(final Note note) {
      return false; //note.getNoteCronConfiguration().isCronEnabled;
    }

    public NoteJobInfo(final String noteId, final boolean isRemoved) {
      this.noteId = noteId;
      this.isRemoved = isRemoved;
    }
  }
}
