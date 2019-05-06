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
package ru.tinkoff.zeppelin.engine.handler;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.storage.*;
import ru.tinkoff.zeppelin.storage.ZLog.ET;

import java.util.List;
import java.util.Set;

/**
 * Class for handle ready for execute jobs
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class ExecutionHandler extends AbstractHandler{

  public ExecutionHandler(final JobBatchDAO jobBatchDAO,
                          final JobDAO jobDAO,
                          final JobResultDAO jobResultDAO,
                          final JobPayloadDAO jobPayloadDAO,
                          final NoteDAO noteDAO,
                          final ParagraphDAO paragraphDAO,
                          final FullParagraphDAO fullParagraphDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO);
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void run(final Note note, final List<Paragraph> paragraphs, final String username, final Set<String> roles) {
    if (noteIsRunning(note)) {
      ZLog.log(ET.JOB_CANCEL_ALREADY_RUNNING,
          String.format("Note[id=%s] is already running! Abort running note", note.getId()),
          String.format(
              "Batch for note[id=%s] with paragraphs[%s] will be rejected when user [name=%s;roles=%s] try execute it",
              note.getId(), paragraphs.toString(), username, roles.toString()
          ),
          username);
      return;
    }

    ZLog.log(ET.JOB_SUBMITTED_FOR_EXECUTION,
        String.format("Note[id=%s] with paragraphs[%s] submitted for execution by user[name=%s;roles=%s]",
            note.getId(), paragraphs.toString(), username, roles.toString()),
        String.format("Batch for note[id=%s] with paragraphs[%s] published by user[name=%s;roles=%s]",
            note.getId(), paragraphs.toString(), username, roles.toString()),
        username);
    publishBatch(note, paragraphs, username, roles, 0);
  }
}
