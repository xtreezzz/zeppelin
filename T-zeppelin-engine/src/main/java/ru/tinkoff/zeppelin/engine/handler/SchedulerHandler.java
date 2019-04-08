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

import org.apache.zeppelin.storage.*;
import org.apache.zeppelin.storage.ZLog.ET;
import org.quartz.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.core.notebook.Scheduler;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

/**
 * Class for handle scheduled tasks
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
@Component
public class SchedulerHandler extends AbstractHandler {

  private final SchedulerDAO schedulerDAO;

  public SchedulerHandler(final JobBatchDAO jobBatchDAO,
                          final JobDAO jobDAO,
                          final JobResultDAO jobResultDAO,
                          final JobPayloadDAO jobPayloadDAO,
                          final NoteDAO noteDAO,
                          final ParagraphDAO paragraphDAO,
                          final FullParagraphDAO fullParagraphDAO,
                          final SchedulerDAO schedulerDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO);
    this.schedulerDAO = schedulerDAO;
  }

  public List<Scheduler> loadJobs() {
    return schedulerDAO.getReadyToExecute(LocalDateTime.now());
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final Scheduler scheduler) {
    ZLog.log(ET.JOB_READY_FOR_EXECUTION_BY_SCHEDULER,
        String.format("Note[id=%s] is ready for execution by scheduler, user[name=%s, roles=%s]",
            scheduler.getNoteId(), scheduler.getUser(), scheduler.getRoles()),
        String.format("Note[id=%s] is ready for execution by scheduler, user[name=%s, roles=%s], "
                + "execution parameters[last execution=%s, next execution=%s, expression=%s]",
            scheduler.getNoteId(), scheduler.getUser(), scheduler.getRoles(),
            scheduler.getLastExecution(), scheduler.getNextExecution(), scheduler.getExpression()),
        scheduler.getUser());
    final Note note = noteDAO.get(scheduler.getNoteId());
    final List<Paragraph> paragraphs = paragraphDAO.getByNoteId(note.getId());
    publishBatch(note, paragraphs);

    final CronExpression cronExpression;
    try {
      cronExpression = new CronExpression(scheduler.getExpression());
    } catch (final Exception e) {
      ZLog.log(ET.SCHEDULED_JOB_ERRORED,
          String.format("Scheduled job[noteId = %s] has wrong expression = %s",
              scheduler.getNoteId(), scheduler.getExpression()),
          String.format("Scheduled job[%s] has wrong expression", scheduler.toString()),
          scheduler.getUser());
      throw new IllegalArgumentException("Wrong cron expression");
    }

    final ZonedDateTime zdt = scheduler.getNextExecution().atZone(ZoneId.systemDefault());
    final Date currentExecution = Date.from(zdt.toInstant());
    final Date nextExecutionDate = cronExpression.getNextValidTimeAfter(currentExecution);
    final LocalDateTime nextExecution = LocalDateTime.ofInstant(nextExecutionDate.toInstant(), ZoneId.systemDefault());

    scheduler.setLastExecution(scheduler.getNextExecution());
    scheduler.setNextExecution(nextExecution);
    schedulerDAO.update(scheduler);
    ZLog.log(ET.JOB_SCHEDULED,
        String.format("Job successfully scheduled, noteId=%s, next execution=%s", scheduler.getNoteId(), nextExecution),
        String.format("Job successfully scheduled, job=%s, next execution=%s", scheduler.toString(), nextExecution),
        scheduler.getUser());
  }

}
