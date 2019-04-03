package org.apache.zeppelin.interpreterV2.handler;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.Scheduler;
import org.apache.zeppelin.storage.FullParagraphDAO;
import org.apache.zeppelin.storage.JobBatchDAO;
import org.apache.zeppelin.storage.JobDAO;
import org.apache.zeppelin.storage.JobPayloadDAO;
import org.apache.zeppelin.storage.JobResultDAO;
import org.apache.zeppelin.storage.NoteDAO;
import org.apache.zeppelin.storage.ParagraphDAO;
import org.apache.zeppelin.storage.SchedulerDAO;
import org.apache.zeppelin.storage.ZLog;
import org.apache.zeppelin.storage.ZLog.ET;
import org.quartz.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

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
