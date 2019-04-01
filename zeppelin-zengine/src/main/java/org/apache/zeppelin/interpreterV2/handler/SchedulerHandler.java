package org.apache.zeppelin.interpreterV2.handler;

import org.apache.zeppelin.notebook.*;
import org.apache.zeppelin.storage.*;
import org.quartz.CronExpression;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;

@Component
public class SchedulerHandler extends AbstractHandler {

  private final SchedulerDAO schedulerDAO;

  public SchedulerHandler(final JobBatchDAO jobBatchDAO,
                          final JobDAO jobDAO,
                          final JobResultDAO jobResultDAO,
                          final JobPayloadDAO jobPayloadDAO,
                          final NotebookDAO notebookDAO,
                          final SchedulerDAO schedulerDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, notebookDAO);
    this.schedulerDAO = schedulerDAO;
  }

  public List<Scheduler> loadJobs() {
    return schedulerDAO.getReadyToExecute(LocalDateTime.now());
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final Scheduler scheduler) {
    final Note note = notebookDAO.getNoteById(scheduler.getNoteId());
    publishBatch(note, note.getParagraphs());

    final CronExpression cronExpression;
    try {
      cronExpression = new CronExpression(scheduler.getExpression());
    } catch (final Exception e) {
      throw new IllegalArgumentException("Wrong cron expression");
    }

    ZonedDateTime zdt = scheduler.getNextExecution().atZone(ZoneId.systemDefault());
    final Date currentExecution = Date.from(zdt.toInstant());
    final Date nextExecutionDate = cronExpression.getNextValidTimeAfter(currentExecution);
    final LocalDateTime nextExecution = LocalDateTime.ofInstant(nextExecutionDate.toInstant(), ZoneId.systemDefault());

    scheduler.setLastExecution(scheduler.getNextExecution());
    scheduler.setNextExecution(nextExecution);
    schedulerDAO.update(scheduler);
  }

}
