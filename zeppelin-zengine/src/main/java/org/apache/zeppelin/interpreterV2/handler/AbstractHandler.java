package org.apache.zeppelin.interpreterV2.handler;

import org.apache.zeppelin.EventService;
import org.apache.zeppelin.externalDTO.ParagraphDTO;
import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.apache.zeppelin.notebook.*;
import org.apache.zeppelin.storage.*;

import java.time.LocalDateTime;
import java.util.List;

abstract class AbstractHandler {

  final JobBatchDAO jobBatchDAO;
  final JobDAO jobDAO;
  private final JobResultDAO jobResultDAO;
  final JobPayloadDAO jobPayloadDAO;
  final NoteDAO noteDAO;
  final ParagraphDAO paragraphDAO;
  private final FullParagraphDAO fullParagraphDAO;

  public AbstractHandler(final JobBatchDAO jobBatchDAO,
                         final JobDAO jobDAO,
                         final JobResultDAO jobResultDAO,
                         final JobPayloadDAO jobPayloadDAO,
                         final NoteDAO noteDAO,
                         final ParagraphDAO paragraphDAO,
                         final FullParagraphDAO fullParagraphDAO) {
    this.jobBatchDAO = jobBatchDAO;
    this.jobDAO = jobDAO;
    this.jobResultDAO = jobResultDAO;
    this.jobPayloadDAO = jobPayloadDAO;
    this.noteDAO = noteDAO;
    this.paragraphDAO = paragraphDAO;
    this.fullParagraphDAO = fullParagraphDAO;
  }

  void setRunningState(final Job job,
                        final String interpreterProcessUUID,
                        final String interpreterJobUUID) {

    final ParagraphDTO before = fullParagraphDAO.getById(job.getParagpaphId());

    job.setStatus(Job.Status.RUNNING);
    job.setInterpreterProcessUUID(interpreterProcessUUID);
    job.setInterpreterJobUUID(interpreterJobUUID);
    jobDAO.update(job);

    final ParagraphDTO after = fullParagraphDAO.getById(job.getParagpaphId());
    EventService.publish(job.getNoteId(), before, after);

    final JobBatch jobBatch = jobBatchDAO.get(job.getBatchId());
    if (jobBatch.getStatus() == JobBatch.Status.PENDING) {
      jobBatch.setStatus(JobBatch.Status.RUNNING);
      jobBatch.setStartedAt(LocalDateTime.now());
      jobBatchDAO.update(jobBatch);
    }
  }

  void setSuccessResult(final Job job,
                                final JobBatch batch,
                                final InterpreterResult interpreterResult) {

    final ParagraphDTO before = fullParagraphDAO.getById(job.getParagpaphId());

    persistMessages(job, interpreterResult.message());

    job.setStatus(Job.Status.DONE);
    job.setEndedAt(LocalDateTime.now());
    job.setInterpreterJobUUID(null);
    job.setInterpreterProcessUUID(null);
    jobDAO.update(job);

    final ParagraphDTO after = fullParagraphDAO.getById(job.getParagpaphId());
    EventService.publish(job.getNoteId(), before, after);

    final List<Job> jobs = jobDAO.loadByBatch(job.getBatchId());
    final boolean isDone = jobs.stream().noneMatch(j -> j.getStatus() != Job.Status.DONE);
    if (isDone) {
      batch.setStatus(JobBatch.Status.DONE);
      batch.setEndedAt(LocalDateTime.now());
      jobBatchDAO.update(batch);
    }
  }


  void setErrorResult(final Job job,
                              final JobBatch batch,
                              final InterpreterResult interpreterResult) {

    setFailedResult(job, Job.Status.ERROR, batch, JobBatch.Status.ERROR, interpreterResult);
  }

  void setAbortResult(final Job job,
                              final JobBatch batch,
                              final InterpreterResult interpreterResult) {

    setFailedResult(job, Job.Status.ABORTED, batch, JobBatch.Status.ABORTED, interpreterResult);
  }

  private void setFailedResult(final Job job,
                               final Job.Status jobStatus,
                               final JobBatch batch,
                               final JobBatch.Status jobBatchStatus,
                               final InterpreterResult interpreterResult) {

    final ParagraphDTO before = fullParagraphDAO.getById(job.getParagpaphId());

    persistMessages(job, interpreterResult.message());

    job.setStatus(jobStatus);
    job.setEndedAt(LocalDateTime.now());
    jobDAO.update(job);

    final ParagraphDTO after = fullParagraphDAO.getById(job.getParagpaphId());
    EventService.publish(job. getNoteId(), before, after);

    final List<Job> jobs = jobDAO.loadByBatch(job.getBatchId());
    for (final Job j : jobs) {
      final ParagraphDTO beforeInner = fullParagraphDAO.getById(j.getParagpaphId());

      if (j.getStatus() == Job.Status.PENDING) {
        j.setStatus(Job.Status.CANCELED);
        j.setStartedAt(LocalDateTime.now());
        j.setEndedAt(LocalDateTime.now());
      }
      j.setInterpreterJobUUID(null);
      j.setInterpreterProcessUUID(null);
      jobDAO.update(j);

      final ParagraphDTO afterInner = fullParagraphDAO.getById(j.getParagpaphId());
      EventService.publish(job.getNoteId(), beforeInner, afterInner);
    }

    batch.setStatus(jobBatchStatus);
    batch.setEndedAt(LocalDateTime.now());
    jobBatchDAO.update(batch);
  }

  private void persistMessages(final Job job,
                               final List<InterpreterResult.Message> messages) {

    for (final InterpreterResult.Message message : messages) {
      final JobResult jobResult = new JobResult();
      jobResult.setJobId(job.getId());
      jobResult.setCreatedAt(LocalDateTime.now());
      jobResult.setType(message.getType().name());
      jobResult.setResult(message.getData());
      jobResultDAO.persist(jobResult);
    }
  }

  void publishBatch(final Note note, final List<Paragraph> paragraphs) {
    final JobBatch batch = new JobBatch();
    batch.setId(0L);
    batch.setNoteId(note.getId());
    batch.setStatus(JobBatch.Status.SAVING);
    batch.setCreatedAt(LocalDateTime.now());
    batch.setStartedAt(null);
    batch.setEndedAt(null);
    final JobBatch saved = jobBatchDAO.persist(batch);

    for (int i = 0; i < paragraphs.size(); i++) {
      final Paragraph p = paragraphs.get(i);

      final ParagraphDTO before = fullParagraphDAO.getById(p.getId());

      final Job job = new Job();
      job.setId(0L);
      job.setBatchId(saved.getId());
      job.setNoteId(note.getId());
      job.setParagpaphId(p.getId());
      job.setIndex(i);
      job.setShebang(p.getShebang());
      job.setStatus(Job.Status.PENDING);
      job.setInterpreterProcessUUID(null);
      job.setInterpreterJobUUID(null);
      job.setCreatedAt(LocalDateTime.now());
      job.setStartedAt(null);
      job.setEndedAt(null);
      jobDAO.persist(job);

      final JobPayload jobPayload = new JobPayload();
      jobPayload.setId(0L);
      jobPayload.setJobId(job.getId());
      jobPayload.setPayload(p.getText());
      jobPayloadDAO.persist(jobPayload);

      p.setJobId(job.getId());
      paragraphDAO.update(p);

      final ParagraphDTO after = fullParagraphDAO.getById(job.getParagpaphId());
      EventService.publish(job.getNoteId(), before, after);
    }
    saved.setStatus(JobBatch.Status.PENDING);
    jobBatchDAO.update(saved);
    note.setBatchJobId(batch.getId());
    noteDAO.update(note);
  }

  void abortingBatch(final Note note) {
    final JobBatch jobBatch = jobBatchDAO.get(note.getBatchJobId());
    jobBatch.setStatus(JobBatch.Status.ABORTING);
    jobBatchDAO.update(jobBatch);
  }
}
