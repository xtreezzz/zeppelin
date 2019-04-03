package org.apache.zeppelin.interpreterV2.handler;

import javax.annotation.PostConstruct;
import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.apache.zeppelin.interpreter.core.PredefinedInterpreterResults;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.JobBatch;
import org.apache.zeppelin.storage.FullParagraphDAO;
import org.apache.zeppelin.storage.JobBatchDAO;
import org.apache.zeppelin.storage.JobDAO;
import org.apache.zeppelin.storage.JobPayloadDAO;
import org.apache.zeppelin.storage.JobResultDAO;
import org.apache.zeppelin.storage.NoteDAO;
import org.apache.zeppelin.storage.ParagraphDAO;
import org.apache.zeppelin.storage.ZLog;
import org.apache.zeppelin.storage.ZLog.ET;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component
public class InterpreterResultHandler extends AbstractHandler {

  private static InterpreterResultHandler instance;

  public static InterpreterResultHandler getInstance() {
    return instance;
  }

  public InterpreterResultHandler(final JobBatchDAO jobBatchDAO,
                                  final JobDAO jobDAO,
                                  final JobResultDAO jobResultDAO,
                                  final JobPayloadDAO jobPayloadDAO,
                                  final NoteDAO noteDAO,
                                  final ParagraphDAO paragraphDAO,
                                  final FullParagraphDAO fullParagraphDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO);
  }

  @PostConstruct
  public void postConstruct() {
    instance = this;
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final String interpreterJobUUID,
                     final InterpreterResult interpreterResult) {

    Job job = null;
    // задержка на закрытие транзакций
    for (int i = 0; i < 2 * 10 * 60 ; i++ ) {
      job = jobDAO.getByInterpreterJobUUID(interpreterJobUUID);
      if (job != null) {
        break;
      }
      try {
        Thread.sleep(100);
      } catch (final Exception e) {
        // SKIp
      }
    }

    if (job == null) {
      ZLog.log(ET.JOB_NOT_FOUND, "Job not found by uuid=" + interpreterJobUUID,
          "Job not found by uuid=" + interpreterJobUUID,"Unknown");
      return;
    }

    final JobBatch batch = jobBatchDAO.get(job.getBatchId());
    ZLog.log(ET.GOT_JOB,
        String.format("Got result [batch status=%s, batch id=%s] for job: id=%s, noteId=%s, paragraphId=%s",
            batch.getStatus(), batch.getId(), job.getId(), job.getNoteId(), job.getParagpaphId()),
        String.format("Got result [batch status=%s, batch id=%s] for job: id=%s, noteId=%s, paragraphId=%s",
            batch.getStatus(), batch.getId(), job.getId(), job.getNoteId(), job.getParagpaphId()),
        "Unknown");

    if (batch.getStatus() == JobBatch.Status.ABORTING || batch.getStatus() == JobBatch.Status.ABORTED) {
      ZLog.log(ET.GOT_ABORTED_JOB,
          String.format("Batch status is %s", batch.getStatus()),
          String.format("Incorrect interpreter behaviour during handling job abort, process for existing job not found: job[%s]", job.toString()),
          "Unknown");
      setAbortResult(job, batch, interpreterResult);
      return;
    }

    if(interpreterResult == null) {
      ZLog.log(ET.INTERPRETER_RESULT_NOT_FOUND,
          String.format("Handler got null result, interpreterJobUUID=%s", interpreterJobUUID),
          String.format("Handler got null result, interpreterJobUUID=%s", interpreterJobUUID),
          "Unknown");

      setErrorResult(job, batch, PredefinedInterpreterResults.ERROR_WHILE_INTERPRET);
      return;
    }

    switch (interpreterResult.code()) {
      case SUCCESS:
        ZLog.log(ET.SUCCESSFUL_RESULT,
            String.format("Got successful result for job with id=%s", interpreterJobUUID),
            String.format("Got successful result for job with id=%s", interpreterJobUUID),
            "Unknown");

        setSuccessResult(job, batch, interpreterResult);
        break;
      case ABORTED:
        ZLog.log(ET.ABORTED_RESULT,
            String.format("Got aborted result for job with id=%s", interpreterJobUUID),
            String.format("Got aborted result for job with id=%s", interpreterJobUUID),
            "Unknown");
        setAbortResult(job, batch, interpreterResult);
        break;
      case ERROR:
        ZLog.log(ET.ERRORED_RESULT,
            String.format("Got errored result for job with id=%s", interpreterJobUUID),
            String.format("Got errored result for job with id=%s", interpreterJobUUID),
            "Unknown");
        setErrorResult(job, batch, interpreterResult);
        break;
    }
  }
}
