package org.apache.zeppelin.interpreterV2.handler;

import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.apache.zeppelin.interpreter.core.PredefinedInterpreterResults;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.JobBatch;
import org.apache.zeppelin.storage.*;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;

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

    final Job job = jobDAO.getByInterpreterJobUUID(interpreterJobUUID);
    if (job == null) {
      return;
    }

    final JobBatch batch = jobBatchDAO.get(job.getBatchId());

    if (batch.getStatus() == JobBatch.Status.ABORTING) {
      setAbortResult(job, batch, interpreterResult);
      return;
    }

    if(interpreterResult == null) {
      setErrorResult(job, batch, PredefinedInterpreterResults.ERROR_WHILE_INTERPRET);
      return;
    }

    switch (interpreterResult.code()) {
      case SUCCESS:
        setSuccessResult(job, batch, interpreterResult);
        break;
      case ABORTED:
        setAbortResult(job, batch, interpreterResult);
        break;
      case ERROR:
        setErrorResult(job, batch, interpreterResult);
        break;
    }
  }
}
