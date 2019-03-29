package org.apache.zeppelin.interpreterV2.handler;

import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.JobBatch;
import org.apache.zeppelin.storage.JobBatchDAO;
import org.apache.zeppelin.storage.JobDAO;
import org.apache.zeppelin.storage.JobResultDAO;
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
                                  final JobResultDAO jobResultDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO);
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
