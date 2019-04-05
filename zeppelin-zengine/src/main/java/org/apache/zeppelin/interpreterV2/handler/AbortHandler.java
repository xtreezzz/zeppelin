package org.apache.zeppelin.interpreterV2.handler;

import org.apache.zeppelin.interpreter.core.PredefinedInterpreterResults;
import org.apache.zeppelin.interpreter.core.thrift.CancelResult;
import org.apache.zeppelin.interpreter.core.thrift.RemoteInterpreterService;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcess;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.JobBatch;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.storage.*;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;

@Component
public class AbortHandler extends AbstractHandler {

  public AbortHandler(final JobBatchDAO jobBatchDAO,
                      final JobDAO jobDAO,
                      final JobResultDAO jobResultDAO,
                      final JobPayloadDAO jobPayloadDAO,
                      final NoteDAO noteDAO,
                      final ParagraphDAO paragraphDAO,
                      final FullParagraphDAO fullParagraphDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO);
  }

  public List<Job> loadJobs() {
    return jobDAO.loadNextCancelling();
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final Job job) {
    final JobBatch batch = jobBatchDAO.get(job.getBatchId());

    final InterpreterProcess remote = InterpreterProcess.get(job.getShebang());
    if (remote == null) {
      setAbortResult(job, batch, PredefinedInterpreterResults.OPERATION_ABORTED);
      return;
    }

    final CancelResult cancelResult;
    RemoteInterpreterService.Client connection = null;
    try {
      connection = remote.getConnection();
      cancelResult = remote.getConnection().cancel(job.getInterpreterJobUUID());
      Objects.requireNonNull(cancelResult);

    } catch (final Exception e) {
      setAbortResult(job, batch, PredefinedInterpreterResults.OPERATION_ABORTED);
      return;

    } finally {
      if (connection != null) {
        remote.releaseConnection(connection);
      }
    }

    switch (cancelResult.status) {
      case ACCEPT:
        job.setStatus(Job.Status.ABORTING);
        jobDAO.update(job);
        break;
      case NOT_FOUND:
        setAbortResult(job, batch, PredefinedInterpreterResults.OPERATION_ABORTED);
        break;
      case ERROR:
      default:
        setAbortResult(job, batch, PredefinedInterpreterResults.OPERATION_ABORTED);
    }
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void abort(final Note note) {
    abortingBatch(note);
  }
}
