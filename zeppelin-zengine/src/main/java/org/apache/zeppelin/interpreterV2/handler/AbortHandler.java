package org.apache.zeppelin.interpreterV2.handler;

import java.util.List;
import java.util.Objects;
import org.apache.zeppelin.interpreter.core.PredefinedInterpreterResults;
import org.apache.zeppelin.interpreter.core.thrift.CancelResult;
import org.apache.zeppelin.interpreter.core.thrift.RemoteInterpreterService;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcess;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.JobBatch;
import org.apache.zeppelin.notebook.Note;
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
      ZLog.log(ET.INTERPRETER_PROCESS_NOT_FOUND,
          String.format("Interpreter process not found, shebang: %s", job.getShebang()),
          String.format("Incorrect interpreter behaviour during handling job abort, process for existing job not found: job[%s]", job.toString()),
          "Unknown");
      return;
    }

    CancelResult cancelResult = null;
    RemoteInterpreterService.Client connection = null;
    try {
      connection = remote.getConnection();
      cancelResult = remote.getConnection().cancel(job.getInterpreterJobUUID());
      Objects.requireNonNull(cancelResult);

    } catch (final Exception e) {
      setAbortResult(job, batch, PredefinedInterpreterResults.OPERATION_ABORTED);
      ZLog.log(ET.JOB_CANCEL_FAILED,
          String.format("Failed to cancel job with uuid: %s", job.getInterpreterJobUUID()),
          String.format("Exception thrown during job canceling: cancelResult[%s], job[%s]",
              cancelResult != null ? cancelResult.toString() : "null", job.toString()),
          "Unknown");
      return;
    } finally {
      if (connection != null) {
        remote.releaseConnection(connection);
      }
    }

    switch (cancelResult.status) {
      case ACCEPT:
        ZLog.log(ET.JOB_CANCEL_ACCEPTED,
            String.format("Interpreter process started to cancel: job[id=%s]", job.getId()),
            String.format("Cancel signal passed to interpreter process: job[%s], process[%s]", job.toString(), remote.toString()),
            "Unknown");
        job.setStatus(Job.Status.ABORTING);
        jobDAO.update(job);
        break;
      case NOT_FOUND:
        ZLog.log(ET.JOB_CANCEL_NOT_FOUND,
            String.format("Process to cancel not found : job[id=%s]", job.getId()),
            String.format("Cancel result status is \"not found\": job[%s], process[%s]", job.toString(), remote.toString()),
            "Unknown");
        setAbortResult(job, batch, PredefinedInterpreterResults.OPERATION_ABORTED);
        break;
      case ERROR:
      default:
        ZLog.log(ET.JOB_CANCEL_ERRORED,
            String.format("Failed to cancel job[id=%s]", job.getId()),
            String.format("Cancel result status is \"error\": job[%s], process[%s]", job.toString(), remote.toString()),
            "Unknown");
        setAbortResult(job, batch, PredefinedInterpreterResults.OPERATION_ABORTED);
    }
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void abort(final Note note) {
    abortingBatch(note);
  }
}
