package org.apache.zeppelin.interpreterV2.handler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.apache.zeppelin.interpreter.core.thrift.PushResult;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcess;
import org.apache.zeppelin.notebook.Job;
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
public class PendingHandler extends AbstractHandler {


  public PendingHandler(final JobBatchDAO jobBatchDAO,
                        final JobDAO jobDAO,
                        final JobResultDAO jobResultDAO,
                        final JobPayloadDAO jobPayloadDAO,
                        final NoteDAO noteDAO,
                        final ParagraphDAO paragraphDAO,
                        final FullParagraphDAO fullParagraphDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO, paragraphDAO, fullParagraphDAO);
  }

  public List<Job> loadJobs() {
    return jobDAO.loadNextPending();
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final Job job, final InterpreterProcess process, final InterpreterOption option) {

    // prepare payload
    final String payload = jobPayloadDAO.getByJobId(job.getId()).getPayload();

    // prepare notecontext
    final Map<String, String> noteContext = new HashMap<>();
    noteContext.put("noteId", String.valueOf(job.getNoteId()));
    noteContext.put("paragraphId", String.valueOf(job.getParagpaphId()));

    // prepare usercontext
    final Map<String, String> userContext = new HashMap<>();

    // prepare configuration
    final Map<String, String> configuration = new HashMap<>();
    option.getConfig()
            .getProperties()
            .forEach((p, v) -> configuration.put(p, String.valueOf(v.getCurrentValue())));

    ZLog.log(ET.JOB_READY_FOR_EXECUTION, "Job ready for execution, id=" + job.getId(),
        String.format("Job ready for execution, job[%s]", job.toString()), "Unknown");
    final PushResult result = process.push(payload, noteContext, userContext, configuration);
    if(result == null) {
      ZLog.log(ET.JOB_REQUEST_IS_EMPTY, "Push result is empty for job with id=" + job.getId(),
          String.format("Push result is empty, job[%s]", job.toString()), "Unknown");
      return;
    }

    switch (result.getStatus()) {
      case ACCEPT:
        ZLog.log(ET.JOB_ACCEPTED, String.format("Job accepted, id=%s", job.getId()),
            String.format("Job accepted, job[%s]", job.toString()), "Unknown");
        setRunningState(job, result.getInterpreterProcessUUID(), result.getInterpreterJobUUID());
        break;
      case DECLINE:
        ZLog.log(ET.JOB_DECLINED, String.format("Job declined, id=%s", job.getId()),
            String.format("Job declined, job[%s]", job.toString()), "Unknown");
        break;
      case ERROR:
        ZLog.log(ET.JOB_REQUEST_ERRORED, String.format("Job errored, id=%s", job.getId()),
            String.format("Job errored, job[%s]", job.toString()), "Unknown");
        break;
      default:
        ZLog.log(ET.JOB_UNDEFINED,
            String.format("System error, job request status undefined, id=%s, status=%s", job.getId(), result.getStatus()),
            String.format("System error, job request status undefined, job[%s], status=%s", job.toString(), result.getStatus()),
            "Unknown");
    }
  }
}
