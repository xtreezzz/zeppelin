package org.apache.zeppelin.interpreterV2.handler;

import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.apache.zeppelin.interpreter.core.thrift.PushResult;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcess;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.storage.*;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class PendingHandler extends AbstractHandler {


  public PendingHandler(final JobBatchDAO jobBatchDAO,
                        final JobDAO jobDAO,
                        final JobResultDAO jobResultDAO,
                        final JobPayloadDAO jobPayloadDAO,
                        final NoteDAO noteDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, noteDAO);
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

    final PushResult result = process.push(payload, noteContext, userContext, configuration);
    if(result == null) {
      return;
    }

    switch (result.getStatus()) {
      case ACCEPT:
        setRunningState(job, result.getInterpreterProcessUUID(), result.getInterpreterJobUUID());
        break;
      case DECLINE:
      case ERROR:
      default:
        // log this
    }
  }
}
