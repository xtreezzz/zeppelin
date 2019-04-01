package org.apache.zeppelin.interpreterV2.handler;

import org.apache.zeppelin.interpreter.configuration.InterpreterArtifactSource;
import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.apache.zeppelin.interpreter.core.PredefinedInterpreterResults;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcess;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcessStarter;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.JobBatch;
import org.apache.zeppelin.storage.*;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;


@Component
public class InterpreterStarterHandler extends AbstractHandler {

  public InterpreterStarterHandler(final JobBatchDAO jobBatchDAO,
                                   final JobDAO jobDAO,
                                   final JobResultDAO jobResultDAO,
                                   final JobPayloadDAO jobPayloadDAO,
                                   final NotebookDAO notebookDAO) {
    super(jobBatchDAO, jobDAO, jobResultDAO, jobPayloadDAO, notebookDAO);
  }


  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final Job job,
                     final InterpreterOption option,
                     final InterpreterArtifactSource source,
                     final String remoteServerClassPath,
                     final String thriftAddr,
                     final int thriftPort) {

    final JobBatch batch = jobBatchDAO.get(job.getBatchId());
    if (option == null) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_NOT_FOUND);
      return;
    }

    if (!option.isEnabled()) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_DISABLED);
    }

    if (source == null) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_NOT_FOUND);
      return;
    }

    if (source.getStatus() != InterpreterArtifactSource.Status.INSTALLED) {
      setErrorResult(job, batch, PredefinedInterpreterResults.INTERPRETER_NOT_FOUND);
      return;
    }
    InterpreterProcess.starting(job.getShebang());

    InterpreterProcessStarter.start(job.getShebang(),
            source.getPath(),
            option.getConfig().getClassName(),
            remoteServerClassPath,
            thriftAddr,
            thriftPort);
  }

}
