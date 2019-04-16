/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ru.tinkoff.zeppelin.engine.handler;

import com.google.common.collect.Sets;
import org.apache.zeppelin.storage.*;
import org.apache.zeppelin.storage.ZLog.ET;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.engine.server.InterpreterProcess;
import ru.tinkoff.zeppelin.interpreter.PredefinedInterpreterResults;
import ru.tinkoff.zeppelin.interpreter.thrift.CancelResult;
import ru.tinkoff.zeppelin.interpreter.thrift.RemoteInterpreterThriftService;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Class for handle abort state of jobs
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
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

  public List<JobBatch> loadJobs() {
    return jobBatchDAO.getAborting();
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void handle(final JobBatch batch) {

    final List<Job> jobs = jobDAO.loadByBatch(batch.getId());
    final Set<Job.Status> runningStatuses = Sets.newHashSet(Job.Status.RUNNING, Job.Status.ABORTING);
    final boolean hasRunningJob = jobs.stream().anyMatch(j -> runningStatuses.contains(j.getStatus()));
    if(! hasRunningJob) {
      setFailedResult(null, null, batch, JobBatch.Status.ABORTED, PredefinedInterpreterResults.OPERATION_ABORTED);
      return;
    }

    jobs.stream()
            .filter(j -> runningStatuses.contains(j.getStatus()))
            .forEach(j -> abortRunningJob(batch, j));

  }

  private void abortRunningJob(final JobBatch batch, final Job job) {
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
    RemoteInterpreterThriftService.Client connection = null;
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
        setFailedResult(job, Job.Status.ABORTING, null, null, null);
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
