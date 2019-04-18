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

package ru.tinkoff.zeppelin.engine;

import org.apache.zeppelin.storage.InterpreterOptionRepository;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterArtifactSource;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.core.notebook.*;
import ru.tinkoff.zeppelin.engine.handler.*;
import ru.tinkoff.zeppelin.engine.server.InterpreterProcess;
import ru.tinkoff.zeppelin.engine.server.InterpreterProcessServer;
import ru.tinkoff.zeppelin.interpreter.thrift.PingResult;
import ru.tinkoff.zeppelin.interpreter.thrift.PingResultStatus;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Core class of logic
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */

@Lazy(false)
@DependsOn("configuration")
@Component
public class NoteExecutorService {

  private InterpreterProcessServer server;

  private final AbortHandler abortHandler;
  private final PendingHandler pendingHandler;
  private final InterpreterDeadHandler interpreterDeadHandler;
  private final InterpreterOptionRepository interpreterOptionRepository;
  private final InterpreterStarterHandler interpreterStarterHandler;
  private final SchedulerHandler schedulerHandler;
  private final ExecutionHandler executionHandler;

  public NoteExecutorService(final AbortHandler abortHandler,
                             final PendingHandler pendingHandler,
                             final InterpreterDeadHandler interpreterDeadHandler,
                             final InterpreterOptionRepository interpreterOptionRepository,
                             final InterpreterStarterHandler interpreterStarterHandler,
                             final SchedulerHandler schedulerHandler,
                             final ExecutionHandler executionHandler) {
    this.abortHandler = abortHandler;
    this.pendingHandler = pendingHandler;
    this.interpreterDeadHandler = interpreterDeadHandler;
    this.interpreterOptionRepository = interpreterOptionRepository;
    this.interpreterStarterHandler = interpreterStarterHandler;
    this.schedulerHandler = schedulerHandler;
    this.executionHandler = executionHandler;
  }

  @PostConstruct
  public void init() throws Exception{
    server = new InterpreterProcessServer();
    server.initSources(interpreterOptionRepository.getAllRepositories());
    server.start();
  }

  @PreDestroy
  private void destroy() {
    server.stop();
  }


  @Scheduled(fixedDelay = 1_000)
  private void handlePending() {
    final List<Job> jobs = pendingHandler.loadJobs();
    for (final Job job : jobs) {
      try {
        final InterpreterOption option = interpreterOptionRepository.getOption(job.getShebang());
        final InterpreterProcess process = InterpreterProcess.get(job.getShebang());
        if (process != null
                && process.getStatus() == InterpreterProcess.Status.READY
                && option != null) {
          pendingHandler.handle(job, process, option);
        } else if (process != null
                && process.getStatus() == InterpreterProcess.Status.STARTING
                && option != null) {
          final String str = ";";
        } else {
          final InterpreterArtifactSource source = option != null
                  ? interpreterOptionRepository.getSource(option.getInterpreterName())
                  : null;

          interpreterStarterHandler.handle(job,
                  option,
                  source,
                  server.getRemoteServerClassPath(),
                  server.getAddr(),
                  server.getPort());
        }
      } catch (final Exception e) {
        //log this
      }
    }
  }

  @Scheduled(fixedDelay = 1_000)
  private void handleAbort() {
    final List<JobBatch> batches = abortHandler.loadJobs();
    for (final JobBatch batch : batches) {
      try {
        abortHandler.handle(batch);
      } catch (final Exception e) {
        //log this
      }
    }
  }

  @Scheduled(fixedDelay = 5_000)
  private void actualizeInterpreters() {
    final List<String> liveInterpretersPID = new ArrayList<>();
    final List<String> shebangs = InterpreterProcess.getShebangs();
    for (final String shebang : shebangs) {
      final InterpreterProcess process = InterpreterProcess.get(shebang);
      if(process.getStatus() == InterpreterProcess.Status.STARTING) {
        continue;
      }
      final InterpreterOption option = interpreterOptionRepository.getOption(shebang);
      final PingResult pingResult = process.ping();

      if (pingResult == null
              || pingResult.status == PingResultStatus.KILL_ME
              || option == null
              || !option.isEnabled()) {

        process.forceKill();
        InterpreterProcess.remove(process.getShebang());
      } else {
        liveInterpretersPID.add(process.getUuid());
      }
    }
    interpreterDeadHandler.handle(liveInterpretersPID);
  }

  //TODO(SAN) (fix new cron time)
  @Scheduled(fixedDelay = 10_000)
  private void notesScheduler() {
    final List<Scheduler> triggered = schedulerHandler.loadJobs();
    for (final Scheduler scheduler : triggered) {
      schedulerHandler.handle(scheduler);
    }
  }

  public void run(final Note note,
                  final List<Paragraph> paragraphs,
                  final String username,
                  final Set<String> roles) {
    executionHandler.run(note, paragraphs, username, roles);
  }

  public void abort(final Note note) {
    abortHandler.abort(note);
  }

}
