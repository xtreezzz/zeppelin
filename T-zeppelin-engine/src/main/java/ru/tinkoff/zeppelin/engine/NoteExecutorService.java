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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterArtifactSource;
import ru.tinkoff.zeppelin.core.configuration.interpreter.InterpreterOption;
import ru.tinkoff.zeppelin.core.notebook.Job;
import ru.tinkoff.zeppelin.core.notebook.JobBatch;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;
import ru.tinkoff.zeppelin.core.notebook.Scheduler;
import ru.tinkoff.zeppelin.engine.handler.AbortHandler;
import ru.tinkoff.zeppelin.engine.handler.ExecutionHandler;
import ru.tinkoff.zeppelin.engine.handler.InterpreterDeadHandler;
import ru.tinkoff.zeppelin.engine.handler.InterpreterStarterHandler;
import ru.tinkoff.zeppelin.engine.handler.PendingHandler;
import ru.tinkoff.zeppelin.engine.handler.SchedulerHandler;
import ru.tinkoff.zeppelin.engine.server.AbstractRemoteProcess;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessServer;
import ru.tinkoff.zeppelin.engine.server.RemoteProcessType;
import ru.tinkoff.zeppelin.interpreter.thrift.PingResult;
import ru.tinkoff.zeppelin.interpreter.thrift.PingResultStatus;

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

  private RemoteProcessServer server;

  private final AbortHandler abortHandler;
  private final PendingHandler pendingHandler;
  private final InterpreterDeadHandler interpreterDeadHandler;
  private final InterpreterSettingService interpreterSettingService;
  private final InterpreterStarterHandler interpreterStarterHandler;
  private final SchedulerHandler schedulerHandler;
  private final ExecutionHandler executionHandler;

  public NoteExecutorService(final AbortHandler abortHandler,
                             final PendingHandler pendingHandler,
                             final InterpreterDeadHandler interpreterDeadHandler,
                             final InterpreterSettingService interpreterSettingService,
                             final InterpreterStarterHandler interpreterStarterHandler,
                             final SchedulerHandler schedulerHandler,
                             final ExecutionHandler executionHandler) {
    this.abortHandler = abortHandler;
    this.pendingHandler = pendingHandler;
    this.interpreterDeadHandler = interpreterDeadHandler;
    this.interpreterSettingService = interpreterSettingService;
    this.interpreterStarterHandler = interpreterStarterHandler;
    this.schedulerHandler = schedulerHandler;
    this.executionHandler = executionHandler;
  }

  @PostConstruct
  public void init() throws Exception{
    server = new RemoteProcessServer();
    server.initSources(interpreterSettingService.getAllRepositories());
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
        final InterpreterOption option = interpreterSettingService.getOption(job.getShebang());
        final AbstractRemoteProcess process = AbstractRemoteProcess.get(job.getShebang(), RemoteProcessType.INTERPRETER);
        if (process != null
                && process.getStatus() == AbstractRemoteProcess.Status.READY
                && option != null) {
          pendingHandler.handle(job, process, option);
        } else if (process != null
                && process.getStatus() == AbstractRemoteProcess.Status.STARTING
                && option != null) {
          final String str = ";";
        } else {
          final InterpreterArtifactSource source = option != null
                  ? interpreterSettingService.getSource(option.getInterpreterName())
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
    final List<String> shebangs = AbstractRemoteProcess.getShebangs(RemoteProcessType.INTERPRETER);
    for (final String shebang : shebangs) {
      final AbstractRemoteProcess process = AbstractRemoteProcess.get(shebang, RemoteProcessType.INTERPRETER);
      if(process.getStatus() == AbstractRemoteProcess.Status.STARTING) {
        continue;
      }
      final InterpreterOption option = interpreterSettingService.getOption(shebang);
      final PingResult pingResult = process.ping();

      if (pingResult == null
              || pingResult.status == PingResultStatus.KILL_ME
              || option == null
              || !option.isEnabled()) {

        process.forceKill();
        AbstractRemoteProcess.remove(process.getShebang(), RemoteProcessType.INTERPRETER);
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
