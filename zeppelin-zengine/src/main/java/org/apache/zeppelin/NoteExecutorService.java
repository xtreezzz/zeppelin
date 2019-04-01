package org.apache.zeppelin;

import org.apache.zeppelin.interpreter.configuration.InterpreterArtifactSource;
import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.apache.zeppelin.interpreter.core.thrift.PingResult;
import org.apache.zeppelin.interpreter.core.thrift.PingResultStatus;
import org.apache.zeppelin.interpreterV2.handler.*;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcess;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcessServer;
import org.apache.zeppelin.notebook.Job;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.Scheduler;
import org.apache.zeppelin.storage.InterpreterOptionRepository;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;

@Lazy(false)
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
  private void init() throws Exception{
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
    final List<Job> jobs = abortHandler.loadJobs();
    for (final Job job : jobs) {
      try {
        abortHandler.handle(job);
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

  @Scheduled(fixedDelay = 10_000)
  private void notesScheduler() {
    final List<Scheduler> triggered = schedulerHandler.loadJobs();
    for (final Scheduler scheduler : triggered) {
      schedulerHandler.handle(scheduler);
    }
  }

  public void run(final Note note, final List<Paragraph> paragraphs) {
    executionHandler.run(note, paragraphs);
  }

  public void abort(final Note note) {
    abortHandler.abort(note);
  }

}
