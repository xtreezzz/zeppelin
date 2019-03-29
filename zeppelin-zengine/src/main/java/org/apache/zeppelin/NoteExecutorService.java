package org.apache.zeppelin;

import org.apache.zeppelin.interpreter.configuration.InterpreterArtifactSource;
import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.apache.zeppelin.interpreter.core.thrift.PingResult;
import org.apache.zeppelin.interpreter.core.thrift.PingResultStatus;
import org.apache.zeppelin.interpreterV2.handler.*;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcess;
import org.apache.zeppelin.interpreterV2.server.InterpreterProcessServer;
import org.apache.zeppelin.notebook.Job;
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

  public NoteExecutorService(final AbortHandler abortHandler,
                             final PendingHandler pendingHandler,
                             final InterpreterDeadHandler interpreterDeadHandler,
                             final InterpreterOptionRepository interpreterOptionRepository,
                             final InterpreterStarterHandler interpreterStarterHandler) {
    this.abortHandler = abortHandler;
    this.pendingHandler = pendingHandler;
    this.interpreterDeadHandler = interpreterDeadHandler;
    this.interpreterOptionRepository = interpreterOptionRepository;
    this.interpreterStarterHandler = interpreterStarterHandler;
  }

  @PostConstruct
  public void init() throws Exception{
    server = new InterpreterProcessServer();
    server.initSources(interpreterOptionRepository.getAllRepositories());
    server.start();
  }

  @PreDestroy
  public void destroy() {
    server.stop();
  }


  @Scheduled(fixedDelay = 1000)
  public void handlePending() {
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

  @Scheduled(fixedDelay = 1000)
  public void handleAbort() {
    final List<Job> jobs = abortHandler.loadJobs();
    for (final Job job : jobs) {
      try {
        abortHandler.handle(job);
      } catch (final Exception e) {
        //log this
      }
    }
  }

  @Scheduled(fixedDelay = 5000)
  public void actualizeInterpreters() {
    final List<String> liveInterpreters = new ArrayList<>();
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
        liveInterpreters.add(process.getUuid());
      }
    }
    interpreterDeadHandler.handle(liveInterpreters);
  }
}
