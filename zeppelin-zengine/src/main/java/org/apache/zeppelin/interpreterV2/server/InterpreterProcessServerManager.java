package org.apache.zeppelin.interpreterV2.server;

import org.apache.commons.exec.ExecuteException;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.interpreter.core.thrift.RegisterInfo;
import org.apache.zeppelin.interpreterV2.configuration.BaseInterpreterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Component
public class InterpreterProcessServerManager {

  private static final Logger LOG = LoggerFactory.getLogger(InterpreterProcessServerManager.class);

  private final InterpreterProcessServer server;
  private final InterpreterInstaller interpreterInstaller;


  private final InterpreterProcessServerEventHandler eventHandler;


  private final Map<String, RegisterInfo> registeredInterpreters = new HashMap<>();
  private final Map<String, BaseInterpreterConfig> defaultInterpreterConfigs = new HashMap<>();

  public InterpreterProcessServerManager() {
    this.server = new InterpreterProcessServer();
    this.interpreterInstaller = new InterpreterInstaller();

    final Consumer<RegisterInfo> onStartInterpreterCallback = this::onInterpreterprocessStarted;
    this.eventHandler = new InterpreterProcessServerEventHandler(onStartInterpreterCallback);
  }

  public static class InterpreterConfig {
    public String interpreterGroup;
    public String artifactName;
  }

  @PostConstruct
  public void init() {
    final List<InterpreterConfig> interpreterConfigs = new ArrayList<>();
    final InterpreterConfig config1 = new InterpreterConfig();
    config1.interpreterGroup = "md";
    config1.artifactName = "org.apache.zeppelin:zeppelin-markdown:0.9.0-SNAPSHOT";
    interpreterConfigs.add(config1);

    for (final InterpreterConfig config : interpreterConfigs) {
      final String group = config.interpreterGroup;
      final String artifact = config.artifactName;
      if(!interpreterInstaller.isInstalled(group, artifact)) {
        interpreterInstaller.install(group, artifact);
      }

      final String classPath = interpreterInstaller.getDirectory(group, artifact);
      final List<BaseInterpreterConfig> defaultConfig = interpreterInstaller.getDafaultConfig(group, artifact);

      interpreterInstaller.getDafaultConfig(group, artifact).forEach(c -> defaultInterpreterConfigs.put(c.getGroup(), c));
    }

    final String tst = ";";

  }

  public void startServer() throws TTransportException {
    server.start(eventHandler);
  }

  public void stopServer() {
    server.stop();
  }

  public TServerSocket getServerSocket() {
    return server.getServerSocket();
  }

  public boolean isServerRunning() {
    return server.isRunning();
  }



  public void startInterpreterProcess() {
    /*
    // start server process
    CommandLine cmdLine = CommandLine.parse(interpreterRunner);
    cmdLine.addArgument("-d", false);
    //cmdLine.addArgument(interpreterDir, false);
    cmdLine.addArgument("-c", false);
    //cmdLine.addArgument(zeppelinServerRPCHost, false);
    cmdLine.addArgument("-p", false);
    //cmdLine.addArgument(String.valueOf(zeppelinServerRPCPort), false);
    cmdLine.addArgument("-r", false);
    //cmdLine.addArgument(interpreterPortRange, false);
    cmdLine.addArgument("-i", false);
    //cmdLine.addArgument(interpreterGroupId, false);
    //if (isUserImpersonated && !userName.equals("anonymous")) {
    //  cmdLine.addArgument("-u", false);
    //  cmdLine.addArgument(userName, false);
    //}
    cmdLine.addArgument("-l", false);
    //cmdLine.addArgument(localRepoDir, false);
    cmdLine.addArgument("-g", false);
    //cmdLine.addArgument(interpreterSettingName, false);

    final DefaultExecutor executor = new DefaultExecutor();

    final ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    executor.setWatchdog(watchdog);

    final Consumer<Integer> onProcessCompleteConsumer = this::onInterpreterProcessComplete;
    final Consumer<ExecuteException> onInterpreterProcessFailedConsumer = this::onInterpreterProcessFailed;
    final ExecuteResultHandler handler = new ExecuteResultHandler() {
      @Override
      public void onProcessComplete(final int exitValue) {
        onProcessCompleteConsumer.accept(exitValue);
      }

      @Override
      public void onProcessFailed(final ExecuteException e) {
        onInterpreterProcessFailedConsumer.accept(e);
      }
    };

    try {
      executor.execute(cmdLine, new HashMap<>(), handler);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    */
  }

  public void stopInterpreterProcess(final String interpreterGroup) {
    //registeredInterpreters.get(interpreterGroup);
    //executor.getWatchdog().destroyProcess();
  }


  public void onInterpreterprocessStarted(final RegisterInfo registerInfo) {
    registeredInterpreters.put(registerInfo.interpreterGroup, registerInfo);
  }

  public void onInterpreterProcessComplete(final int exitValue) {

  }

  public void onInterpreterProcessFailed(final ExecuteException e) {

  }

  public Map<String, RegisterInfo> getRegisteredInterpreters() {
    return registeredInterpreters;
  }
}
