//package org.apache.zeppelin.interpreterV2.server;
//
//import org.apache.commons.exec.ExecuteException;
//import org.apache.thrift.transport.TServerSocket;
//import org.apache.thrift.transport.TTransportException;
//import org.apache.zeppelin.interpreter.core.thrift.RegisterInfo;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.HashMap;
//import java.util.Map;
//import java.util.function.Consumer;
//
//public class InterpreterProcessServerManager {
//
//  private static final Logger LOG = LoggerFactory.getLogger(InterpreterProcessServerManager.class);
//
//
//  private final InterpreterProcessServer server;
//
//  private final InterpreterProcessInstaller interpreterProcessInstaller;
//
//
//  private final InterpreterProcessServerEventHandler eventHandler;
//
//
//  private final Map<String, RegisterInfo> registeredInterpreters = new HashMap<>();
//
//  public InterpreterProcessServerManager() {
//    this.server = new InterpreterProcessServer();
//    this.interpreterProcessInstaller = new InterpreterProcessInstaller();
//
//    final Consumer<RegisterInfo> onStartInterpreterCallback = this::onInterpreterprocessStarted;
//    this.eventHandler = new InterpreterProcessServerEventHandler(onStartInterpreterCallback);
//  }
//
//
//  public void startServer() throws TTransportException {
//    server.start(eventHandler);
//  }
//
//  public void stopServer() {
//    server.stop();
//  }
//
//  public TServerSocket getServerSocket() {
//    return server.getServerSocket();
//  }
//
//  public boolean isServerRunning() {
//    return server.isRunning();
//  }
//
//  public boolean isInterpreterInstalled(final String interpreterGroup, final String artifact) {
//    return interpreterProcessInstaller.isInterpreterInstalled(interpreterGroup, artifact);
//  }
//
//  public void installInterpreter(final String interpreterGroup, final String artifact) {
//    interpreterProcessInstaller.installInterpreter(interpreterGroup, artifact);
//  }
//
//  public void uninstallInterpreter(final String interpreterGroup, final String artifact) {
//    interpreterProcessInstaller.uninstallInterpreter(interpreterGroup, artifact);
//  }
//
//
//
//  public void startInterpreterProcess() {
//    /*
//    // start server process
//    CommandLine cmdLine = CommandLine.parse(interpreterRunner);
//    cmdLine.addArgument("-d", false);
//    //cmdLine.addArgument(interpreterDir, false);
//    cmdLine.addArgument("-c", false);
//    //cmdLine.addArgument(zeppelinServerRPCHost, false);
//    cmdLine.addArgument("-p", false);
//    //cmdLine.addArgument(String.valueOf(zeppelinServerRPCPort), false);
//    cmdLine.addArgument("-r", false);
//    //cmdLine.addArgument(interpreterPortRange, false);
//    cmdLine.addArgument("-i", false);
//    //cmdLine.addArgument(interpreterGroupId, false);
//    //if (isUserImpersonated && !userName.equals("anonymous")) {
//    //  cmdLine.addArgument("-u", false);
//    //  cmdLine.addArgument(userName, false);
//    //}
//    cmdLine.addArgument("-l", false);
//    //cmdLine.addArgument(localRepoDir, false);
//    cmdLine.addArgument("-g", false);
//    //cmdLine.addArgument(interpreterSettingName, false);
//
//    final DefaultExecutor executor = new DefaultExecutor();
//
//    final ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
//    executor.setWatchdog(watchdog);
//
//    final Consumer<Integer> onProcessCompleteConsumer = this::onInterpreterProcessComplete;
//    final Consumer<ExecuteException> onInterpreterProcessFailedConsumer = this::onInterpreterProcessFailed;
//    final ExecuteResultHandler handler = new ExecuteResultHandler() {
//      @Override
//      public void onProcessComplete(final int exitValue) {
//        onProcessCompleteConsumer.accept(exitValue);
//      }
//
//      @Override
//      public void onProcessFailed(final ExecuteException e) {
//        onInterpreterProcessFailedConsumer.accept(e);
//      }
//    };
//
//    try {
//      executor.execute(cmdLine, new HashMap<>(), handler);
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//    */
//  }
//
//  public void stopInterpreterProcess(final String interpreterGroup) {
//    //registeredInterpreters.get(interpreterGroup);
//    //executor.getWatchdog().destroyProcess();
//  }
//
//
//  public void onInterpreterprocessStarted(final RegisterInfo registerInfo) {
//    registeredInterpreters.put(registerInfo.interpreterGroup, registerInfo);
//  }
//
//  public void onInterpreterProcessComplete(final int exitValue) {
//
//  }
//
//  public void onInterpreterProcessFailed(final ExecuteException e) {
//
//  }
//
//  public Map<String, RegisterInfo> getRegisteredInterpreters() {
//    return registeredInterpreters;
//  }
//}
