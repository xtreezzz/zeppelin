package org.apache.zeppelin.interpreterV2.server;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.zeppelin.storage.ZLog;
import org.apache.zeppelin.storage.ZLog.ET;

public class InterpreterProcessStarter {


  public static void start(final String shebang,
                           final String interpreterClassPath,
                           final String interpreterClassName,
                           final String remoteServerClassPath,
                           final String thriftAddr,
                           final long thriftPort) {

    final String cmd = String.format("java " +
                    //" -agentlib:jdwp=transport=dt_socket,server=n,address=5005,suspend=y" +
                    " -cp \"./*:%s/*\"" +
                    " org.apache.zeppelin.interpreter.remote.RemoteInterpreterServer" +
                    " -h %s" +
                    " -p %s" +
                    " -sb %s" +
                    " -cp %s" +
                    " -cn %s ",
            remoteServerClassPath,
            thriftAddr,
            thriftPort,
            shebang,
            interpreterClassPath,
            interpreterClassName
    );
    ZLog.log(ET.INTERPRETER_PROCESS_START_REQUESTED,
        "Requested to start interpreter, cmd: " + cmd,
        "Requested to start interpreter, cmd: " + cmd,
        "Unknown");

    // start server process
    final CommandLine cmdLine = CommandLine.parse(cmd);

    final DefaultExecutor executor = new DefaultExecutor();
    executor.setWorkingDirectory(new File(interpreterClassPath));

    final ExecuteWatchdog watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
    executor.setWatchdog(watchdog);

    final ExecuteResultHandler handler = new ExecuteResultHandler() {

      @Override
      public void onProcessComplete(final int exitValue) {
        InterpreterProcess.handleProcessCompleteEvent(shebang);
      }

      @Override
      public void onProcessFailed(final ExecuteException e) {
        InterpreterProcess.handleProcessCompleteEvent(shebang);
      }
    };

    try {
      executor.execute(cmdLine, new HashMap<>(), handler);
      ZLog.log(ET.INTERPRETER_PROCESS_FINISHED,
          "Interpreter process finished, cmd: " + cmd,
          "Interpreter process finished, cmd: " + cmd,
          "Unknown");
    } catch (final IOException e) {
      ZLog.log(ET.INTERPRETER_PROCESS_FAILED,
          "Interpreter process failed, cmd: " + cmd,
          String.format("Error occured during process execution, cmd: %s, error: %s",
              cmd, e.getMessage()), "Unknown");
      InterpreterProcess.handleProcessCompleteEvent(shebang);
    }
  }
}
