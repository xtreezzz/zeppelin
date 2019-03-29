package org.apache.zeppelin.interpreterV2.server;

import org.apache.commons.exec.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class InterpreterProcessStarter {


  public static void start(final String shebang,
                           final String interpreterClassPath,
                           final String interpreterClassName,
                           final String remoteServerClassPath,
                           final String thriftAddr,
                           final long thriftPort) {

    final String cmd = String.format("java " +
                    //  " -agentlib:jdwp=transport=dt_socket,server=n,address=172.27.79.51:5005,suspend=y" +
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
    } catch (final IOException e) {
      InterpreterProcess.handleProcessCompleteEvent(shebang);
    }
  }
}
