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
package ru.tinkoff.zeppelin.engine.server;

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


/**
 * Class for execute cmd
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
public class InterpreterProcessStarter {


  public static void start(final String shebang,
                           final String interpreterClassPath,
                           final String interpreterClassName,
                           final String remoteServerClassPath,
                           final String thriftAddr,
                           final long thriftPort,
                           final String jvmOptions,
                           final String zeppelinInstance) {

    final String cmd = String.format("java " +
                    " -DzeppelinInstance=%s" +
                    " %s" +
                    " -cp \"./*:%s/*\"" +
                    " ru.tinkoff.zeppelin.remote.RemoteInterpreterServer" +
                    " -h %s" +
                    " -p %s" +
                    " -sb %s" +
                    " -cp %s" +
                    " -cn %s ",
            zeppelinInstance,
            jvmOptions,
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
