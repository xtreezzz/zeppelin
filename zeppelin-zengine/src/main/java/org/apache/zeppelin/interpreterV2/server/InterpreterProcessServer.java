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

package org.apache.zeppelin.interpreterV2.server;

import com.google.gson.Gson;
import java.util.List;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.Repository;
import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.apache.zeppelin.interpreter.core.thrift.RegisterInfo;
import org.apache.zeppelin.interpreter.core.thrift.RemoteInterpreterEventService;
import org.apache.zeppelin.interpreterV2.handler.InterpreterResultHandler;
import org.apache.zeppelin.storage.ZLog;
import org.apache.zeppelin.storage.ZLog.ET;

public class InterpreterProcessServer {

  private TServerSocket serverSocket;
  private TThreadPoolServer thriftServer;

  private String remoteServerClassPath;

  public void initSources(final List<Repository> repositories) {
    InterpreterInstaller.uninstallInterpreter("remote-server");
    final InterpreterInstaller interpreterInstaller = new InterpreterInstaller();
    interpreterInstaller.install("remote-server", "org.apache.zeppelin:zeppelin-interpreter:1.0.0-T-SNAPSHOT", repositories);
    remoteServerClassPath = InterpreterInstaller.getDirectory("remote-server");
  }

  public void start() throws TTransportException {
    this.serverSocket = new TServerSocket(40000);

    final Thread startingThread = new Thread(() -> {
      ZLog.log(ET.INTERPRETER_EVENT_SERVER_STARTING,
          String.format("InterpreterEventServer is starting at %s:%s",
              serverSocket.getServerSocket().getInetAddress().getHostAddress(),
              serverSocket.getServerSocket().getLocalPort()),
          String.format("InterpreterEventServer is starting at %s:%s",
              serverSocket.getServerSocket().getInetAddress().getHostAddress(),
              serverSocket.getServerSocket().getLocalPort()), "Unknown"
      );
      final RemoteInterpreterEventService.Processor<RemoteInterpreterEventService.Iface> processor;
      processor = new RemoteInterpreterEventService.Processor<>(new RemoteInterpreterEventService.Iface() {
        @Override
        public void registerInterpreterProcess(final RegisterInfo registerInfo) {
          InterpreterProcess.handleRegisterEvent(registerInfo);
        }

        @Override
        public void handleInterpreterResult(final String UUID, final String payload) {
          InterpreterResultHandler.getInstance().handle(UUID, new Gson().fromJson(payload, InterpreterResult.class));
        }
      });

      thriftServer = new TThreadPoolServer(new TThreadPoolServer.Args(serverSocket).processor(processor));
      thriftServer.serve();
    });
    startingThread.start();
    final long start = System.currentTimeMillis();
    while ((System.currentTimeMillis() - start) < 30 * 1000) {
      if (thriftServer != null && thriftServer.isServing()) {
        break;
      }
      try {
        Thread.sleep(500);
      } catch (final InterruptedException e) {
        ZLog.log(ET.INTERPRETER_EVENT_SERVER_START_FAILED,
            String.format("Failed to start interpreter event server at %s:%s",
                serverSocket.getServerSocket().getInetAddress().getHostAddress(),
                serverSocket.getServerSocket().getLocalPort()),
            String.format("Error occurred during interpreter event server start at %s:%s, error:%s",
                serverSocket.getServerSocket().getInetAddress().getHostAddress(),
                serverSocket.getServerSocket().getLocalPort(), e.getMessage()),
            "Unknown");
      }
    }

    if (thriftServer != null && !thriftServer.isServing()) {
      throw new TTransportException("Fail to start InterpreterEventServer in 30 seconds.");
    }
    ZLog.log(ET.INTERPRETER_EVENT_SERVER_STARTED,
        String.format("InterpreterEventServer is started at %s:%s",
            serverSocket.getServerSocket().getInetAddress().getHostAddress(),
            serverSocket.getServerSocket().getLocalPort()),
        String.format("InterpreterEventServer is started at %s:%s",
            serverSocket.getServerSocket().getInetAddress().getHostAddress(),
            serverSocket.getServerSocket().getLocalPort()), "Unknown"
    );
  }

  public void stop() {
    if (thriftServer != null) {
      thriftServer.stop();
    }
    ZLog.log(ET.INTERPRETER_EVENT_SERVER_STOPPED,
        String.format("InterpreterEventServer at %s:%s stopped",
            serverSocket.getServerSocket().getInetAddress().getHostAddress(),
            serverSocket.getServerSocket().getLocalPort()),
        String.format("InterpreterEventServer at %s:%s stopped",
            serverSocket.getServerSocket().getInetAddress().getHostAddress(),
            serverSocket.getServerSocket().getLocalPort()), "Unknown"
    );
  }

  public String getAddr() {
    return "127.0.0.1";
  }

  public int getPort() {
    return 40000;
  }

  public String getRemoteServerClassPath() {
    return remoteServerClassPath;
  }
}
