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

import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.interpreter.core.thrift.RemoteInterpreterEventService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InterpreterProcessServer {

  private static final Logger LOGGER = LoggerFactory.getLogger(InterpreterProcessServer.class);

  private TServerSocket serverSocket;
  private TThreadPoolServer thriftServer;


  void start(final RemoteInterpreterEventService.Iface facade) throws TTransportException {
    this.serverSocket = new TServerSocket(40000);

    final Thread startingThread = new Thread(() -> {
      LOGGER.info(
              "InterpreterEventServer is starting at {}:{}",
              serverSocket.getServerSocket().getInetAddress().getHostAddress(),
              serverSocket.getServerSocket().getLocalPort()
      );
      final RemoteInterpreterEventService.Processor<RemoteInterpreterEventService.Iface> processor;
      processor = new RemoteInterpreterEventService.Processor<>(facade);

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
        // skip
      }
    }

    if (thriftServer != null && !thriftServer.isServing()) {
      throw new TTransportException("Fail to start InterpreterEventServer in 30 seconds.");
    }
    LOGGER.info("RemoteInterpreterEventServer is started");
  }

  void stop() {
    if (thriftServer != null) {
      thriftServer.stop();
    }
    LOGGER.info("RemoteInterpreterEventServer is stopped");
  }

  TServerSocket getServerSocket() {
    return serverSocket;
  }
}
