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

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import ru.tinkoff.zeppelin.storage.ZLog;
import ru.tinkoff.zeppelin.storage.ZLog.ET;
import ru.tinkoff.zeppelin.interpreter.thrift.PingResult;
import ru.tinkoff.zeppelin.interpreter.thrift.RegisterInfo;
import ru.tinkoff.zeppelin.interpreter.thrift.RemoteProcessThriftService;

import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Represent Interpreter process
 *
 * @author Andrey Koshkin
 * @version 1.0
 * @since 1.0
 */
public abstract class AbstractRemoteProcess<T extends RemoteProcessThriftService.Client> {

  public enum Status {
    STARTING,
    READY
  }

  private static final Map<RemoteProcessType, Map<String, AbstractRemoteProcess>> processMap = new ConcurrentHashMap<>();

  static void starting(final String shebang, final RemoteProcessType processType) {
    ZLog.log(ET.PROCESS_STARTED,
            String.format("Process started by shebang=%s", shebang),
            String.format("New interpreter process added to process map by shebang=%s", shebang),
            "Unknown");

    if (!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }
  final AbstractRemoteProcess process;
      switch (processType) {
        case INTERPRETER:
          process = new InterpreterRemoteProcess(shebang, AbstractRemoteProcess.Status.STARTING, null, -1);
          break;
        case COMPLETER:
          process = new CompleterRemoteProcess(shebang, AbstractRemoteProcess.Status.STARTING, null, -1);
          break;
        default:
          throw new IllegalArgumentException();
      }

    processMap.get(processType).put(shebang, process);
  }

  static void handleRegisterEvent(final RegisterInfo registerInfo) {
    final RemoteProcessType processType = RemoteProcessType.valueOf(registerInfo.getProcessType());
    if (!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }
    if (processMap.get(processType).containsKey(registerInfo.getShebang())) {
      final AbstractRemoteProcess process = processMap.get(processType).get(registerInfo.getShebang());
      process.host = registerInfo.getHost();
      process.port = registerInfo.getPort();
      process.uuid = registerInfo.getProcessUUID();
      process.status = Status.READY;
      ZLog.log(ET.REMOTE_CONNECTION_REGISTERED,
              String.format("Registered remote connection to interpreter process with shebang=%s", registerInfo.getShebang()),
              String.format("Received register event for interpreter, process details: shebang=%s, host=%s, port=%s, process uuid=%s",
                      registerInfo.getShebang(), registerInfo.getHost(), String.valueOf(registerInfo.getPort()),
                      registerInfo.getProcessUUID()), "Unknown");
    }
    ZLog.log(ET.BAD_REMOTE_CONNECTION,
            String.format("Requested interpreter process[shebang:%s] for remote connection not found", registerInfo.getShebang()),
            String.format("Interpreter process with shebang=%s not exist in process map, process details: host=%s, port=%s, process uuid=%s",
                    registerInfo.getShebang(), registerInfo.getHost(), String.valueOf(registerInfo.getPort()), registerInfo.getProcessUUID()),
            "Unknown");
  }

  static void remove(final String shebang, final RemoteProcessType processType) {
    if (!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }

    final AbstractRemoteProcess removedProcess = processMap.get(processType).remove(shebang);
    if (removedProcess == null) {
      ZLog.log(ET.COMPLETED_PROCESS_NOT_FOUND,
              String.format("System error, finished process by shebang: %s not found", shebang),
              String.format("Interpreter process with shebang=%s not exist in process map", shebang),
              "Unknown");
    } else {
      ZLog.log(ET.PROCESS_COMPLETED,
              String.format("Process with shebang=%s and uuid=%s finished", removedProcess.getShebang(), removedProcess.uuid),
              String.format("Process finished, details: shebang=%s, host=%s, port=%s, process uuid=%s, status=%s",
                      removedProcess.getShebang(), removedProcess.host, removedProcess.port, removedProcess.uuid, removedProcess.status),
              "Unknown");
    }
  }

  public static AbstractRemoteProcess get(final String shebang, final RemoteProcessType processType) {
    if(!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }

    return processMap.get(processType).get(shebang);
  }

  public static List<String> getShebangs(final RemoteProcessType processType) {
    if(!processMap.containsKey(processType)) {
      processMap.put(processType, new ConcurrentHashMap<>());
    }

    return new ArrayList<>(processMap.get(processType).keySet());
  }

  private final String shebang;
  private Status status;

  private String host;
  private int port;
  String uuid;

  protected AbstractRemoteProcess(final String shebang,
                                  final Status status,
                                  final String host,
                                  final int port) {
    this.shebang = shebang;
    this.status = status;
    this.host = host;
    this.port = port;
    this.uuid = null;
  }

  public String getShebang() {
    return shebang;
  }

  public Status getStatus() {
    return status;
  }

  @SuppressWarnings("unchecked")
  T getConnection() {
    final TSocket transport = new TSocket(host, port);
    try {
      transport.open();
      final TProtocol protocol = new TBinaryProtocol(transport);

      // a little bit of reflection
      final Class<T> clazz = ((Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
      final Constructor<T> constructor = clazz.getConstructor(TProtocol.class);
      return constructor.newInstance(protocol);
      //return new RemoteInterpreterThriftService.Client(protocol);

    } catch (final Exception e) {
      ZLog.log(ET.CONNECTION_FAILED,
              String.format("Failed to open connection with host=%s, port=%s", host, port),
              String.format("Error occurred during opening TSocket with host=%s, port=%s, error: %s",
                      host, port, e.getMessage()), "Unknown");
      return null;
    }
  }

  void releaseConnection(final RemoteProcessThriftService.Client connection) {
    try {
      connection.getOutputProtocol().getTransport().close();
    } catch (final Throwable t) {
      ZLog.log(ET.FAILED_TO_RELEASE_CONNECTION,
              "System error, failed to close connection",
              String.format("Failed to close connection, process details=%s, error=%s",
                      this.toString(), t.getMessage()), "Unknown");
    }
  }

  public String getUuid() {
    return uuid;
  }

  public PingResult ping() {
    final T client = getConnection();
    if (client == null) {
      ZLog.log(ET.PING_FAILED_CLIENT_NOT_FOUND,
              String.format("Ping failed: client not found, uuid=%s", this.uuid),
              String.format("Ping failed: client not found, process details=%s", this.toString()),
              "Unknown");
      return null;
    }

    try {
      return client.ping();
    } catch (final Throwable throwable) {
      ZLog.log(ET.PING_FAILED,
              String.format("Ping failed, uuid=%s", this.uuid),
              String.format("Error occurred during ping, process details=%s, error=%s",
                      this.toString(), throwable.getMessage()), "Unknown");
      return null;
    } finally {
      releaseConnection(client);
    }
  }

  public void forceKill() {
    ZLog.log(ET.FORCE_KILL_REQUESTED,
            String.format("Close process with uuid=%s", this.uuid),
            String.format("Force kill called for process: %s", this.toString()),
            "Unknown");
    final RemoteProcessThriftService.Client client = getConnection();
    if (client == null) {
      ZLog.log(ET.FORCE_KILL_FAILED_CLIENT_NOT_FOUND,
              String.format("Force kill failed: client not found, uuid=%s", this.uuid),
              String.format("Force kill failed: client not found, process details=%s", this.toString()),
              "Unknown");
      return;
    }

    try {
      client.shutdown();
    } catch (final Throwable throwable) {
      ZLog.log(ET.FORCE_KILL_FAILED,
              String.format("Force kill failed, uuid=%s", this.uuid),
              String.format("Error occurred during force kill, process details=%s, error=%s",
                      this.toString(), throwable.getMessage()), "Unknown");
    } finally {
      releaseConnection(client);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final AbstractRemoteProcess that = (AbstractRemoteProcess) o;
    return port == that.port &&
            shebang.equals(that.shebang) &&
            Objects.equals(host, that.host) &&
            Objects.equals(uuid, that.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shebang, host, port, uuid);
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
            .add("shebang='" + shebang + "'")
            .add("status=" + status)
            .add("host='" + host + "'")
            .add("port=" + port)
            .add("uuid='" + uuid + "'")
            .toString();
  }
}
