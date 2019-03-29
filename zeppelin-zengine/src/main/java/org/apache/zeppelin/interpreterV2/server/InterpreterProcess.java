package org.apache.zeppelin.interpreterV2.server;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.interpreter.core.thrift.PingResult;
import org.apache.zeppelin.interpreter.core.thrift.PushResult;
import org.apache.zeppelin.interpreter.core.thrift.RegisterInfo;
import org.apache.zeppelin.interpreter.core.thrift.RemoteInterpreterService;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public class InterpreterProcess {

  public enum Status {
    STARTING,
    READY
  }

  private static Map<String, InterpreterProcess> processMap = new ConcurrentHashMap<>();

  public static void starting(final String shebang) {
    processMap.put(shebang, new InterpreterProcess(shebang, Status.STARTING, null, -1));
  }

  static void handleRegisterEvent(final RegisterInfo registerInfo) {
    if (processMap.containsKey(registerInfo.getShebang())) {
      final InterpreterProcess process = processMap.get(registerInfo.getShebang());
      process.host = registerInfo.getHost();
      process.port = registerInfo.getPort();
      process.uuid = registerInfo.getInterpreterProcessUUID();
      process.status = Status.READY;
    }
  }

  static void handleProcessCompleteEvent(final String shebang) {
    processMap.remove(shebang);
  }

  public static void remove(final String shebang) {
    processMap.remove(shebang);
  }

  public static InterpreterProcess get(final String shebang) {
    return processMap.get(shebang);
  }

  public static List<String> getShebangs() {
    return new ArrayList<>(processMap.keySet());
  }


  private String shebang;
  private Status status;

  private String host;
  private int port;
  private String uuid;

  private InterpreterProcess(final String shebang, final Status status, final String host, final int port) {
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

  public RemoteInterpreterService.Client getConnection() {
    final TSocket transport = new TSocket(host, port);
    try {
      transport.open();
    } catch (final TTransportException e) {
      return null;
    }
    final TProtocol protocol = new TBinaryProtocol(transport);
    return new RemoteInterpreterService.Client(protocol);
  }

  public void releaseConnection(final RemoteInterpreterService.Client connection) {
    try {
      connection.getOutputProtocol().getTransport().close();
    } catch (final Throwable t) {
      // skip
    }
  }

  public String getUuid() {
    return uuid;
  }

  public PushResult push(final String payload,
                         final Map<String, String> noteContext,
                         final Map<String, String> userContext,
                         final Map<String, String> configuration) {
    final RemoteInterpreterService.Client client = getConnection();
    if(client == null) {
      return null;
    }

    try {
      return client.push(payload, noteContext, userContext, configuration);
    } catch (final Throwable throwable) {
      return null;
    } finally {
      releaseConnection(client);
    }
  }

  public PingResult ping() {
    final RemoteInterpreterService.Client client = getConnection();
    if(client == null) {
      return null;
    }

    try {
      return client.ping();
    } catch (final Throwable throwable) {
      return null;
    } finally {
      releaseConnection(client);
    }
  }

  public void forceKill() {
    final RemoteInterpreterService.Client client = getConnection();
    if(client == null) {
      return;
    }

    try {
      client.shutdown();
    } catch (final Throwable throwable) {
      // log this
    } finally {
      releaseConnection(client);
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final InterpreterProcess that = (InterpreterProcess) o;
    return port == that.port &&
            shebang.equals(that.shebang) &&
            Objects.equals(host, that.host) &&
            Objects.equals(uuid, that.uuid);
  }

  @Override
  public int hashCode() {
    return Objects.hash(shebang, host, port, uuid);
  }
}
