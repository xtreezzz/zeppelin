package org.apache.zeppelin.interpreterV2.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.interpreter.core.thrift.PingResult;
import org.apache.zeppelin.interpreter.core.thrift.PushResult;
import org.apache.zeppelin.interpreter.core.thrift.RegisterInfo;
import org.apache.zeppelin.interpreter.core.thrift.RemoteInterpreterService;
import org.apache.zeppelin.storage.ZLog;
import org.apache.zeppelin.storage.ZLog.ET;

public class InterpreterProcess {

  public enum Status {
    STARTING,
    READY
  }

  private static final Map<String, InterpreterProcess> processMap = new ConcurrentHashMap<>();

  public static void starting(final String shebang) {
    ZLog.log(ET.PROCESS_STARTED,
        String.format("Process started by shebang=%s", shebang),
        String.format("New interpreter process added to process map by shebang=%s", shebang),
        "Unknown");
    processMap.put(shebang, new InterpreterProcess(shebang, Status.STARTING, null, -1));
  }

  static void handleRegisterEvent(final RegisterInfo registerInfo) {
    if (processMap.containsKey(registerInfo.getShebang())) {
      final InterpreterProcess process = processMap.get(registerInfo.getShebang());
      process.host = registerInfo.getHost();
      process.port = registerInfo.getPort();
      process.uuid = registerInfo.getInterpreterProcessUUID();
      process.status = Status.READY;
      ZLog.log(ET.REMOTE_CONNECTION_REGISTERED,
          String.format("Registered remote connection to interpreter process with shebang=%s", registerInfo.getShebang()),
          String.format("Received register event for interpreter, process details: shebang=%s, host=%s, port=%s, process uuid=%s",
              registerInfo.getShebang(), registerInfo.getHost(), String.valueOf(registerInfo.getPort()),
              registerInfo.getInterpreterProcessUUID()), "Unknown");
    }
    ZLog.log(ET.BAD_REMOTE_CONNECTION,
        String.format("Requested interpreter process[shebang:%s] for remote connection not found", registerInfo.getShebang()),
        String.format("Interpreter process with shebang=%s not exist in process map, process details: host=%s, port=%s, process uuid=%s",
            registerInfo.getShebang(), registerInfo.getHost(), String.valueOf(registerInfo.getPort()), registerInfo.getInterpreterProcessUUID()),
        "Unknown");
  }

  static void handleProcessCompleteEvent(final String shebang) {
    final InterpreterProcess foundedProcess = processMap.remove(shebang);
    if (foundedProcess == null) {
      ZLog.log(ET.COMPLETED_PROCESS_NOT_FOUND,
          String.format("System error, finished process by shebang: %s not found", shebang),
          String.format("Interpreter process with shebang=%s not exist in process map", shebang),
          "Unknown");
    } else {
      ZLog.log(ET.PROCESS_COMPLETED,
          String.format("Process with shebang=%s and uuid=%s finished", foundedProcess.getShebang(), foundedProcess.uuid),
          String.format("Process finished, details: shebang=%s, host=%s, port=%s, process uuid=%s, status=%s",
              foundedProcess.getShebang(), foundedProcess.host, foundedProcess.port, foundedProcess.uuid, foundedProcess.status),
          "Unknown");
    }
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

  private final String shebang;
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
      ZLog.log(ET.CONNECTION_FAILED,
          String.format("Failed to open connection with host=%s, port=%s", host, port),
          String.format("Error occurred during opening TSocket with host=%s, port=%s, error: %s",
              host, port, e.getMessage()), "Unknown");
      return null;
    }
    final TProtocol protocol = new TBinaryProtocol(transport);
    return new RemoteInterpreterService.Client(protocol);
  }

  public void releaseConnection(final RemoteInterpreterService.Client connection) {
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

  public PushResult push(final String payload,
                         final Map<String, String> noteContext,
                         final Map<String, String> userContext,
                         final Map<String, String> configuration) {
    final RemoteInterpreterService.Client client = getConnection();
    if(client == null) {
      ZLog.log(ET.PUSH_FAILED_CLIENT_NOT_FOUND,
          String.format("Push failed: client not found, uuid=%s", this.uuid),
          String.format("Push failed: client not found, process details=%s", this.toString()),
          "Unknown");
      return null;
    }

    try {
      return client.push(payload, noteContext, userContext, configuration);
    } catch (final Throwable throwable) {
      ZLog.log(ET.PUSH_FAILED,
          String.format("Push failed, uuid=%s", this.uuid),
          String.format("Error occurred during push, process details=%s, error=%s",
              this.toString(), throwable.getMessage()), "Unknown");
      return null;
    } finally {
      releaseConnection(client);
    }
  }

  public PingResult ping() {
    final RemoteInterpreterService.Client client = getConnection();
    if(client == null) {
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
    final RemoteInterpreterService.Client client = getConnection();
    if(client == null) {
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
