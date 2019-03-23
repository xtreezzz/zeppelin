package org.apache.zeppelin.interpreterV2.server;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zeppelin.interpreter.configuration.InterpreterOption;
import org.apache.zeppelin.interpreter.core.thrift.RemoteInterpreterService;

public class InterpreterProcess {

  public enum Status {
    NOT_FOUND,
    STARTING,
    READY,
    DEAD
  }

  private String shebang;
  private Status status;
  private InterpreterOption config;
  private RemoteInterpreterService.Client connection;

  private String host;
  private int port;
  private String interpreterProcessUUID;


  public String getShebang() {
    return shebang;
  }

  public void setShebang(final String shebang) {
    this.shebang = shebang;
  }

  public Status getStatus() {
    return status;
  }

  public void setStatus(final Status status) {
    this.status = status;
  }

  public InterpreterOption getConfig() {
    return config;
  }

  public void setConfig(final InterpreterOption config) {
    this.config = config;
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


  public String getHost() {
    return host;
  }

  public void setHost(final String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(final int port) {
    this.port = port;
  }

  public String getInterpreterProcessUUID() {
    return interpreterProcessUUID;
  }

  public void setInterpreterProcessUUID(final String interpreterProcessUUID) {
    this.interpreterProcessUUID = interpreterProcessUUID;
  }
}
