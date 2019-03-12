package org.apache.zeppelin.interpreterV2.server;

import org.apache.thrift.TException;
import org.apache.zeppelin.interpreter.core.thrift.OutputAppendEvent;
import org.apache.zeppelin.interpreter.core.thrift.RegisterInfo;
import org.apache.zeppelin.interpreter.core.thrift.RemoteInterpreterEventService;

import java.util.function.Consumer;

public class InterpreterProcessServerEventHandler  implements RemoteInterpreterEventService.Iface  {

  private final Consumer<RegisterInfo> onStartInterpreterCallback;

  public InterpreterProcessServerEventHandler(final Consumer<RegisterInfo> onStartInterpreterCallback) {
    this.onStartInterpreterCallback = onStartInterpreterCallback;
  }

  @Override
  public void registerInterpreterProcess(final RegisterInfo registerInfo) throws TException {
    onStartInterpreterCallback.accept(registerInfo);
  }

  @Override
  public void appendOutput(final OutputAppendEvent event) throws TException {

  }
}
