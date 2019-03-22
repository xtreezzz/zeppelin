package org.apache.zeppelin.interpreterV2.server;

import com.google.gson.Gson;
import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.apache.zeppelin.interpreter.core.thrift.RegisterInfo;
import org.apache.zeppelin.interpreter.core.thrift.RemoteInterpreterEventService;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class InterpreterProcessServerEventHandler  implements RemoteInterpreterEventService.Iface  {

  private final Consumer<RegisterInfo> registerInfoConsumer;
  private final BiConsumer<String , InterpreterResult> interpreterResultBiConsumer;

  public InterpreterProcessServerEventHandler(final Consumer<RegisterInfo> registerInfoConsumer,
                                              final BiConsumer<String , InterpreterResult> interpreterResultBiConsumer) {
    this.registerInfoConsumer = registerInfoConsumer;
    this.interpreterResultBiConsumer = interpreterResultBiConsumer;
  }

  @Override
  public void registerInterpreterProcess(final RegisterInfo registerInfo) {
    registerInfoConsumer.accept(registerInfo);
  }

  @Override
  public void handleInterpreterResult(final String UUID, final String payload) {
    interpreterResultBiConsumer.accept(UUID, new Gson().fromJson(payload, InterpreterResult.class));
  }
}
