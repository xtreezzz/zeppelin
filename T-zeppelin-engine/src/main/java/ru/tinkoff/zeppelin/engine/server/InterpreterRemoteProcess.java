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

import java.util.Map;
import ru.tinkoff.zeppelin.SystemEvent;
import ru.tinkoff.zeppelin.interpreter.thrift.CancelResult;
import ru.tinkoff.zeppelin.interpreter.thrift.PushResult;
import ru.tinkoff.zeppelin.interpreter.thrift.RemoteInterpreterThriftService;
import ru.tinkoff.zeppelin.storage.SystemEventType.ET;
import ru.tinkoff.zeppelin.storage.ZLog;

public class InterpreterRemoteProcess extends AbstractRemoteProcess<RemoteInterpreterThriftService.Client> {

  InterpreterRemoteProcess(final String shebang, final Status status, final String host, final int port) {
    super(shebang, status, host, port);
  }

  public PushResult push(final String payload,
                         final Map<String, String> noteContext,
                         final Map<String, String> userContext,
                         final Map<String, String> configuration) {
    final RemoteInterpreterThriftService.Client client = getConnection();
    if(client == null) {

      ZLog.log(ET.PUSH_FAILED_CLIENT_NOT_FOUND,
          String.format("Не удалось вызвать интерпретатор: клиент не найден, uuid=%s", this.uuid),
          String.format("Не удалось вызвать интерпретатор: клиент не найден, информация о процессе=%s", this.toString()),
          SystemEvent.SYSTEM_USERNAME);
      return null;
    }

    try {
      return client.push(payload, noteContext, userContext, configuration);
    } catch (final Throwable throwable) {

      ZLog.log(ET.PUSH_FAILED, String.format("Не удалось вызвать интерпретатор: uuid=%s", this.uuid),
          String.format("Ошибка в ходе вызова интерпретатора, описание процесса=%s, ошибка=%s",
              this.toString(), throwable.getMessage()), SystemEvent.SYSTEM_USERNAME);
      return null;
    } finally {
      releaseConnection(client);
    }
  }

  public CancelResult cancel(String interpreterJobUUID) {
    final RemoteInterpreterThriftService.Client client = getConnection();
    if(client == null) {
      return null;
    }

    try {
      return client.cancel(interpreterJobUUID);
    } catch (final Throwable throwable) {
      ZLog.log(ET.JOB_CANCEL_FAILED,
              String.format("Failed to cancel job with uuid: %s", interpreterJobUUID),
              String.format("Exception thrown during job canceling: cancelResult[%s]",
                      throwable.toString())
      );
      return null;
    } finally {
      releaseConnection(client);
    }
  }

}
