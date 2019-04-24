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

import org.apache.zeppelin.storage.ZLog;
import ru.tinkoff.zeppelin.interpreter.thrift.CancelResult;
import ru.tinkoff.zeppelin.interpreter.thrift.PushResult;
import ru.tinkoff.zeppelin.interpreter.thrift.RemoteInterpreterThriftService;

import java.util.Map;

public class InterpreterRemoteProcess extends AbstractRemoteProcess {

  InterpreterRemoteProcess(final String shebang, final Status status, final String host, final int port) {
    super(shebang, status, host, port);
  }

  public PushResult push(final String payload,
                         final Map<String, String> noteContext,
                         final Map<String, String> userContext,
                         final Map<String, String> configuration) {
    final RemoteInterpreterThriftService.Client client = getConnection();
    if(client == null) {
      ZLog.log(ZLog.ET.PUSH_FAILED_CLIENT_NOT_FOUND,
              String.format("Push failed: client not found, uuid=%s", this.uuid),
              String.format("Push failed: client not found, process details=%s", this.toString()),
              "Unknown");
      return null;
    }

    try {
      return client.push(payload, noteContext, userContext, configuration);
    } catch (final Throwable throwable) {
      ZLog.log(ZLog.ET.PUSH_FAILED,
              String.format("Push failed, uuid=%s", this.uuid),
              String.format("Error occurred during push, process details=%s, error=%s",
                      this.toString(), throwable.getMessage()), "Unknown");
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
      ZLog.log(ZLog.ET.JOB_CANCEL_FAILED,
              String.format("Failed to cancel job with uuid: %s", interpreterJobUUID),
              String.format("Exception thrown during job canceling: cancelResult[%s]",
                      throwable.toString()),
              "Unknown");
      return null;
    } finally {
      releaseConnection(client);
    }
  }

}
