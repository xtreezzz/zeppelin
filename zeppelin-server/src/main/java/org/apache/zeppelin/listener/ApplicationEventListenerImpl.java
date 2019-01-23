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

package org.apache.zeppelin.listener;

import org.apache.zeppelin.helium.ApplicationEventListener;
import org.apache.zeppelin.helium.HeliumPackage;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.websocket.ConnectionManager;
import org.apache.zeppelin.websocket.Operation;
import org.apache.zeppelin.websocket.SockMessage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component(value = "ApplicationEventListenerImpl")
public class ApplicationEventListenerImpl implements ApplicationEventListener {

  private final ConnectionManager connectionManager;

  @Autowired
  public ApplicationEventListenerImpl(final ConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }

  /**
   * When application append output.
   */
  @Override
  public void onOutputAppend(final String noteId,
                             final String paragraphId,
                             final int index,
                             final String appId,
                             final String output) {
    final SockMessage msg = new SockMessage(Operation.APP_APPEND_OUTPUT)
            .put("noteId", noteId)
            .put("paragraphId", paragraphId)
            .put("index", index)
            .put("appId", appId)
            .put("data", output);
    connectionManager.broadcast(noteId, msg);
  }

  /**
   * When application update output.
   */
  @Override
  public void onOutputUpdated(final String noteId,
                              final String paragraphId,
                              final int index,
                              final String appId,
                              final InterpreterResult.Type type,
                              final String output) {
    final SockMessage msg = new SockMessage(Operation.APP_UPDATE_OUTPUT)
            .put("noteId", noteId)
            .put("paragraphId", paragraphId)
            .put("index", index)
            .put("type", type)
            .put("appId", appId)
            .put("data", output);
    connectionManager.broadcast(noteId, msg);
  }

  @Override
  public void onLoad(final String noteId,
                     final String paragraphId,
                     final String appId,
                     final HeliumPackage pkg) {
    final SockMessage msg = new SockMessage(Operation.APP_LOAD)
            .put("noteId", noteId)
            .put("paragraphId", paragraphId)
            .put("appId", appId)
            .put("pkg", pkg);
    connectionManager.broadcast(noteId, msg);
  }

  @Override
  public void onStatusChange(final String noteId,
                             final String paragraphId,
                             final String appId,
                             final String status) {
    final SockMessage msg = new SockMessage(Operation.APP_STATUS_CHANGE)
            .put("noteId", noteId)
            .put("paragraphId", paragraphId)
            .put("appId", appId)
            .put("status", status);
    connectionManager.broadcast(noteId, msg);
  }
}
