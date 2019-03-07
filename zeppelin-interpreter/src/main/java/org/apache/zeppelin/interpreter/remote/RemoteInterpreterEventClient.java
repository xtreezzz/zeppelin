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
package org.apache.zeppelin.interpreter.remote;

import com.google.gson.Gson;
import org.apache.thrift.TException;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResultMessage;
import org.apache.zeppelin.interpreter.thrift.*;
import org.apache.zeppelin.notebook.display.AngularObject;
import org.apache.zeppelin.notebook.display.AngularObjectRegistryListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is used to communicate with ZeppelinServer via thrift.
 * All the methods are synchronized because thrift client is not thread safe.
 */
public class RemoteInterpreterEventClient implements AngularObjectRegistryListener {
  private final Logger LOGGER = LoggerFactory.getLogger(RemoteInterpreterEventClient.class);
  private final Gson gson = new Gson();

  private RemoteInterpreterEventService.Client intpEventServiceClient;
  private String intpGroupId;

  public RemoteInterpreterEventClient(RemoteInterpreterEventService.Client intpEventServiceClient) {
    this.intpEventServiceClient = intpEventServiceClient;
  }

  public void setIntpGroupId(String intpGroupId) {
    this.intpGroupId = intpGroupId;
  }


  public synchronized void onInterpreterOutputAppend(
      String noteId, String paragraphId, int outputIndex, String output) {
    try {
      intpEventServiceClient.appendOutput(
          new OutputAppendEvent(noteId, paragraphId, outputIndex, output, null));
    } catch (TException e) {
      LOGGER.warn("Fail to appendOutput", e);
    }
  }

  public synchronized void onInterpreterOutputUpdate(
      String noteId, String paragraphId, int outputIndex,
      InterpreterResult.Type type, String output) {
    try {
      intpEventServiceClient.updateOutput(
          new OutputUpdateEvent(noteId, paragraphId, outputIndex, type.name(), output, null));
    } catch (TException e) {
      LOGGER.warn("Fail to updateOutput", e);
    }
  }

  public synchronized void onInterpreterOutputUpdateAll(
      String noteId, String paragraphId, List<InterpreterResultMessage> messages) {
    try {
      intpEventServiceClient.updateAllOutput(
          new OutputUpdateAllEvent(noteId, paragraphId, convertToThrift(messages)));
    } catch (TException e) {
      LOGGER.warn("Fail to updateAllOutput", e);
    }
  }

  private List<org.apache.zeppelin.interpreter.thrift.RemoteInterpreterResultMessage>
        convertToThrift(List<InterpreterResultMessage> messages) {
    List<org.apache.zeppelin.interpreter.thrift.RemoteInterpreterResultMessage> thriftMessages =
        new ArrayList<>();
    for (InterpreterResultMessage message : messages) {
      thriftMessages.add(
          new org.apache.zeppelin.interpreter.thrift.RemoteInterpreterResultMessage(
              message.getType().name(), message.getData()));
    }
    return thriftMessages;
  }

  public synchronized void runParagraphs(String noteId,
                                         List<String> paragraphIds,
                                         List<Integer> paragraphIndices,
                                         String curParagraphId) {
    RunParagraphsEvent event =
        new RunParagraphsEvent(noteId, paragraphIds, paragraphIndices, curParagraphId);
    try {
      intpEventServiceClient.runParagraphs(event);
    } catch (TException e) {
      LOGGER.warn("Fail to runParagraphs: " + event, e);
    }
  }

  public synchronized void onAppOutputAppend(
      String noteId, String paragraphId, int index, String appId, String output) {
    AppOutputAppendEvent event =
        new AppOutputAppendEvent(noteId, paragraphId, appId, index, output);
    try {
      intpEventServiceClient.appendAppOutput(event);
    } catch (TException e) {
      LOGGER.warn("Fail to appendAppOutput: " + event, e);
    }
  }


  public synchronized void onAppOutputUpdate(
      String noteId, String paragraphId, int index, String appId,
      InterpreterResult.Type type, String output) {
    AppOutputUpdateEvent event =
        new AppOutputUpdateEvent(noteId, paragraphId, appId, index, type.name(), output);
    try {
      intpEventServiceClient.updateAppOutput(event);
    } catch (TException e) {
      LOGGER.warn("Fail to updateAppOutput: " + event, e);
    }
  }

  public synchronized void onAppStatusUpdate(String noteId, String paragraphId, String appId,
                                             String status) {
    AppStatusUpdateEvent event = new AppStatusUpdateEvent(noteId, paragraphId, appId, status);
    try {
      intpEventServiceClient.updateAppStatus(event);
    } catch (TException e) {
      LOGGER.warn("Fail to updateAppStatus: " + event, e);
    }
  }

  public synchronized void onParaInfosReceived(Map<String, String> infos) {
    try {
      intpEventServiceClient.sendParagraphInfo(intpGroupId, gson.toJson(infos));
    } catch (TException e) {
      LOGGER.warn("Fail to onParaInfosReceived: " + infos, e);
    }
  }

  @Override
  public synchronized void onAdd(String interpreterGroupId, AngularObject object) {
    try {
      intpEventServiceClient.addAngularObject(intpGroupId, object.toJson());
    } catch (TException e) {
      LOGGER.warn("Fail to add AngularObject: " + object, e);
    }
  }

  @Override
  public synchronized void onUpdate(String interpreterGroupId, AngularObject object) {
    try {
      intpEventServiceClient.updateAngularObject(intpGroupId, object.toJson());
    } catch (TException e) {
      LOGGER.warn("Fail to update AngularObject: " + object, e);
    }
  }

  @Override
  public synchronized void onRemove(String interpreterGroupId, String name, String noteId,
                                    String paragraphId) {
    try {
      intpEventServiceClient.removeAngularObject(intpGroupId, noteId, paragraphId, name);
    } catch (TException e) {
      LOGGER.warn("Fail to remove AngularObject", e);
    }
  }
}
