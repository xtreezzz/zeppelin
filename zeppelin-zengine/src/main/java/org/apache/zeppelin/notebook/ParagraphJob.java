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

package org.apache.zeppelin.notebook;

import com.google.common.base.Strings;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.zeppelin.helium.HeliumPackage;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.notebook.conf.ParagraphJobContext;
import org.apache.zeppelin.scheduler.Job.Status;
import org.apache.zeppelin.scheduler.JobListener;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Paragraph execution unit.
 */
//TODO(egorklimov):
// * Убрал extends JobWithProgressPoller<InterpreterResult>, т.к. нужно
//    переписать логику исполнения
// * Убрал все что связано с personalizedMode, нужный вывод параграфа должен выдаваться
//    InterpreterResultService'ом
// * Убрал runtimeInfos:
//     * https://github.com/apache/zeppelin/pull/3150
//     * > For now the runtimes info is only about the spark job info you see in paragraph.
// * Убрал Interpreter и Note, т.к. они использовались для запуска
public class ParagraphJob {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParagraphJob.class);

  /**
   * Paragraph data.
   */
  private final org.apache.zeppelin.notebook.core.Paragraph paragraph;

  private boolean isEnabled;

  // contains all runtime paragraphJob info
  private ParagraphJobContext context;

  private AuthenticationInfo subject;

  public ParagraphJob(final String paragraphId, final Note note, final JobListener listener) {
    //super(paragraphId, generateId(), listener);
    this.paragraph = note.getParagraph(paragraphId);
    this.context = new ParagraphJobContext(paragraph.getText());
  }

  public ParagraphJob(final Note note, final org.apache.zeppelin.notebook.core.Paragraph p, final JobListener listener) {
    //super(generateId(), listener);
    this.paragraph = p;
    this.context = new ParagraphJobContext(paragraph.getText());
  }

  //  @Override
  //  public void setResult(InterpreterResult result) {
  //    // paragraph.setResults(result);
  //    throw new NotImplementedException("InterpreterResultService is not created yet");
  //  }

  public ParagraphJob cloneParagraphForUser(final String user) {
    throw new NotImplementedException("Clone logic should be refactored");
  }

  /**
   * Update text and update InterpreterContext.
   * @param newText - full paragraph text.
   */
  public void setText(final String newText) {
    paragraph.setText(newText);
    context.updateContext(newText);
  }

  public AuthenticationInfo getAuthenticationInfo() {
    return subject;
  }

  public void setAuthenticationInfo(final AuthenticationInfo subject) {
    this.subject = subject;
    if (subject != null) {
      paragraph.setUser(subject.getUser());
    }
  }

  public org.apache.zeppelin.notebook.core.Paragraph getParagraph() {
    return paragraph;
  }

  public ParagraphJobContext getContext() {
    return context;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public boolean isBlankParagraph() {
    return Strings.isNullOrEmpty(context.getScriptText());
  }

  private InterpreterContext getInterpreterContext() {
    throw new NotImplementedException("Execution logic should be fixed");
    //    final Paragraph self = this;
    //
    //    return getInterpreterContext(
    //        new InterpreterOutput(
    //            new InterpreterOutputListener() {
    //              ParagraphJobListener paragraphJobListener = (ParagraphJobListener) getListener();
    //
    //              @Override
    //              public void onAppend(int index, InterpreterResultMessageOutput out, byte[] line) {
    //                if (null != paragraphJobListener) {
    //                  paragraphJobListener.onOutputAppend(self, index, new String(line));
    //                }
    //              }
    //
    //              @Override
    //              public void onUpdate(int index, InterpreterResultMessageOutput out) {
    //                try {
    //                  if (null != paragraphJobListener) {
    //                    paragraphJobListener.onOutputUpdate(
    //                        self, index, out.toInterpreterResultMessage());
    //                  }
    //                } catch (IOException e) {
    //                  LOGGER.error(e.getMessage(), e);
    //                }
    //              }
    //
    //              @Override
    //              public void onUpdateAll(InterpreterOutput out) {
    //                try {
    //                  List<InterpreterResultMessage> messages = out.toInterpreterResultMessage();
    //                  if (null != paragraphJobListener) {
    //                    paragraphJobListener.onOutputUpdateAll(self, messages);
    //                  }
    //                  updateParagraphResult(messages);
    //                } catch (IOException e) {
    //                  LOGGER.error(e.getMessage(), e);
    //                }
    //              }
    //
    //      private void updateParagraphResult(List<InterpreterResultMessage> msgs) {
    //        // update paragraph results
    //        InterpreterResult result = new InterpreterResult(Code.SUCCESS, msgs);
    //        setReturn(result, null);
    //      }
    //    }));
  }

  public InterpreterContext getInterpreterContext(final InterpreterOutput output) {
    throw new NotImplementedException("Execution logic should be fixed");

    //    AngularObjectRegistry registry = null;
    //    ResourcePool resourcePool = null;
    //
    //    if (this.interpreter != null) {
    //      registry = this.interpreter.getInterpreterGroup().getAngularObjectRegistry();
    //      resourcePool = this.interpreter.getInterpreterGroup().getResourcePool();
    //    }
    //
    //    Credentials credentials = note.getCredentials();
    //    if (subject != null) {
    //      UserCredentials userCredentials =
    //          credentials.getUserCredentials(subject.getUser());
    //      subject.setUserCredentials(userCredentials);
    //    }
    //
    //    InterpreterContext interpreterContext =
    //        InterpreterContext.builder()
    //            .setNoteId(note.getId())
    //            .setNoteName(note.getName())
    //            .setParagraphId(getId())
    //            .setReplName(intpText)
    //            .setParagraphTitle(paragraph.getTitle())
    //            .setParagraphText(paragraph.getText())
    //            .setAuthenticationInfo(subject)
    //            .setLocalProperties(localProperties)
    //            .setGUI(paragraph.getSettings())
    //            .setNoteGUI(getNoteGui())
    //            .setAngularObjectRegistry(registry)
    //            .setResourcePool(resourcePool)
    //            .setInterpreterOut(output)
    //            .build();
    //    return interpreterContext;
  }

  public void setStatusToUserParagraph(final Status status) {
    throw new NotImplementedException("Status should be updated by InterpreterResultService");
  }

  public void setReturn(final InterpreterResult value, final Throwable t) {
    throw new NotImplementedException("Result should be updated by InterpreterResultService");
  }

  private String getApplicationId(final HeliumPackage pkg) {
    throw new NotImplementedException("Helium properties should be obtained by HeliumService");
  }

  public ApplicationState createOrGetApplicationState(final HeliumPackage pkg) {
    //    synchronized (paragraph) {
    //      for (ApplicationState as : paragraph.getApps()) {
    //        //TODO(egorklimov): fix this, equals always false
    //        if (as.equals(pkg)) {
    //          return as;
    //        }
    //      }
    //
    //      String appId = getApplicationId(pkg);
    //      ApplicationState appState = new ApplicationState(appId, pkg);
    //      paragraph.addApplicationState(appState);
    //      return appState;
    //    }
    throw new NotImplementedException("HeliumService is not implemented yet");
  }
}
