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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang.StringUtils;
import org.apache.zeppelin.display.AngularObject;
import org.apache.zeppelin.display.AngularObjectRegistry;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.display.Input;
import org.apache.zeppelin.helium.HeliumPackage;
import org.apache.zeppelin.interpreter.*;
import org.apache.zeppelin.interpreter.Interpreter.FormType;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.notebook.core.Paragraph;
import org.apache.zeppelin.resource.ResourcePool;
import org.apache.zeppelin.scheduler.AbstractScheduler;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.scheduler.JobListener;
import org.apache.zeppelin.scheduler.JobWithProgressPoller;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.apache.zeppelin.user.Credentials;
import org.apache.zeppelin.user.UserCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Paragraph execution unit.
 *
 * @see AbstractScheduler#runJob(Job)
 */
public class ParagraphJob extends JobWithProgressPoller<InterpreterResult> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParagraphJob.class);
  private static final Pattern REPL_PATTERN =
      Pattern.compile("(\\s*)%([\\w\\.]+)(\\(.*?\\))?.*", Pattern.DOTALL);

  /**
   * Paragraph data.
   */
  private final Paragraph paragraph;

  //TODO(egorklimov) all below fields are computable in runtime
  /**
   * Interpreter name.
   *
   * example: @spark.pyspark
   */
  private String intpText;

  /**
   * Paragraph source text without interpreter info.
   */
  private String scriptText;

  /**
   * Used for running selected text
   */
  private String selectedText;

  /**
   * Interpreter that was used during paragraph execution. Init by binded interpreter
   *
   * {@link ParagraphJob#getInterpreterContext()}
   */
  private Interpreter interpreter;

  /**
   * Parent note.
   *
   * Needed to extract note forms and interpreter factory
   */
  private Note note;
  private AuthenticationInfo subject;

  /**
   * Personalized mode - e673949c61f4f4c65289ca2d6fc426fb4e90a8b2
   */
  private Map<String, ParagraphJob> userParagraphMap = new HashMap<>();

  /**
   * Interpreter local properties.
   *
   * https://zeppelin.apache.org/docs/0.8.0/interpreter/mahout.html#passing-a-variable-from-mahout-to-r-and-plotting
   * example: %spark.r {"imageWidth": "400px"}
   */
  private Map<String, String> localProperties = new HashMap<>();

  /**
   * https://github.com/apache/zeppelin/pull/3150
   * > For now the runtimes info is only about the spark job info you see in paragraph.
   */
  private Map<String, ParagraphRuntimeInfo> runtimeInfos = new HashMap<>();

  public ParagraphJob(String paragraphId, Note note, JobListener listener) {
    super(paragraphId, generateId(), listener);
    this.note = note;
    this.paragraph = note.getParagraph(paragraphId).getParagraph();
  }

  public ParagraphJob(Note note, Paragraph p, JobListener listener) {
    super(generateId(), listener);
    this.paragraph = p;
    this.note = note;
  }

  // Used for clone ParagraphJob
  public ParagraphJob(ParagraphJob p2) {
    super(p2.getId(), null);
    this.paragraph = p2.getParagraph();
    this.note = p2.note;
    this.setAuthenticationInfo(p2.getAuthenticationInfo());
    setStatus(p2.getStatus());
  }

  private static String generateId() {
    return "paragraph_" + System.currentTimeMillis() + "_" + new SecureRandom().nextInt();
  }

  public Map<String, ParagraphJob> getUserParagraphMap() {
    return userParagraphMap;
  }

  public ParagraphJob getUserParagraph(String user) {
    if (!userParagraphMap.containsKey(user)) {
      cloneParagraphForUser(user);
    }
    return userParagraphMap.get(user);
  }

  @Override
  public void setResult(InterpreterResult result) {
    paragraph.setResults(result);
  }

  public ParagraphJob cloneParagraphForUser(String user) {
    ParagraphJob p = new ParagraphJob(this);
    // reset status to READY when clone ParagraphJob for personalization.
    p.status = Status.READY;
    addUser(p, user);
    return p;
  }

  public void clearUserParagraphs() {
    userParagraphMap.clear();
  }

  public void addUser(ParagraphJob p, String user) {
    userParagraphMap.put(user, p);
  }

  /**
   * Update text and update InterpreterContext.
   * @param newText - full paragraph text.
   */
  public void setText(String newText) {
    paragraph.setText(newText);
    prepareInterpreterContext();
  }

  /**
   * Used only for running selected text.
   * @param text
   */
  public void setSelectedText(String text) {
    this.selectedText = null;
    if (text != null && !text.isEmpty()) {
      Matcher matcher = REPL_PATTERN.matcher(text);
      int skipCount = 0;
      if (matcher.matches()) {
        skipCount += 1;
        for (int i = 1; i <= matcher.groupCount(); i++) {
          skipCount += matcher.group(i) == null ? 0 : matcher.group(i).length();
        }
      }
      this.selectedText = text.substring(skipCount).trim();
    }
  }

  // prepare interpreter context
  public void prepareInterpreterContext() {
    // parse text to get interpreter component
    if (paragraph.getText() != null) {
      // clean localProperties, otherwise previous localProperties will be used for the next run
      this.localProperties.clear();
      Matcher matcher = REPL_PATTERN.matcher(paragraph.getText());
      if (matcher.matches()) {
        String headingSpace = matcher.group(1);
        this.intpText = matcher.group(2);

        if (matcher.groupCount() == 3 && matcher.group(3) != null) {
          String localPropertiesText = matcher.group(3);
          String[] splits = localPropertiesText.substring(1, localPropertiesText.length() -1)
              .split(",");
          for (String split : splits) {
            String[] kv = split.split("=");
            if (StringUtils.isBlank(split) || kv.length == 0) {
              continue;
            }
            if (kv.length > 2) {
              throw new RuntimeException("Invalid paragraph properties format: " + split);
            }
            if (kv.length == 1) {
              localProperties.put(kv[0].trim(), kv[0].trim());
            } else {
              localProperties.put(kv[0].trim(), kv[1].trim());
            }
          }
          this.scriptText = paragraph.getText().substring(headingSpace.length() + intpText.length() +
              localPropertiesText.length() + 1).trim();
        } else {
          this.scriptText = paragraph.getText().substring(headingSpace.length() + intpText.length() + 1).trim();
        }
      } else {
        this.intpText = "";
        this.scriptText = paragraph.getText().trim();
      }
    }
  }

  public AuthenticationInfo getAuthenticationInfo() {
    return subject;
  }

  public void setAuthenticationInfo(AuthenticationInfo subject) {
    this.subject = subject;
    if (subject != null) {
      paragraph.setUser(subject.getUser());
    }
  }

  public Paragraph getParagraph() {
    return paragraph;
  }

  public String getIntpText() {
    return intpText;
  }

  public String getScriptText() {
    return scriptText;
  }

  public void setNote(Note note) {
    this.note = note;
  }

  public Note getNote() {
    return note;
  }

  public Map<String, String> getLocalProperties() {
    return localProperties;
  }

  public boolean isEnabled() {
    Boolean enabled = (Boolean) paragraph.getConfig().get("enabled");
    return enabled == null || enabled.booleanValue();
  }

  public Interpreter getBindedInterpreter() throws InterpreterNotFoundException {
    return this.note.getInterpreterFactory().getInterpreter(paragraph.getUser(), note.getId(),
        intpText, note.getDefaultInterpreterGroup());
  }

  public void setInterpreter(Interpreter interpreter) {
    this.interpreter = interpreter;
  }


  @Override
  public InterpreterResult getReturn() {
    return paragraph.getResults();
  }

  @Override
  public int progress() {
    try {
      if (this.interpreter != null) {
        return this.interpreter.getProgress(getInterpreterContext(null));
      } else {
        return 0;
      }
    } catch (InterpreterException e) {
      throw new RuntimeException("Fail to get progress", e);
    }
  }

  @Override
  public Map<String, Object> info() {
    return null;
  }

  public boolean isBlankParagraph() {
    return Strings.isNullOrEmpty(scriptText);
  }

  public boolean execute(boolean blocking) {
    if (isBlankParagraph()) {
      LOGGER.info("Skip to run blank paragraph. {}", getId());
      setStatus(Job.Status.FINISHED);
      return true;
    }

    try {
      Interpreter interpreter = getBindedInterpreter();
      setStatus(Status.READY);
      if (paragraph.getConfig().get("enabled") == null
          || (Boolean) paragraph.getConfig().get("enabled")) {
        setAuthenticationInfo(getAuthenticationInfo());
        interpreter.getScheduler().submit(this);
      }

      if (blocking) {
        while (!getStatus().isCompleted()) {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        return getStatus() == Status.FINISHED;
      } else {
        return true;
      }
    } catch (InterpreterNotFoundException e) {
      InterpreterResult intpResult =
          new InterpreterResult(InterpreterResult.Code.ERROR);
      setReturn(intpResult, e);
      setStatus(Job.Status.ERROR);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected InterpreterResult jobRun() throws Throwable {
    this.runtimeInfos.clear();
    interpreter = getBindedInterpreter();
    if (interpreter == null) {
      LOGGER.error("Can not find interpreter name " + intpText);
      throw new RuntimeException("Can not find interpreter for " + intpText);
    }
    LOGGER.info("Run paragraph [paragraph_id: {}, interpreter: {}, note_id: {}, user: {}]",
        getId(), interpreter.getClassName(), note.getId(), subject.getUser());
    InterpreterSetting interpreterSetting = ((ManagedInterpreterGroup)
        interpreter.getInterpreterGroup()).getInterpreterSetting();
    if (interpreterSetting != null) {
      interpreterSetting.waitForReady();
    }
    if (paragraph.getUser() != null) {
      if (subject != null && !interpreterSetting.isUserAuthorized(subject.getUsersAndRoles())) {
        String msg = String.format("%s has no permission for %s", subject.getUser(), intpText);
        LOGGER.error(msg);
        return new InterpreterResult(Code.ERROR, msg);
      }
    }

    for (ParagraphJob p : userParagraphMap.values()) {
      p.setText(paragraph.getText());
    }

    String script = this.selectedText == null ? this.scriptText : this.selectedText;
    // inject form
    if (interpreter.getFormType() == FormType.NATIVE) {
      paragraph.getSettings().clear();
    } else if (interpreter.getFormType() == FormType.SIMPLE) {
      // inputs will be built from script body
      LinkedHashMap<String, Input> inputs = Input.extractSimpleQueryForm(script, false);
      LinkedHashMap<String, Input> noteInputs = Input.extractSimpleQueryForm(script, true);
      final AngularObjectRegistry angularRegistry =
          interpreter.getInterpreterGroup().getAngularObjectRegistry();
      String scriptBody = extractVariablesFromAngularRegistry(script, inputs, angularRegistry);

      paragraph.getSettings().setForms(inputs);
      if (!noteInputs.isEmpty()) {
        if (!note.getNoteForms().isEmpty()) {
          Map<String, Input> currentNoteForms =  note.getNoteForms();
          for (String s : noteInputs.keySet()) {
            if (!currentNoteForms.containsKey(s)) {
              currentNoteForms.put(s, noteInputs.get(s));
            }
          }
        } else {
          note.setNoteForms(noteInputs);
        }
      }
      script = Input.getSimpleQuery(note.getNoteParams(), scriptBody, true);
      script = Input.getSimpleQuery(paragraph.getSettings().getParams(), script, false);
    }
    LOGGER.debug("RUN : " + script);
    try {
      InterpreterContext context = getInterpreterContext();
      InterpreterContext.set(context);
      InterpreterResult ret = interpreter.interpret(script, context);

      if (interpreter.getFormType() == FormType.NATIVE) {
        note.setNoteParams(context.getNoteGui().getParams());
        note.setNoteForms(context.getNoteGui().getForms());
      }

      if (Code.KEEP_PREVIOUS_RESULT == ret.code()) {
        return getReturn();
      }

      context.out.flush();
      List<InterpreterResultMessage> resultMessages = context.out.toInterpreterResultMessage();
      resultMessages.addAll(ret.message());
      InterpreterResult res = new InterpreterResult(ret.code(), resultMessages);
      ParagraphJob p = getUserParagraph(paragraph.getUser());
      if (null != p) {
        p.setResult(res);
        p.getParagraph().getSettings().setParams(paragraph.getSettings().getParams());
      }

      LOGGER.info("End of Run paragraph [paragraph_id: {}, interpreter: {}, note_id: {}, user: {}]",
          getId(), intpText, note.getId(), subject.getUser());

      return res;
    } finally {
      InterpreterContext.remove();
    }
  }

  @Override
  protected boolean jobAbort() {
    if (interpreter == null) {
      return true;
    }
    try {
      interpreter.cancel(getInterpreterContext(null));
    } catch (InterpreterException e) {
      throw new RuntimeException(e);
    }

    return true;
  }

  private InterpreterContext getInterpreterContext() {
    final ParagraphJob self = this;

    return getInterpreterContext(
        new InterpreterOutput(
            new InterpreterOutputListener() {
              ParagraphJobListener paragraphJobListener = (ParagraphJobListener) getListener();

              @Override
              public void onAppend(int index, InterpreterResultMessageOutput out, byte[] line) {
                if (null != paragraphJobListener) {
                  paragraphJobListener.onOutputAppend(self, index, new String(line));
                }
              }

              @Override
              public void onUpdate(int index, InterpreterResultMessageOutput out) {
                try {
                  if (null != paragraphJobListener) {
                    paragraphJobListener.onOutputUpdate(
                        self, index, out.toInterpreterResultMessage());
                  }
                } catch (IOException e) {
                  LOGGER.error(e.getMessage(), e);
                }
              }

              @Override
              public void onUpdateAll(InterpreterOutput out) {
                try {
                  List<InterpreterResultMessage> messages = out.toInterpreterResultMessage();
                  if (null != paragraphJobListener) {
                    paragraphJobListener.onOutputUpdateAll(self, messages);
                  }
                  updateParagraphResult(messages);
                } catch (IOException e) {
                  LOGGER.error(e.getMessage(), e);
                }
              }

      private void updateParagraphResult(List<InterpreterResultMessage> msgs) {
        // update paragraph results
        InterpreterResult result = new InterpreterResult(Code.SUCCESS, msgs);
        setReturn(result, null);
      }
    }));
  }

  public InterpreterContext getInterpreterContext(InterpreterOutput output) {
    AngularObjectRegistry registry = null;
    ResourcePool resourcePool = null;

    if (this.interpreter != null) {
      registry = this.interpreter.getInterpreterGroup().getAngularObjectRegistry();
      resourcePool = this.interpreter.getInterpreterGroup().getResourcePool();
    }

    Credentials credentials = note.getCredentials();
    if (subject != null) {
      UserCredentials userCredentials =
          credentials.getUserCredentials(subject.getUser());
      subject.setUserCredentials(userCredentials);
    }

    InterpreterContext interpreterContext =
        InterpreterContext.builder()
            .setNoteId(note.getId())
            .setNoteName(note.getName())
            .setParagraphId(getId())
            .setReplName(intpText)
            .setParagraphTitle(paragraph.getTitle())
            .setParagraphText(paragraph.getText())
            .setAuthenticationInfo(subject)
            .setLocalProperties(localProperties)
            .setConfig(paragraph.getConfig())
            .setGUI(paragraph.getSettings())
            .setNoteGUI(getNoteGui())
            .setAngularObjectRegistry(registry)
            .setResourcePool(resourcePool)
            .setInterpreterOut(output)
            .build();
    return interpreterContext;
  }

  public void setStatusToUserParagraph(Status status) {
    String user = paragraph.getUser();
    if (null != user) {
      getUserParagraph(paragraph.getUser()).setStatus(status);
    }
  }

  public void setReturn(InterpreterResult value, Throwable t) {
    setResult(value);
    setException(t);
  }

  private String getApplicationId(HeliumPackage pkg) {
    return "app_" + getNote().getId() + "-" + getId() + pkg.getName().replaceAll("\\.", "_");
  }

  public ApplicationState createOrGetApplicationState(HeliumPackage pkg) {
    synchronized (paragraph) {
      for (ApplicationState as : paragraph.getApps()) {
        //TODO(egorklimov): fix this, equals always false
        if (as.equals(pkg)) {
          return as;
        }
      }

      String appId = getApplicationId(pkg);
      ApplicationState appState = new ApplicationState(appId, pkg);
      paragraph.addApplicationState(appState);
      return appState;
    }
  }

  String extractVariablesFromAngularRegistry(String scriptBody, Map<String, Input> inputs,
      AngularObjectRegistry angularRegistry) {

    final String noteId = this.getNote().getId();
    final String paragraphId = this.getId();

    final Set<String> keys = new HashSet<>(inputs.keySet());

    for (String varName : keys) {
      final AngularObject paragraphScoped = angularRegistry.get(varName, noteId, paragraphId);
      final AngularObject noteScoped = angularRegistry.get(varName, noteId, null);
      final AngularObject angularObject = paragraphScoped != null ? paragraphScoped : noteScoped;
      if (angularObject != null) {
        inputs.remove(varName);
        final String pattern = "[$][{]\\s*" + varName + "\\s*(?:=[^}]+)?[}]";
        scriptBody = scriptBody.replaceAll(pattern, angularObject.get().toString());
      }
    }
    return scriptBody;
  }

  public boolean isValidInterpreter(String replName) {
    try {
      return note.getInterpreterFactory().getInterpreter(paragraph.getUser(), note.getId(), replName,
          note.getDefaultInterpreterGroup()) != null;
    } catch (InterpreterNotFoundException e) {
      return false;
    }
  }

  public void updateRuntimeInfos(String label, String tooltip, Map<String, String> infos,
      String group, String intpSettingId) {
    if (this.runtimeInfos == null) {
      this.runtimeInfos = new HashMap<>();
    }

    if (infos != null) {
      for (String key : infos.keySet()) {
        ParagraphRuntimeInfo info = this.runtimeInfos.get(key);
        if (info == null) {
          info = new ParagraphRuntimeInfo(key, label, tooltip, group, intpSettingId);
          this.runtimeInfos.put(key, info);
        }
        info.addValue(infos.get(key));
      }
    }
  }

  public Map<String, ParagraphRuntimeInfo> getRuntimeInfos() {
    return runtimeInfos;
  }

  public void cleanRuntimeInfos() {
    this.runtimeInfos.clear();
  }

  private GUI getNoteGui() {
    GUI gui = new GUI();
    gui.setParams(this.note.getNoteParams());
    gui.setForms(this.note.getNoteForms());
    return gui;
  }

}
