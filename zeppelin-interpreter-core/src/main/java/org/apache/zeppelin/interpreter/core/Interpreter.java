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

package org.apache.zeppelin.interpreter.core;


import java.net.URL;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for interpreters.
 * If you want to implement new Zeppelin interpreter, extend this class
 *
 * Please see,
 * https://zeppelin.apache.org/docs/latest/development/writingzeppelininterpreter.html
 *
 * open(), close(), interpret() is three the most important method you need to implement.
 * cancel(), getProgress(), completion() is good to have
 * getFormType(), getScheduler() determine Zeppelin's behavior
 */
public abstract class Interpreter {

  /**
   * Opens interpreter. You may want to place your initialize routine here.
   * open() is called only once
   */
  public abstract void open() throws InterpreterException;

  /**
   * Closes interpreter. You may want to free your resources up here.
   * close() is called only once
   */
  public abstract void close() throws InterpreterException;

  /**
   * Run precode if exists.
   */
  public InterpreterResult executePrecode(InterpreterContext interpreterContext)
      throws InterpreterException {
    String simpleName = this.getClass().getSimpleName();
    String precode = getProperty(String.format("zeppelin.%s.precode", simpleName));
    if (StringUtils.isNotBlank(precode)) {
      return interpret(precode, interpreterContext);
    }
    return null;
  }

  /*
  protected String interpolate(String cmd, ResourcePool resourcePool) {
    Pattern zVariablePattern = Pattern.compile("([^{}]*)([{]+[^{}]*[}]+)(.*)", Pattern.DOTALL);
    StringBuilder sb = new StringBuilder();
    Matcher m;
    String st = cmd;
    while ((m = zVariablePattern.matcher(st)).matches()) {
      sb.append(m.group(1));
      String varPat = m.group(2);
      if (varPat.matches("[{][^{}]+[}]")) {
        // substitute {variable} only if 'variable' has a value ...
        Resource resource = resourcePool.get(varPat.substring(1, varPat.length() - 1));
        Object variableValue = resource == null ? null : resource.get();
        if (variableValue != null)
          sb.append(variableValue);
        else
          return cmd;
      } else if (varPat.matches("[{]{2}[^{}]+[}]{2}")) {
        // escape {{text}} ...
        sb.append("{").append(varPat.substring(2, varPat.length() - 2)).append("}");
      } else {
        // mismatched {{ }} or more than 2 braces ...
        return cmd;
      }
      st = m.group(3);
    }
    sb.append(st);
    return sb.toString();
  }
*/

  /**
   * Run code and return result, in synchronous way.
   *
   * @param st statements to run
   */
  public abstract InterpreterResult interpret(String st, InterpreterContext context)
      throws InterpreterException;

  /**
   * Optionally implement the canceling routine to abort interpret() method
   */
  public abstract void cancel(InterpreterContext context) throws InterpreterException;

  /**
   * Dynamic form handling
   * see http://zeppelin.apache.org/docs/dynamicform.html
   *
   * @return FormType.SIMPLE enables simple pattern replacement (eg. Hello ${name=world}),
   * FormType.NATIVE handles form in API
   */
  public abstract FormType getFormType() throws InterpreterException;

  /**
   * get interpret() method running process in percentage.
   *
   * @return number between 0-100
   */
  public abstract int getProgress(InterpreterContext context) throws InterpreterException;

  /**
   * Get completion list based on cursor position.
   * By implementing this method, it enables auto-completion.
   *
   * @param buf statements
   * @param cursor cursor position in statements
   * @param interpreterContext
   * @return list of possible completion. Return empty list if there're nothing to return.
   */
//  public List<InterpreterCompletion> completion(String buf, int cursor, InterpreterContext interpreterContext) throws InterpreterException {
//    return null;
//  }

  public static Logger logger = LoggerFactory.getLogger(Interpreter.class);
  private URL[] classloaderUrls;
  protected Properties properties;
  protected String userName;

  public Interpreter(Properties properties) {
    this.properties = properties;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }

  public Properties getProperties() {
    Properties p = new Properties();
    p.putAll(properties);
    replaceContextParameters(p);
    return p;
  }

  public String getProperty(String key) {
    logger.debug("key: {}, value: {}", key, getProperties().getProperty(key));

    return getProperties().getProperty(key);
  }

  public String getProperty(String key, String defaultValue) {
    logger.debug("key: {}, value: {}", key, getProperties().getProperty(key, defaultValue));

    return getProperties().getProperty(key, defaultValue);
  }

  public void setProperty(String key, String value) {
    properties.setProperty(key, value);
  }

  public String getClassName() {
    return this.getClass().getName();
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getUserName() {
    return this.userName;
  }

 // public void setInterpreterGroup(InterpreterGroup interpreterGroup) {
 //   this.interpreterGroup = interpreterGroup;
 // }

  //public InterpreterGroup getInterpreterGroup() {
  //  return this.interpreterGroup;
  //}

  public URL[] getClassloaderUrls() {
    return classloaderUrls;
  }

  public void setClassloaderUrls(URL[] classloaderUrls) {
    this.classloaderUrls = classloaderUrls;
  }

  /**
   * Replace markers #{contextFieldName} by values from {@link InterpreterContext} fields
   * with same name and marker #{user}. If value == null then replace by empty string.
   */
  private void replaceContextParameters(Properties properties) {
    /*
    InterpreterContext interpreterContext = InterpreterContext.get();
    if (interpreterContext != null) {
      String markerTemplate = "#\\{%s\\}";
      List<String> skipFields = Arrays.asList("paragraphTitle", "paragraphId", "paragraphText");
      List typesToProcess = Arrays.asList(String.class, Double.class, Float.class, Short.class,
          Byte.class, Character.class, Boolean.class, Integer.class, Long.class);
      for (String key : properties.stringPropertyNames()) {
        String p = properties.getProperty(key);
        if (StringUtils.isNotEmpty(p)) {
          for (Field field : InterpreterContext.class.getDeclaredFields()) {
            Class clazz = field.getType();
            if (!skipFields.contains(field.getName()) && (typesToProcess.contains(clazz)
                || clazz.isPrimitive())) {
              Object value = null;
              try {
                value = FieldUtils.readField(field, interpreterContext, true);
              } catch (Exception e) {
                logger.error("Cannot read value of field {0}", field.getName());
              }
              p = p.replaceAll(String.format(markerTemplate, field.getName()),
                  value != null ? value.toString() : StringUtils.EMPTY);
            }
          }
          p = p.replaceAll(String.format(markerTemplate, "user"),
              StringUtils.defaultString(userName, StringUtils.EMPTY));
          properties.setProperty(key, p);
        }
      }
    }
    */
  }

  /**
   * Type of interpreter.
   */
  public enum FormType {
    NATIVE, SIMPLE, NONE
  }

}
