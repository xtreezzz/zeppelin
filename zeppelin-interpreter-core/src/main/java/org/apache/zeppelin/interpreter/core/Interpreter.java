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


import java.util.Map;

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

  protected Map<String, String> configuration;
  protected String classPath;

  // service methods and fields
  private volatile String sessionUUID;
  public String getSessionUUID() {
    return sessionUUID;
  }
  public void setSessionUUID(final String sessionUUID) {
    this.sessionUUID = sessionUUID;
  }


  public Interpreter() { }

  public abstract boolean isAlive();

  public abstract boolean isOpened();

  /**
   * Opens interpreter. You may want to place your initialize routine here.
   * open() is called only once
   */
  public abstract void open(final Map<String, String> configuration, final String classPath) throws InterpreterException;


  public abstract boolean isReusableForConfiguration(final Map<String, String> configuration);

  /**
   * Optionally implement the canceling routine to abort interpret() method
   */
  public abstract void  cancel() throws InterpreterException;

  /**
   * Closes interpreter. You may want to free your resources up here.
   * close() is called only once
   */
  public abstract void close() throws InterpreterException;

  /**
   * Run code and return result, in synchronous way.
   *
   * @param st statements to run
   */
  public abstract InterpreterResult interpretV2(final String st,
                                       final Map<String, String> noteContext,
                                       final Map<String, String> userContext,
                                       final Map<String, String> configuration);


  /**
   * Dynamic form handling
   * see http://zeppelin.apache.org/docs/dynamicform.html
   *
   * @return FormType.SIMPLE enables simple pattern replacement (eg. Hello ${name=world}),
   * FormType.NATIVE handles form in API
   */
  public abstract FormType getFormType() throws InterpreterException;


  /**
   * Type of interpreter.
   */
  public enum FormType {
    NATIVE, SIMPLE, NONE
  }

}
