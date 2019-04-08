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
package ru.tinkoff.zeppelin.interpreter;

public class PredefinedInterpreterResults {

  public static InterpreterResult INTERPRETER_NOT_FOUND = new InterpreterResult(
          InterpreterResult.Code.ERROR,
          new InterpreterResult.Message(
                  InterpreterResult.Message.Type.TEXT,
                  "Interpreter nof found or not configured"
          )
  );

  public static InterpreterResult INTERPRETER_DISABLED = new InterpreterResult(
          InterpreterResult.Code.ERROR,
          new InterpreterResult.Message(
                  InterpreterResult.Message.Type.TEXT,
                  "Interpreter disabled"
          )
  );

  public static InterpreterResult OPERATION_ABORTED = new InterpreterResult(
          InterpreterResult.Code.ABORTED,
          new InterpreterResult.Message(
                  InterpreterResult.Message.Type.TEXT,
                  "Aborted"
          )
  );

  public static InterpreterResult ERROR_WHILE_INTERPRET = new InterpreterResult(
          InterpreterResult.Code.ERROR,
          new InterpreterResult.Message(
                  InterpreterResult.Message.Type.TEXT,
                  "Unknown error while interpret request"
          )
  );

}
