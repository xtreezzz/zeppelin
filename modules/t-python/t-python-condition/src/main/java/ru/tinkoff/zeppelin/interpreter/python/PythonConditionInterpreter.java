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
package ru.tinkoff.zeppelin.interpreter.python;

import ru.tinkoff.zeppelin.interpreter.InterpreterResult;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("unused")
public class PythonConditionInterpreter extends AbstractPythonInterpreter {

  public PythonConditionInterpreter() {
    super();
  }

  private volatile AtomicBoolean interrupted = new AtomicBoolean(false);

  @Override
  public void cancel() {
    try {
      super.cancel();
    } catch (final Exception e) {
      //SKIP
    }
    interrupted.set(true);
  }

  @Override
  public void close() {
    try {
      super.close();
    } catch (final Exception e) {
      //SKIP
    }

    interrupted.set(true);
  }

  @Override
  public InterpreterResult interpretV2(String st,
                                       Map<String, String> noteContext,
                                       Map<String, String> userContext,
                                       Map<String, String> configuration) {
    final int maxLife = Integer.parseInt(configuration.get("python.condition.timeout"));
    final int cycleTime = Integer.parseInt(configuration.get("python.condition.cycle.timeout"));
    final int waitExitCode = Integer.parseInt(configuration.get("python.condition.wait.exit.code"));

    interrupted.set(false);

    final long startTime = java.time.Instant.now().getEpochSecond();
    final List<InterpreterResult.Message> messages = new LinkedList<>();
    while (java.time.Instant.now().getEpochSecond() < startTime + maxLife && !interrupted.get()) {
      final PythonInterpreterResult result = super.execute(st, noteContext, userContext, configuration);

      // override messages
      messages.addAll(result.getInterpreterResult().message());
      result.getInterpreterResult().message().clear();
      result.getInterpreterResult().message().addAll(messages);
      if (result.getExitCode() != waitExitCode) {
        return result.getInterpreterResult();
      }

      // publish tempText
      final StringBuilder tempText = new StringBuilder();
      messages.stream()
              .filter(m -> m.getType() == InterpreterResult.Message.Type.TEXT)
              .forEach(m -> tempText.append(m.getData()).append("\n"));
      getTempTextPublisher().accept(tempText.toString());

      int cycles = 0;
      while (cycles <= cycleTime && !interrupted.get()) {
        try {
          Thread.sleep(1000);
        } catch (final Exception e) {
          // SKIP
        }
        cycles++;
      }
    }

    messages.add(new InterpreterResult.Message(
            InterpreterResult.Message.Type.TEXT,
            "Killed by user/timeout")
    );

    final InterpreterResult result = new InterpreterResult(InterpreterResult.Code.ERROR);
    result.message().addAll(messages);
    return result;
  }
}
