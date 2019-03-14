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

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Interpreter result template.
 */
public class InterpreterResult implements Serializable {
  transient Logger logger = LoggerFactory.getLogger(InterpreterResult.class);
  private static final Gson gson = new Gson();

  /**
   *  Type of result after code execution.
   */
  public enum Code {
    SUCCESS,
    INCOMPLETE,
    ERROR,
    KEEP_PREVIOUS_RESULT
  }

  /**
   * Type of Data.
   */
  public enum Type {
    TEXT,
    HTML,
    ANGULAR,
    TABLE,
    IMG,
    SVG,
    NULL,
    NETWORK
  }

  Code code;
  List<Message> msg = new LinkedList<>();

  public InterpreterResult(Code code) {
    this.code = code;
  }

  public InterpreterResult(Code code, List<Message> msgs) {
    this.code = code;
    msg.addAll(msgs);
  }

  public InterpreterResult(Code code, String msg) {
    this.code = code;
    add(msg);
  }

  public InterpreterResult(Code code, Type type, String msg) {
    this.code = code;
    add(type, msg);
  }

  /**
   * Automatically detect %[display_system] directives
   * @param msg
   */
  public void add(String msg) {
   /* InterpreterOutput out = new InterpreterOutput(null);
    try {
      out.write(msg);
      out.flush();
      this.msg.addAll(out.toInterpreterResultMessage());
      out.close();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
    }
    */

  }

  public void add(Type type, String data) {
    msg.add(new Message(type, data));
  }

  public void add(Message message) {
    msg.add(message);
  }

  public Code code() {
    return code;
  }

  public List<Message> message() {
    return msg;
  }


  public String toString() {
    StringBuilder sb = new StringBuilder();
    Type prevType = null;
    for (Message m : msg) {
      if (prevType != null) {
        sb.append("\n");
        if (prevType == Type.TABLE) {
          sb.append("\n");
        }
      }
      sb.append(m.toString());
      prevType = m.getType();
    }

    return sb.toString();
  }

  /**
   * Interpreter result message
   */
  public static class Message implements Serializable {

    public static final String EXCEEDS_LIMIT_ROWS =
            "<strong>Output is truncated</strong> to %s rows. Learn more about <strong>%s</strong>";
    public static final String EXCEEDS_LIMIT_SIZE =
            "<strong>Output is truncated</strong> to %s bytes. Learn more about <strong>%s</strong>";
    public static final String EXCEEDS_LIMIT =
            "<div class=\"result-alert alert-warning\" role=\"alert\">" +
                    "<button type=\"button\" class=\"close\" data-dismiss=\"alert\" aria-label=\"Close\">" +
                    "<span aria-hidden=\"true\">&times;</span></button>" +
                    "%s" +
                    "</div>";

    InterpreterResult.Type type;
    String data;

    public Message(InterpreterResult.Type type, String data) {
      this.type = type;
      this.data = data;
    }

    public InterpreterResult.Type getType() {
      return type;
    }

    public String getData() {
      return data;
    }

    public static Message getExceedsLimitRowsMessage(int amount, String variable) {
      Message message = new Message(InterpreterResult.Type.HTML,
              String.format(EXCEEDS_LIMIT, String.format(EXCEEDS_LIMIT_ROWS, amount, variable)));
      return message;
    }

    public static Message getExceedsLimitSizeMessage(int amount, String variable) {
      Message message = new Message(InterpreterResult.Type.HTML,
              String.format(EXCEEDS_LIMIT, String.format(EXCEEDS_LIMIT_SIZE, amount, variable)));
      return message;
    }

    public String toString() {
      return "%" + type.name().toLowerCase() + " " + data;
    }
  }
}