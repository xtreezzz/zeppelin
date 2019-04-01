package org.apache.zeppelin.interpreter.core;

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
