package org.apache.zeppelin.rest;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.zeppelin.JDBCCompleter;
import org.junit.Test;

public class CompletionRestApiTest {

  @Test
  public void testTreeMap() {
    try {
      final JDBCCompleter completer =
          new JDBCCompleter("jdbc:postgresql://localhost:5432/zeppelin", "jdbc", "pass", "org.postgresql.Driver");
      System.out.println(
          completer.complete("select public.", "select public.".length() - 1));
    } catch (Exception e) {
      System.out.println(ExceptionUtils.getStackTrace(e));
    }
  }

}