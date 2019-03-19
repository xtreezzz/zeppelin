package org.apache.zeppelin.storage;

import com.google.gson.Gson;
import java.sql.SQLException;
import org.postgresql.util.PGobject;

public class Utils {
  private static final Gson gson = new Gson();

  static PGobject generatePGjson(final Object value) {
    try {
      PGobject pgObject = new PGobject();
      pgObject.setType("jsonb");
      pgObject.setValue(gson.toJson(value));
      return pgObject;
    } catch (SQLException e) {
      throw new RuntimeException("Can't generate postgres json", e);
    }
  }
}
