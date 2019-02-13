package org.apache.zeppelin.notebook.repo.converter.notebook;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.apache.zeppelin.display.GUI;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.notebook.Paragraph;
import org.apache.zeppelin.notebook.repo.api.dto.display.GUIDTO;
import org.apache.zeppelin.notebook.repo.api.dto.interpreter.InterpreterResultDTO;
import org.apache.zeppelin.notebook.repo.api.dto.notebook.ParagraphDTO;
import org.apache.zeppelin.user.AuthenticationInfo;
import org.junit.Test;

public class ParagraphConverterTest {

  @Test
  public void convertToDTO() {
    Date date = new Date();
    ParagraphDTO dto = new ParagraphDTO("title", "text", "user", date,
        new HashMap<>(), new GUIDTO(new HashMap<>(), new HashMap<>()), new InterpreterResultDTO(
        Code.SUCCESS.name(), new ArrayList<>()), new ArrayList<>());
    Paragraph original = new Paragraph(null, null);
    original.setApps(new ArrayList<>());
    original.setAuthenticationInfo(new AuthenticationInfo("user"));
    original.setDateUpdated(date);
    original.setResult(new InterpreterResult(Code.SUCCESS, new ArrayList<>()));
    original.settings = new GUI();
    original.setTitle("title");
    original.setText("text");

    assertEquals(dto, ParagraphConverter.convertToDTO(original));
  }

  @Test
  public void convertFromDTOToObject() {
  }
}