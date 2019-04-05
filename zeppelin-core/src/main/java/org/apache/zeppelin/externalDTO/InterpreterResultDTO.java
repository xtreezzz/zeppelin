package org.apache.zeppelin.externalDTO;

import java.util.LinkedList;
import java.util.List;

public class InterpreterResultDTO {

    public static class Message {
        String type;
        String data;

        public Message(String type, String data) {
            this.type = type;
            this.data = data;
        }
    }

    private String code;
    private List<Message> msg = new LinkedList<>();

    public InterpreterResultDTO() {
    }

    public InterpreterResultDTO(final String code, final List<Message> msg) {
        this.code = code;
        this.msg = msg;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public List<Message> getMsg() {
        return msg;
    }

    public void setMsg(List<Message> msg) {
        this.msg = msg;
    }
}
