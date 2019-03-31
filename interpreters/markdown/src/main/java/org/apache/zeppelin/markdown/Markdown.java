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

package org.apache.zeppelin.markdown;

import org.apache.zeppelin.interpreter.core.Interpreter;
import org.apache.zeppelin.interpreter.core.InterpreterException;
import org.apache.zeppelin.interpreter.core.InterpreterResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * MarkdownInterpreter interpreter for Zeppelin.
 */
public class Markdown extends Interpreter {
    private static final Logger LOGGER = LoggerFactory.getLogger(Markdown.class);

    private String parserType;
    private MarkdownParser parser;


    private static final String MARKDOWN_PARSER_TYPE = "markdown.parser.type";
    private static final String PARSER_TYPE_PEGDOWN = "pegdown";
    private static final String PARSER_TYPE_MARKDOWN4J = "markdown4j";

    public Markdown() {
        super();
    }


    @Override
    public boolean isAlive() {
        return parserType != null && parser != null;
    }

    @Override
    public boolean isOpened() {
        return parserType != null && parser != null;
    }

    @Override
    public void open(final Map<String, String> configuration, final String classPath) throws InterpreterException {
        this.configuration = configuration;
        this.classPath = classPath;
        this.parserType = configuration.get(MARKDOWN_PARSER_TYPE);

        switch (parserType) {
            case PARSER_TYPE_MARKDOWN4J:
                parser = new Markdown4jParser();
                break;
            case PARSER_TYPE_PEGDOWN:
                parser = new PegdownParser();
                break;
            default:
                throw new IllegalArgumentException("");
        }
    }

    @Override
    public boolean isReusableForConfiguration(final Map<String, String> configuration) {
        return this.parserType.equals(configuration.get(MARKDOWN_PARSER_TYPE));
    }

    @Override
    public void cancel() throws InterpreterException {

    }

    @Override
    public void close() throws InterpreterException {

    }

    @Override
    public InterpreterResult interpretV2(final String st,
                                         final Map<String, String> noteContext,
                                         final Map<String, String> userContext,
                                         final Map<String, String> configuration) {
        try {
            final String html = parser.render(st);
            final InterpreterResult result = new InterpreterResult(InterpreterResult.Code.SUCCESS);
            result.message().add(new InterpreterResult.Message(InterpreterResult.Message.Type.HTML, html));
            return result;
        } catch (RuntimeException e) {
            LOGGER.error("Exception in MarkdownInterpreter while interpret ", e);
            final InterpreterResult result = new InterpreterResult(InterpreterResult.Code.ERROR);
            result.message().add(new InterpreterResult.Message(InterpreterResult.Message.Type.TEXT, ""));
            return result;
        }
    }

    @Override
    public FormType getFormType() {
        return FormType.SIMPLE;
    }

}