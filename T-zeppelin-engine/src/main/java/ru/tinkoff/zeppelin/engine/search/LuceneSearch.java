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
package ru.tinkoff.zeppelin.engine.search;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import ru.tinkoff.zeppelin.storage.NoteDAO;
import ru.tinkoff.zeppelin.storage.ParagraphDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import ru.tinkoff.zeppelin.core.notebook.Note;
import ru.tinkoff.zeppelin.core.notebook.Paragraph;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Search (both, indexing and query) the notebooks using Lucene. Query is thread-safe, as creates
 * new IndexReader every time. Index is thread-safe, as re-uses single IndexWriter, which is
 * thread-safe.
 */

@Lazy(false)
@Component
public class LuceneSearch {
  private static final Logger logger = LoggerFactory.getLogger(LuceneSearch.class);

  private static final String SEARCH_FIELD_TEXT = "contents";
  private static final String SEARCH_FIELD_TITLE = "header";
  private static final String PARAGRAPH = "paragraph";
  private static final String ID_FIELD = "id";

  private Directory directory;
  private IndexWriterConfig indexWriterConfig;
  private IndexWriter indexWriter;

  private final NoteDAO noteDAO;
  private final ParagraphDAO paragraphDAO;

  public LuceneSearch(final NoteDAO noteDAO, final ParagraphDAO paragraphDAO) {
    this.noteDAO = noteDAO;
    this.paragraphDAO = paragraphDAO;
  }

  @Scheduled(fixedDelay = 1 * 60 * 60 * 1000 /* 1 hours */)
  private void index() {
    // it's correct logic!
    close();
    open();

    final Map<Long, Note> idToNote = new HashMap<>();
    final List<Note> notes = noteDAO.getAllNotes();
    for (final Note note : notes) {
      try {
        idToNote.put(note.getId(), note);
        indexNote(note);
      } catch (Exception e) {
        logger.error("Fail to add note to search index", e);
      }
    }

    final List<Paragraph> paragraphs = paragraphDAO.getAll();
    for (final Paragraph paragraph : paragraphs) {
      try {
        indexParagraph(idToNote.get(paragraph.getNoteId()), paragraph);
      } catch (Exception e) {
        logger.error("Fail to add paragraph to search index", e);
      }
    }

    commit();
  }


  public List<Map<String, String>> query(String queryStr) {
    List<Map<String, String>> result = Collections.emptyList();
    try (IndexReader indexReader = DirectoryReader.open(directory)) {
      IndexSearcher indexSearcher = new IndexSearcher(indexReader);
      Analyzer analyzer = new StandardAnalyzer();
      MultiFieldQueryParser parser = new MultiFieldQueryParser(new String[]{SEARCH_FIELD_TEXT, SEARCH_FIELD_TITLE}, analyzer);

      Query query = parser.parse(queryStr);
      logger.debug("Searching for: " + query.toString(SEARCH_FIELD_TEXT));

      SimpleHTMLFormatter htmlFormatter = new SimpleHTMLFormatter();
      Highlighter highlighter = new Highlighter(htmlFormatter, new QueryScorer(query));

      result = doSearch(indexSearcher, query, analyzer, highlighter);
    } catch (IOException e) {
      logger.error("Failed to open index dir {}, make sure indexing finished OK", directory, e);
    } catch (ParseException e) {
      logger.error("Failed to parse query " + queryStr, e);
    }
    return result;
  }

  private List<Map<String, String>> doSearch(
          IndexSearcher searcher,
          Query query,
          Analyzer analyzer,
          Highlighter highlighter) {
    List<Map<String, String>> matchingParagraphs = Lists.newArrayList();
    ScoreDoc[] hits;
    try {
      hits = searcher.search(query, 20).scoreDocs;
      for (int i = 0; i < hits.length; i++) {

        int id = hits[i].doc;
        Document doc = searcher.doc(id);
        String path = doc.get(ID_FIELD);
        if (path != null) {

          String title = doc.get("title");
          String text = doc.get(SEARCH_FIELD_TEXT);
          String header = doc.get(SEARCH_FIELD_TITLE);
          String fragment = "";

          if (text != null) {
            TokenStream tokenStream =
                    TokenSources.getTokenStream(
                            searcher.getIndexReader(), id, SEARCH_FIELD_TEXT, analyzer);
            TextFragment[] frag = highlighter.getBestTextFragments(tokenStream, text, true, 3);
            fragment = (frag != null && frag.length > 0) ? frag[0].toString() : "";
          }

          if (header != null) {
            TokenStream tokenTitle =
                    TokenSources.getTokenStream(
                            searcher.getIndexReader(), id, SEARCH_FIELD_TITLE, analyzer);
            TextFragment[] frgTitle = highlighter.getBestTextFragments(tokenTitle, header, true, 3);
            header = (frgTitle != null && frgTitle.length > 0) ? frgTitle[0].toString() : "";
          } else {
            header = "";
          }
          matchingParagraphs.add(ImmutableMap.of(
                  "id", path,
                  "name", title,
                  "snippet", fragment,
                  "text", text,
                  "header", header));
        }
      }
    } catch (IOException | InvalidTokenOffsetsException e) {
      logger.error("Exception on searching for {}", query, e);
    }
    return matchingParagraphs;
  }


  private void indexNote(final Note note) throws IOException {
    Document doc = new Document();

    doc.add(new StringField(ID_FIELD, note.getUuid(), Field.Store.YES));
    doc.add(new StringField("title", note.getPath(), Field.Store.YES));

    doc.add(new TextField(SEARCH_FIELD_TEXT, note.getName(), Field.Store.YES));
    indexWriter.addDocument(doc);
  }

  private void indexParagraph(final Note note, Paragraph paragraph) throws IOException {
    if (paragraph.getText() == null) {
      return;
    }
    String id = Joiner.on('/').join(note.getUuid(), PARAGRAPH, paragraph.getUuid());

    Document doc = new Document();

    doc.add(new StringField(ID_FIELD, id, Field.Store.YES));
    doc.add(new StringField("title", note.getPath(), Field.Store.YES));
    doc.add(new TextField(SEARCH_FIELD_TEXT, paragraph.getText(), Field.Store.YES));

    if (paragraph.getTitle() != null) {
      doc.add(new TextField(SEARCH_FIELD_TITLE, paragraph.getTitle(), Field.Store.YES));
    }

    indexWriter.addDocument(doc);
  }

  private void commit() {
    try {
      indexWriter.commit();
    } catch (Exception e) {
      logger.error("Failed to .close() the notebook index", e);
    }
  }

  private void open() {
    try {
      this.directory = new RAMDirectory();
      this.indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
      this.indexWriter = new IndexWriter(directory, indexWriterConfig);
    } catch (Exception e) {
      logger.error("Failed to .open() the notebook index", e);
    }
  }

  private void close() {
    try {
      indexWriter.close();
    } catch (Exception e) {
      // SKIP
    }
    try {
      directory.close();
    } catch (Exception e) {
      // SKIP
    }
  }
}
