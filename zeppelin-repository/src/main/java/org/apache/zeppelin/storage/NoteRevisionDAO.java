package org.apache.zeppelin.storage;

public class NoteRevisionDAO {

//  private final NamedParameterJdbcTemplate jdbcTemplate;
//  private final ParagraphDAO paragraphDAO;
//
//  private static final String INSERT_REVISION = "INSERT INTO revisions (note_id, message, date) values (:note_id, :message, :date);";
//  private static final String GET_ALL_REVISIONS = "SELECT * FROM revisions WHERE note_id=:note_id";
//
//
//  public NoteRevisionDAO(final NamedParameterJdbcTemplate jdbcTemplate, final ParagraphDAO paragraphDAO) {
//    this.jdbcTemplate = jdbcTemplate;
//    this.paragraphDAO = paragraphDAO;
//  }
//
//  void createRevision(final Note note, final String message) {
//
//    if (note.getRevision() != null) {
//      throw new RuntimeException("Can't create revision. The note is in view of another revision.");
//    }
//
//    NoteRevision revision = new NoteRevision(note.getId(), LocalDateTime.now(), message);
//
//    GeneratedKeyHolder holder = new GeneratedKeyHolder();
//    jdbcTemplate.update(INSERT_REVISION, convertNoteRevisionToParameters(revision), holder);
//    revision.setId((Long) holder.getKeys().get("id"));
//
//    paragraphDAO.saveNoteParagraphs(note, revision.getId());
//  }
//
//  void checkoutRevision(final Note note, final Long revisionId) {
//    NoteRevision revision = null;
//    if (revisionId != null) {
//      revision = getRevisions(note.getId()).stream()
//          .filter(r -> r.getId() == revisionId)
//          .findAny().orElse(null);
//    }
//    note.setRevision(revision);
//    note.getParagraphs().clear();
//    note.getParagraphs().addAll(paragraphDAO.getParagraphs(note, revision));
//  }
//
//  void applyRevision(final Note note, final Long revisionId) {
//    checkoutRevision(note, revisionId);
//    note.setRevision(null);
//    paragraphDAO.saveNoteParagraphs(note, null);
//  }
//
//  List<NoteRevision> getRevisions(final long noteId) {
//    return jdbcTemplate
//        .query(GET_ALL_REVISIONS, new MapSqlParameterSource("note_id", noteId),
//            (resultSet, i) -> convertResultSetToRevision(resultSet));
//  }
//
//  private NoteRevision convertResultSetToRevision(final ResultSet resultSet) throws SQLException {
//    long id = resultSet.getLong("id");
//    long noteId = resultSet.getLong("note_id");
//    String message = resultSet.getString("message");
//    LocalDateTime date = resultSet.getTimestamp("date").toLocalDateTime();
//
//    NoteRevision revision = new NoteRevision(noteId, date, message);
//    revision.setId(id);
//    return revision;
//  }
//
//  private MapSqlParameterSource convertNoteRevisionToParameters(final NoteRevision revision) {
//    return new MapSqlParameterSource()
//        .addValue("note_id", revision.getUuid())
//        .addValue("message", revision.getMessage())
//        .addValue("date", revision.getDate());
//  }
}
