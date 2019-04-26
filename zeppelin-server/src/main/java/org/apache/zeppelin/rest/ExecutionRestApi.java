//package org.apache.zeppelin.rest;
//
//import java.io.IOException;
//import java.util.HashMap;
//import java.util.Map;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.ResponseEntity;
//import org.springframework.web.bind.annotation.DeleteMapping;
//import org.springframework.web.bind.annotation.GetMapping;
//import org.springframework.web.bind.annotation.PathVariable;
//import org.springframework.web.bind.annotation.PostMapping;
//import org.springframework.web.bind.annotation.RequestParam;
//import ru.tinkoff.zeppelin.core.notebook.Note;
//import ru.tinkoff.zeppelin.core.notebook.Paragraph;
//
//public class ExecutionRestApi {
//
//  /**
//   * Run note jobs REST API.
//   *
//   * @param noteId ID of Note
//   * @return JSON with status.OK
//   */
//  @PostMapping(value = "/job/{noteId}", produces = "application/json")
//  public ResponseEntity runNoteJobs(@PathVariable("noteId") final String noteId,
//      @RequestParam("waitToFinish") final Boolean waitToFinish)
//      throws IOException, IllegalArgumentException {
//    final boolean blocking = waitToFinish == null || waitToFinish;
//    LOG.info("run note jobs {} waitToFinish: {}", noteId, blocking);
//    final Note note = noteService.getNote(noteId);
//    checkIfNoteIsNotNull(note);
//    checkIfUserCanRun(noteId, "Insufficient privileges you cannot run job for this note");
//
//    try {
//      //TODO(egorklimov): Исполнение было убрано из Note
//      //note.runAllParagraphs(subject, blocking);
//    } catch (final Exception ex) {
//      LOG.error("Exception from run", ex);
//      return new JsonResponse(HttpStatus.PRECONDITION_FAILED,
//          ex.getMessage() + "- Not selected or Invalid Interpreter bind").build();
//    }
//    return new JsonResponse(HttpStatus.OK).build();
//  }
//
//  /**
//   * Stop(delete) note jobs REST API.
//   *
//   * @param noteId ID of Note
//   * @return JSON with status.OK
//   */
//  @DeleteMapping(value = "/job/{noteId}", produces = "application/json")
//  public ResponseEntity stopNoteJobs(@PathVariable("noteId") final String noteId)
//      throws IllegalArgumentException {
//    LOG.info("stop note jobs {} ", noteId);
//    final Note note = noteService.getNote(noteId);
//    checkIfNoteIsNotNull(note);
//    checkIfUserCanRun(noteId, "Insufficient privileges you cannot stop this job for this note");
//
//    //TODO(egorklimov): информация о выполнении была убрана из парграфа
//    //    for (final Paragraph p : note.getParagraphs()) {
//    //      if (!p.isTerminated()) {
//    //        p.abort();
//    //      }
//    //    }
//    return new JsonResponse(HttpStatus.OK).build();
//  }
//
//  /**
//   * Get note job status REST API.
//   *
//   * @param noteId ID of Note
//   * @return JSON with status.OK
//   */
//  @GetMapping(value = "/job/{noteId}", produces = "application/json")
//  public ResponseEntity getNoteJobStatus(@PathVariable("noteId") final String noteId)
//      throws IOException, IllegalArgumentException {
//    LOG.info("get note job status.");
//    final Note note = noteService.getNote(noteId);
//    checkIfNoteIsNotNull(note);
//    checkIfUserCanRead(noteId, "Insufficient privileges you cannot get job status");
//
//    return new JsonResponse(HttpStatus.OK, null, null).build();
//    //return new JsonResponse(HttpStatus.OK, null, note.generateParagraphsInfo()).build();
//  }
//
//  /**
//   * Get note paragraph job status REST API.
//   *
//   * @param noteId ID of Note
//   * @param paragraphId ID of Paragraph
//   * @return JSON with status.OK
//   */
//  @GetMapping(value = "/job/{noteId}/{paragraphId}", produces = "application/json")
//  public ResponseEntity getNoteParagraphStatus(@PathVariable("noteId") final String noteId,
//      @PathVariable("paragraphId") final String paragraphId)
//      throws IllegalArgumentException {
//    LOG.info("get note paragraph job status.");
//    final Note note = noteService.getNote(noteId);
//    checkIfNoteIsNotNull(note);
//    checkIfUserCanRead(noteId, "Insufficient privileges you cannot get job status");
//
//    final Paragraph paragraph = null;//note.getParagraph(paragraphId);
//    checkIfParagraphIsNotNull(paragraph);
//
//    return new JsonResponse(HttpStatus.OK, null, null).build();
//    //return new JsonResponse(HttpStatus.OK, null, note.generateSingleParagraphInfo(paragraphId)).build();
//  }
//
//  /**
//   * Run asynchronously paragraph job REST API.
//   *
//   * @param message - JSON with params if user wants to update dynamic form's value
//   *                null, empty string, empty json if user doesn't want to update
//   * @return JSON with status.OK
//   * @throws IOException
//   * @throws IllegalArgumentException
//   */
//  //TODO(KOT): FIX
//      /*
//  //@POST
//  //@Path("job/{noteId}/{paragraphId}")
//  @ZeppelinApi
//  @PostMapping(value = "/job/{noteId}/{paragraphId}", produces = "application/json")
//  public ResponseEntity runParagraph(@PathVariable("noteId") String noteId,
//                               @PathVariable("paragraphId") String paragraphId, String message)
//          throws IOException, IllegalArgumentException {
//    LOG.info("run paragraph job asynchronously {} {} {}", noteId, paragraphId, message);
//
//    Note note = zeppelinRepository.getNote(noteId);
//    checkIfNoteIsNotNull(note);
//    Paragraph paragraph = note.getParagraph(paragraphId);
//    checkIfParagraphIsNotNull(paragraph);
//
//    Map<String, Object> params = new HashMap<>();
//    if (!StringUtils.isEmpty(message)) {
//      RunParagraphWithParametersRequest request =
//              RunParagraphWithParametersRequest.fromJson(message);
//      params = request.getParams();
//    }
//    zeppelinRepositoryService.runParagraph(noteId, paragraphId, paragraph.getTitle(),
//            paragraph.getText(), null, params, new HashMap<>(),
//            false, false, getServiceContext(), new RestServiceCallback<>());
//    return new JsonResponse(HttpStatus.OK).build();
//  }
//  */
//
//  /**
//   * Run synchronously a paragraph REST API.
//   *
//   * @param noteId      - noteId
//   * @param paragraphId - paragraphId
//   * @param message     - JSON with params if user wants to update dynamic form's value
//   *                    null, empty string, empty json if user doesn't want to update
//   * @return JSON with status.OK
//   * @throws IOException
//   * @throws IllegalArgumentException
//   */
//  //TODO(KOT): FIX
//      /*
//  //@POST
//  //@Path("run/{noteId}/{paragraphId}")
//  @ZeppelinApi
//  @PostMapping(value = "/run/{noteId}/{paragraphId}", produces = "application/json")
//  public ResponseEntity runParagraphSynchronously(@PathVariable("noteId") String noteId,
//                                            @PathVariable("paragraphId") String paragraphId,
//                                            String message)
//          throws IOException, IllegalArgumentException {
//    LOG.info("run paragraph synchronously {} {} {}", noteId, paragraphId, message);
//
//    Note note = zeppelinRepository.getNote(noteId);
//    checkIfNoteIsNotNull(note);
//    Paragraph paragraph = note.getParagraph(paragraphId);
//    checkIfParagraphIsNotNull(paragraph);
//
//    Map<String, Object> params = new HashMap<>();
//    if (!StringUtils.isEmpty(message)) {
//      RunParagraphWithParametersRequest request =
//              RunParagraphWithParametersRequest.fromJson(message);
//      params = request.getParams();
//    }
//
//    if (zeppelinRepositoryService.runParagraph(noteId, paragraphId, paragraph.getTitle(),
//            paragraph.getText(), params,
//            new HashMap<>(), false, true, getServiceContext(), new RestServiceCallback<>())) {
//      note = zeppelinRepositoryService.getNote(noteId, getServiceContext(), new RestServiceCallback<>());
//      Paragraph p = note.getParagraph(paragraphId);
//      InterpreterResult result = p.getReturn();
//      if (result.code() == InterpreterResult.Code.SUCCESS) {
//        return new JsonResponse(HttpStatus.OK, result).build();
//      } else {
//        return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, result).build();
//      }
//    } else {
//      return new JsonResponse(HttpStatus.INTERNAL_SERVER_ERROR, "Fail to run paragraph").build();
//    }
//  }
//*/
//
//  /**
//   * Stop(delete) paragraph job REST API.
//   *
//   * @param noteId      ID of Note
//   * @param paragraphId ID of Paragraph
//   * @return JSON with status.OK
//   * @throws IOException
//   * @throws IllegalArgumentException
//   */
//  //TODO(KOT): FIX
//      /*
//  //@DELETE
//  //@Path("job/{noteId}/{paragraphId}")
//  @ZeppelinApi
//  @DeleteMapping(value = "job/{noteId}/{paragraphId}", produces = "application/json")
//  public ResponseEntity cancelParagraph(@PathVariable("noteId") String noteId,
//                                  @PathVariable("paragraphId") String paragraphId)
//          throws IOException, IllegalArgumentException {
//    LOG.info("stop paragraph job {} ", noteId);
//    zeppelinRepositoryService.cancelParagraph(noteId, paragraphId, getServiceContext(),
//            new RestServiceCallback<Paragraph>());
//    return new JsonResponse(HttpStatus.OK).build();
//  }
//  */
//
//  /**
//   * Get note jobs for job manager.
//   *
//   * @return JSON with status.OK
//   */
//  //TODO(KOT): FIX
//  @GetMapping(value = "/jobmanager/", produces = "application/json")
//  public ResponseEntity getJobListforNote() throws IOException, IllegalArgumentException {
//    LOG.info("Get note jobs for job manager");
//    //final List<JobManagerService.NoteJobInfo> noteJobs = jobManagerService.getNoteJobInfoByUnixTime(0);
//    final Map<String, Object> response = new HashMap<>();
//    response.put("lastResponseUnixTime", System.currentTimeMillis());
//    response.put("jobs", null);
//    return new JsonResponse(HttpStatus.OK, response).build();
//  }
//
//  /**
//   * Get updated note jobs for job manager
//   * <p>
//   * Return the `Note` change information within the post unix timestamp.
//   *
//   * @return JSON with status.OK
//   */
//  @GetMapping(value = "/jobmanager/{lastUpdateUnixtime}/", produces = "application/json")
//  public ResponseEntity getUpdatedJobListforNote(
//      @PathVariable("lastUpdateUnixtime") long lastUpdateUnixTime)
//      throws IOException, IllegalArgumentException {
//    LOG.info("Get updated note jobs lastUpdateTime {}", lastUpdateUnixTime);
//    //List<JobManagerService.NoteJobInfo> noteJobs = jobManagerService.getNoteJobInfoByUnixTime(lastUpdateUnixTime);
//    Map<String, Object> response = new HashMap<>();
//    response.put("lastResponseUnixTime", System.currentTimeMillis());
//    response.put("jobs", null);
//    return new JsonResponse(HttpStatus.OK, response).build();
//  }
//
//
//}
