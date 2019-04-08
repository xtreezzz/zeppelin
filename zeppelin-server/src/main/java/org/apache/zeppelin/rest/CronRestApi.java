package org.apache.zeppelin.rest;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.zeppelin.NoteService;
import org.apache.zeppelin.annotation.ZeppelinApi;
import org.apache.zeppelin.notebook.Note;
import org.apache.zeppelin.notebook.Scheduler;
import org.apache.zeppelin.rest.exception.NoteNotFoundException;
import org.apache.zeppelin.server.JsonResponse;
import org.apache.zeppelin.service.SecurityService;
import org.apache.zeppelin.storage.SchedulerDAO;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/notebook/cron")
public class CronRestApi {

  private static final Logger LOG = LoggerFactory.getLogger(CronRestApi.class);

  private final NoteService noteRepository;
  private final SecurityService securityService;
  private final SchedulerDAO schedulerDAO;

  public CronRestApi(
      final NoteService noteRepository,
      @Qualifier("NoSecurityService") final SecurityService securityService,
      final SchedulerDAO schedulerDAO) {
    this.noteRepository = noteRepository;
    this.securityService = securityService;
    this.schedulerDAO = schedulerDAO;
  }

  /**
   * Register cron job REST API.
   *
   * @return JSON with status.OK
   */
  @ZeppelinApi
  @PostMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity registerCronJob(
      @PathVariable("noteId") final String noteIdParam,
      @RequestBody  Map<String, String> params
  ) throws IllegalArgumentException {

    String expression = params.get("expression");
    String isEnableParam = params.get("enable");
    long noteId = Long.parseLong(noteIdParam);
    boolean isEnable = isEnableParam == null ? true : Boolean.valueOf(isEnableParam);
    LOG.info("Register cron job note={} request cron msg={}", noteId, expression);

    final Note note = noteRepository.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRun(noteId, "Insufficient privileges you cannot set a cron job for this note");

    final CronExpression cronExpression;
    try {
      cronExpression = new CronExpression(expression);
    } catch (final Exception e) {
      return new JsonResponse(HttpStatus.BAD_REQUEST, "wrong cron expressions.").build();
    }

    Scheduler scheduler = schedulerDAO.getByNote(note.getId());

    if (scheduler != null) {
      scheduler.setExpression(expression);
      scheduler.setEnabled(isEnable);
      scheduler = schedulerDAO.update(scheduler);
    } else {
      final Date nextExecutionDate = cronExpression.getNextValidTimeAfter(new Date());
      final LocalDateTime nextExecution = LocalDateTime.ofInstant(nextExecutionDate.toInstant(), ZoneId.systemDefault());
      scheduler = new Scheduler(
          null,
          note.getId(),
          isEnable,
          expression,
          securityService.getPrincipal(),
          new ArrayList<>(securityService.getAssociatedRoles()),
          nextExecution,
          nextExecution
      );
      scheduler = schedulerDAO.persist(scheduler);
    }

    HashMap<String, Object> responce = new HashMap<>(2);
    responce.put("newCronExpression", scheduler.getExpression());
    responce.put("enable", scheduler.isEnabled());
    return new JsonResponse(HttpStatus.OK, responce).build();
  }

  /**
   * Check valid cron expression REST API.
   *
   * @return JSON with status.OK
   */
  @ZeppelinApi
  @PostMapping(value = "/check_valid", produces = "application/json")
  public ResponseEntity checkCronExpression(@RequestParam("cronExpression") final String expression)
      throws IllegalArgumentException {
    if (!CronExpression.isValidExpression(expression)) {
      return new JsonResponse(HttpStatus.OK, "invalid").build();
    }
    return new JsonResponse(HttpStatus.OK, "valid").build();
  }

  /**
   * Remove cron job REST API.
   *
   * @param noteIdParam ID of Note
   * @return JSON with status.OK
   */
  @ZeppelinApi
  @DeleteMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity removeCronJob(@PathVariable("noteId") final String noteIdParam)
      throws IOException, IllegalArgumentException {
    LOG.info("Remove cron job note {}", noteIdParam);

    long noteId = Long.parseLong(noteIdParam);

    final Note note = noteRepository.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserIsOwner(noteId,
        "Insufficient privileges you cannot remove this cron job from this note");

    note.getScheduler().setEnabled(false);
    //zeppelinRepository.refreshCron(note.getNoteId());
    return new JsonResponse(HttpStatus.OK).build();
  }

  /**
   * Get cron job REST API.
   *
   * @param noteIdParam ID of Note
   * @return JSON with status.OK
   */
  @ZeppelinApi
  @GetMapping(value = "/{noteId}", produces = "application/json")
  public ResponseEntity getCronJob(@PathVariable("noteId") final String noteIdParam)
      throws IOException, IllegalArgumentException {
    LOG.info("Get cron job note {}", noteIdParam);

    long noteId = Long.parseLong(noteIdParam);
    final Note note = noteRepository.getNote(noteId);
    checkIfNoteIsNotNull(note);
    checkIfUserCanRead(noteId, "Insufficient privileges you cannot get cron information");
    Scheduler scheduler = schedulerDAO.getByNote(note.getId());
    final Map<String, Object> response = new HashMap<>();
    response.put("cron", scheduler == null ? null : scheduler.getExpression());
    response.put("enable", scheduler == null ? false : scheduler.isEnabled());

    return new JsonResponse(HttpStatus.OK, response).build();
  }

  /**
   * Check if the current user own the given note.
   */
  private void checkIfUserIsOwner(final Long noteId, final String errorMsg) {
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(securityService.getPrincipal());
    userAndRoles.addAll(securityService.getAssociatedRoles());
    //if (!notePermissionsService.isOwner(userAndRoles, noteId)) {
    //  throw new ForbiddenException(errorMsg);
    //}
  }

  /**
   * Check if the current user can access (at least he have to be reader) the given note.
   */
  private void checkIfUserCanRead(final Long noteId, final String errorMsg) {
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(securityService.getPrincipal());
    userAndRoles.addAll(securityService.getAssociatedRoles());
    //if (!notePermissionsService.hasReadAuthorization(userAndRoles, noteId)) {
    //  throw new ForbiddenException(errorMsg);
    //}
  }

  /**
   * Check if the current user can run the given note.
   */
  private void checkIfUserCanRun(final Long noteId, final String errorMsg) {
    final Set<String> userAndRoles = Sets.newHashSet();
    userAndRoles.add(securityService.getPrincipal());
    userAndRoles.addAll(securityService.getAssociatedRoles());
    //if (!notePermissionsService.hasRunAuthorization(userAndRoles, noteId)) {
    // throw new ForbiddenException(errorMsg);
    //}
  }

  private void checkIfNoteIsNotNull(final Note note) {
    if (note == null) {
      throw new NoteNotFoundException("note not found");
    }
  }
}
