package org.apache.zeppelin.repo.api;

import org.apache.commons.lang.StringUtils;

/**
 * Represents the 'Revision' a point in life of the notebook
 */
public class Revision {
  public static final Revision EMPTY = new Revision(StringUtils.EMPTY, StringUtils.EMPTY, 0);

  public String id;
  public String message;
  public int time;

  public Revision(String revId, String message, int time) {
    this.id = revId;
    this.message = message;
    this.time = time;
  }

  public static boolean isEmpty(Revision revision) {
    return revision == null || EMPTY.equals(revision);
  }
}