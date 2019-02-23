package org.apache.zeppelin.notebook;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.zeppelin.conf.ZeppelinConfiguration;
import org.quartz.CronExpression;

/**
 * note config:
 *  > cron -> String cronExpression
 *  -- {@link Notebook#refreshCron}
 *
 *  > personalizedMode -> String (zachem??) true/false
 *  -- {@link Note#setPersonalizedMode(Boolean)}
 *
 *  > isZeppelinNotebookCronEnable -> Boolean
 *  -- {@link Note#setCronSupported(ZeppelinConfiguration)}
 *
 *  > cronExecutingUser -> String
 *  > cronExecutingRoles -> String
 *  -- {@link Notebook.CronJob#runAll(Note)}
 *
 *  > releaseresource -> Boolean
 *  -- {@link NotebookRestApi#registerCronJob} (849)
 */
public class CronJobConfiguration {

  public final boolean isCronEnabled;
  public final boolean releaseResourceFlag;
  public final String cronExpression;
  public final String cronExecutingUser;
  public final String cronExecutingRoles;

  private CronJobConfiguration(Builder builder) {
    this.isCronEnabled = builder.isCronEnabled;
    this.releaseResourceFlag = builder.releaseResourceFlag;
    this.cronExpression = builder.cronExpression;
    this.cronExecutingUser = builder.cronExecutingUser;
    this.cronExecutingRoles = builder.cronExecutingRoles;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    CronJobConfiguration that = (CronJobConfiguration) o;
    return new EqualsBuilder()
        .append(isCronEnabled, that.isCronEnabled)
        .append(releaseResourceFlag, that.releaseResourceFlag)
        .append(cronExpression, that.cronExpression)
        .append(cronExecutingUser, that.cronExecutingUser)
        .append(cronExecutingRoles, that.cronExecutingRoles)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(isCronEnabled)
        .append(releaseResourceFlag)
        .append(cronExpression)
        .append(cronExecutingUser)
        .append(cronExecutingRoles)
        .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
        .append("isCronEnabled", isCronEnabled)
        .append("releaseResourceFlag", releaseResourceFlag)
        .append("cronExpression", cronExpression)
        .append("cronExecutingUser", cronExecutingUser)
        .append("cronExecutingRoles", cronExecutingRoles)
        .toString();
  }


  public static class Builder {

    private boolean isCronEnabled = false;
    private boolean releaseResourceFlag = false;
    private String cronExpression = StringUtils.EMPTY;
    private String cronExecutingUser = "anonymous";
    private String cronExecutingRoles = StringUtils.EMPTY;

    public static Builder fromExisting(CronJobConfiguration configuration) {
      Builder b = new Builder();
      b.isCronEnabled = configuration.isCronEnabled;
      b.releaseResourceFlag = configuration.releaseResourceFlag;
      b.cronExpression = configuration.cronExpression;
      b.cronExecutingUser = configuration.cronExecutingUser;
      b.cronExecutingRoles = configuration.cronExecutingRoles;
      return b;
    }

    public static Builder isCronEnabled(boolean isCronEnabled) {
      Builder b = new Builder();
      b.isCronEnabled = isCronEnabled;
      return b;
    }

    public Builder enableCron(boolean isCronEnabled) {
      this.isCronEnabled = isCronEnabled;
      return this;
    }

    public Builder releaseResource(boolean releaseResourceFlag) {
      this.releaseResourceFlag = releaseResourceFlag;
      return this;
    }

    public Builder cronExpression(String cronExpression) {
      if (CronExpression.isValidExpression(cronExpression)) {
        this.cronExpression = cronExpression;
      } else {
        //TODO(egorklimov) fix cron expression validation logic
      }
      return this;
    }

    public Builder cronExecutingUser(String cronExecutingUser) {
      this.cronExecutingUser = cronExecutingUser;
      return this;
    }

    public Builder cronExecutingRoles(String cronExecutingRoles) {
      this.cronExecutingRoles = cronExecutingRoles;
      return this;
    }

    public CronJobConfiguration build() {
      return new CronJobConfiguration(this);
    }
  }
}
