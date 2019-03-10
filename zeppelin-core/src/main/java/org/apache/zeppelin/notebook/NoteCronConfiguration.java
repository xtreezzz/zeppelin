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

package org.apache.zeppelin.notebook;

import java.io.Serializable;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.quartz.CronExpression;

/**
 * Paragraph cron configuration.
 */
public class NoteCronConfiguration implements Serializable {

  private boolean isCronEnabled;
  // Should CronJob release resources after execution?
  private boolean releaseResourceFlag;
  private String cronExpression;

  // TODO(egorklimov): Возможно это не нужно здесь хранить
  private String cronExecutingUser;
  private String cronExecutingRoles;

  public NoteCronConfiguration() {
    isCronEnabled = false;
    releaseResourceFlag = false;
    cronExpression = StringUtils.EMPTY;
    cronExecutingUser = "anonymous";
    cronExecutingRoles = StringUtils.EMPTY;
  }

  public NoteCronConfiguration(boolean isCronEnabled, boolean releaseResourceFlag,
                               String cronExpression, String cronExecutingUser, String cronExecutingRoles) {
    this.isCronEnabled = isCronEnabled;
    this.releaseResourceFlag = releaseResourceFlag;
    this.cronExpression = cronExpression;
    this.cronExecutingUser = cronExecutingUser;
    this.cronExecutingRoles = cronExecutingRoles;
  }

  public NoteCronConfiguration(Map config) {
    this.isCronEnabled = (boolean) config.get("isCronEnabled");
    this.releaseResourceFlag = (boolean) config.get("releaseResourceFlag");
    this.cronExpression = (String) config.get("cronExpression");
    this.cronExecutingUser = (String) config.get("cronExecutingUser");
    this.cronExecutingRoles = (String) config.get("cronExecutingRoles");
  }

  public boolean isCronEnabled() {
    return isCronEnabled;
  }

  public boolean isReleaseResourceFlag() {
    return releaseResourceFlag;
  }

  public String getCronExpression() {
    return cronExpression;
  }

  public String getCronExecutingUser() {
    return cronExecutingUser;
  }

  public String getCronExecutingRoles() {
    return cronExecutingRoles;
  }

  public void setCronEnabled(boolean cronEnabled) {
    isCronEnabled = cronEnabled;
  }

  public void setReleaseResourceFlag(boolean releaseResourceFlag) {
    this.releaseResourceFlag = releaseResourceFlag;
  }

  public void setCronExpression(String cronExpression) {
    if (!CronExpression.isValidExpression(cronExpression)) {
      throw new IllegalArgumentException("Wrong cron expression passed - " + cronExpression);
    }
    this.cronExpression = cronExpression;
  }

  public void setCronExecutingUser(String cronExecutingUser) {
    this.cronExecutingUser = cronExecutingUser;
  }

  public void setCronExecutingRoles(String cronExecutingRoles) {
    this.cronExecutingRoles = cronExecutingRoles;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    NoteCronConfiguration that = (NoteCronConfiguration) o;
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
}
