package org.apache.zeppelin.interpreter.configuration.option;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import javax.annotation.Nonnull;

/**
 * Option 'permission' on interpreter configuration page.
 */
public class Permissions implements Serializable {

  @Nonnull
  private final List<String> owners;

  private boolean isEnabled;

  public Permissions() {
    this.owners = new ArrayList<>();
    this.isEnabled = false;
  }

  public Permissions(@Nonnull final List<String> owners, final boolean isEnabled) {
    this.owners = owners;
    this.isEnabled = isEnabled;
  }

  //TODO(egorklimov): conf.isUsernameForceLowerCase()??
  // было:
  //    if (null != owners && conf.isUsernameForceLowerCase()) {
  //      List<String> lowerCaseUsers = new ArrayList<String>();
  //      for (String owner : owners) {
  //        lowerCaseUsers.add(owner.toLowerCase());
  //      }
  //      return lowerCaseUsers;
  //    }
  //    return owners;
  @Nonnull
  public List<String> getOwners() {
    return owners;
  }

  public boolean isEnabled() {
    return isEnabled;
  }

  public void setEnabled(final boolean enabled) {
    isEnabled = enabled;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", "{", "}")
        .add("owners: " + owners)
        .add("isEnabled: " + isEnabled)
        .toString();
  }
}
