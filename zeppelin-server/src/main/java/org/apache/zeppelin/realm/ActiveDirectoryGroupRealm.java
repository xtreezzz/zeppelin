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
package org.apache.zeppelin.realm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.authc.UsernamePasswordToken;
import org.apache.shiro.authz.AuthorizationException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.ldap.AbstractLdapRealm;
import org.apache.shiro.realm.ldap.DefaultLdapContextFactory;
import org.apache.shiro.realm.ldap.LdapContextFactory;
import org.apache.shiro.realm.ldap.LdapUtils;
import org.apache.shiro.subject.PrincipalCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.shiro.realm.Realm} that authenticates with an active directory LDAP
 * server to determine the roles for a particular user.  This implementation
 * queries for the user's groups and then maps the group names to roles using the
 * {@link #groupRolesMap}.
 *
 * @since 0.1
 */
public class ActiveDirectoryGroupRealm extends AbstractLdapRealm {
  private static final Logger log = LoggerFactory.getLogger(ActiveDirectoryGroupRealm.class);

  private static final String ROLE_NAMES_DELIMETER = ",";

  private String userSearchAttributeName = "sAMAccountName";

  public String getUserSearchAttributeName() {
    return userSearchAttributeName;
  }
  
  private LdapContextFactory ldapContextFactory;

  protected void onInit() {
    super.onInit();
    this.getLdapContextFactory();
  }

  public LdapContextFactory getLdapContextFactory() {
    if (this.ldapContextFactory == null) {
      if (log.isDebugEnabled()) {
        log.debug("No LdapContextFactory specified - creating a default instance.");
      }

      final DefaultLdapContextFactory defaultFactory = new DefaultLdapContextFactory();
      defaultFactory.setPrincipalSuffix(this.principalSuffix);
      defaultFactory.setSearchBase(this.searchBase);
      defaultFactory.setUrl(this.url);
      defaultFactory.setSystemUsername(this.systemUsername);
      defaultFactory.setSystemPassword(this.systemPassword);
      this.ldapContextFactory = defaultFactory;
    }

    return this.ldapContextFactory;
  }

  protected AuthenticationInfo doGetAuthenticationInfo(final AuthenticationToken token)
          throws AuthenticationException {
    try {
      final AuthenticationInfo info = this.queryForAuthenticationInfo(token,
              this.getLdapContextFactory());
      return info;
    } catch (final javax.naming.AuthenticationException var5) {
      throw new AuthenticationException("LDAP authentication failed.", var5);
    } catch (final NamingException var6) {
      final String msg = "LDAP naming error while attempting to authenticate user.";
      throw new AuthenticationException(msg, var6);
    }
  }

  protected AuthorizationInfo doGetAuthorizationInfo(final PrincipalCollection principals) {
    try {
      final AuthorizationInfo info = this.queryForAuthorizationInfo(principals,
              this.getLdapContextFactory());
      return info;
    } catch (final NamingException var5) {
      final String msg = "LDAP naming error while attempting to " +
              "retrieve authorization for user [" + principals + "].";
      throw new AuthorizationException(msg, var5);
    }
  }

  /**
   * Builds an {@link AuthenticationInfo} object by querying the active directory LDAP context for
   * the specified username.  This method binds to the LDAP server using the provided username
   * and password - which if successful, indicates that the password is correct.
   * <p/>
   * This method can be overridden by subclasses to query the LDAP server in a more complex way.
   *
   * @param token              the authentication token provided by the user.
   * @param ldapContextFactory the factory used to build connections to the LDAP server.
   * @return an {@link AuthenticationInfo} instance containing information retrieved from LDAP.
   * @throws NamingException if any LDAP errors occur during the search.
   */
  protected AuthenticationInfo queryForAuthenticationInfo(final AuthenticationToken token,
                                                          final LdapContextFactory ldapContextFactory) throws NamingException {
    final UsernamePasswordToken upToken = (UsernamePasswordToken) token;

    // Binds using the username and password provided by the user.
    LdapContext ctx = null;
    try {
      String userPrincipalName = upToken.getUsername();
      if (!isValidPrincipalName(userPrincipalName)) {
        return null;
      }
      if (this.principalSuffix != null && userPrincipalName.indexOf('@') < 0) {
        userPrincipalName = upToken.getUsername() + this.principalSuffix;
      }
      ctx = ldapContextFactory.getLdapContext(
              userPrincipalName, upToken.getPassword());
    } finally {
      LdapUtils.closeContext(ctx);
    }

    return buildAuthenticationInfo(upToken.getUsername(), upToken.getPassword());
  }

  private Boolean isValidPrincipalName(final String userPrincipalName) {
    if (userPrincipalName != null) {
      if (StringUtils.isNotEmpty(userPrincipalName) && userPrincipalName.contains("@")) {
        final String userPrincipalWithoutDomain = userPrincipalName.split("@")[0].trim();
        return StringUtils.isNotEmpty(userPrincipalWithoutDomain);
      } else return StringUtils.isNotEmpty(userPrincipalName);
    }
    return false;
  }

  protected AuthenticationInfo buildAuthenticationInfo(String username, final char[] password) {
    if (this.principalSuffix != null && username.indexOf('@') > 1) {
      username = username.split("@")[0];
    }
    return new SimpleAuthenticationInfo(username, password, getName());
  }

  /**
   * Builds an {@link org.apache.shiro.authz.AuthorizationInfo} object by querying the active
   * directory LDAP context for the groups that a user is a member of.  The groups are then
   * translated to role names by using the configured {@link #groupRolesMap}.
   * <p/>
   * This implementation expects the <tt>principal</tt> argument to be a String username.
   * <p/>
   * Subclasses can override this method to determine authorization data (roles, permissions, etc)
   * in a more complex way.  Note that this default implementation does not support permissions,
   * only roles.
   *
   * @param principals         the principal of the Subject whose account is being retrieved.
   * @param ldapContextFactory the factory used to create LDAP connections.
   * @return the AuthorizationInfo for the given Subject principal.
   * @throws NamingException if an error occurs when searching the LDAP server.
   */
  protected AuthorizationInfo queryForAuthorizationInfo(final PrincipalCollection principals,
                                                        final LdapContextFactory ldapContextFactory) throws NamingException {
    final String username = (String) getAvailablePrincipal(principals);
    return queryForAuthorizationInfo(username, ldapContextFactory);
  }

  protected AuthorizationInfo queryForAuthorizationInfo(final String principal,
                                                        final LdapContextFactory ldapContextFactory) throws NamingException {

    // Perform context search
    final LdapContext ldapContext = ldapContextFactory.getSystemLdapContext();

    Set<String> roleNames;

    try {
      roleNames = getRoleNamesForUser(principal, ldapContext);
    } finally {
      LdapUtils.closeContext(ldapContext);
    }

    return buildAuthorizationInfo(roleNames);
  }

  protected AuthorizationInfo buildAuthorizationInfo(final Set<String> roleNames) {
    return new SimpleAuthorizationInfo(roleNames);
  }

  public List<String> searchForUserName(final String containString, final LdapContext ldapContext,
                                        final int numUsersToFetch)
          throws NamingException {
    final List<String> userNameList = new ArrayList<>();

    final SearchControls searchCtls = new SearchControls();
    searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchCtls.setCountLimit(numUsersToFetch);
    searchCtls.setReturningAttributes(new String[] {this.userSearchAttributeName});

    final String searchFilter = String.format(
            "(&(objectClass=person)(objectClass=top)(%s=%s*))",
            this.userSearchAttributeName,
            containString
    );

    final Object[] searchArguments = new Object[]{containString};

    final NamingEnumeration answer = ldapContext.search(searchBase, searchFilter, searchArguments,
            searchCtls);

    while (answer.hasMoreElements()) {
      final SearchResult sr = (SearchResult) answer.next();

      if (log.isDebugEnabled()) {
        log.debug("Retrieving userprincipalname names for user [" + sr.getName() + "]");
      }

      final Attributes attrs = sr.getAttributes();
      if (attrs != null) {
        final NamingEnumeration ae = attrs.getAll();
        while (ae.hasMore()) {
          final Attribute attr = (Attribute) ae.next();
          if (attr.getID().equals(this.userSearchAttributeName)) {
            userNameList.addAll(LdapUtils.getAllAttributeValues(attr));
            break;
          }
        }
      }
    }
    return userNameList;
  }

  public Set<String> getListRoles(final String containString,
                                  final LdapContext ldapContext,
                                  final int numUsersToFetch) throws NamingException {

    final Set<String> names = new HashSet<>();

    final SearchControls searchCtls = new SearchControls();
    searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchCtls.setCountLimit(numUsersToFetch);
    searchCtls.setReturningAttributes(new String[] {this.userSearchAttributeName});

    final String searchFilter = String.format(
            "(&(objectClass=group)(objectClass=top)(%s=%s*))",
            this.userSearchAttributeName,
            containString
    );
    final Object[] searchArguments = new Object[]{};

    final NamingEnumeration answer = ldapContext.search(searchBase, searchFilter, searchArguments,
            searchCtls);

    while (answer.hasMoreElements()) {
      final SearchResult sr = (SearchResult) answer.next();

      if (log.isDebugEnabled()) {
        log.debug("Retrieving group names for user [" + sr.getName() + "]");
      }

      final Attributes attrs = sr.getAttributes();
      if (attrs != null) {
        final NamingEnumeration ae = attrs.getAll();
        while (ae.hasMore()) {
          final Attribute attr = (Attribute) ae.next();

          if (attr.getID().equals(this.userSearchAttributeName)) {
            names.addAll(LdapUtils.getAllAttributeValues(attr));
            break;
          }
        }
      }

    }
    return names;
  }

  private Set<String> getRoleNamesForUser(final String username, final LdapContext ldapContext)
          throws NamingException {
    final Set<String> roleNames = new LinkedHashSet<>();

    final SearchControls searchCtls = new SearchControls();
    searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);
    searchCtls.setCountLimit(1);
    searchCtls.setReturningAttributes(new String[] {"memberOf"});
    String userPrincipalName = username;
    if (this.principalSuffix != null && userPrincipalName.indexOf('@') > 1) {
      userPrincipalName = userPrincipalName.split("@")[0];
    }

    final String searchFilter = String.format(
            "(&(objectClass=person)(objectClass=top)(%s=%s*))",
            this.userSearchAttributeName,
            userPrincipalName);

    final Object[] searchArguments = new Object[]{userPrincipalName};

    final NamingEnumeration answer = ldapContext.search(searchBase, searchFilter, searchArguments,
            searchCtls);

    while (answer.hasMoreElements()) {
      final SearchResult sr = (SearchResult) answer.next();

      if (log.isDebugEnabled()) {
        log.debug("Retrieving group names for user [" + sr.getName() + "]");
      }

      final Attributes attrs = sr.getAttributes();

      if (attrs != null) {
        final NamingEnumeration ae = attrs.getAll();
        while (ae.hasMore()) {
          final Attribute attr = (Attribute) ae.next();

          if (attr.getID().equals("memberOf")) {

            final Collection<String> groupNames = LdapUtils.getAllAttributeValues(attr);

            if (log.isDebugEnabled()) {
              log.debug("Groups found for user [" + username + "]: " + groupNames);
            }
            roleNames.addAll(groupNames.stream()
                    .map(s -> s.substring(s.indexOf('=') + 1, s.indexOf(',')))
                    .collect(Collectors.toList())
            );
            break;
          }
        }
      }
    }
    return roleNames;
  }
}
