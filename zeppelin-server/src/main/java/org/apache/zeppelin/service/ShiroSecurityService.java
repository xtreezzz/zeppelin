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
package org.apache.zeppelin.service;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.shiro.UnavailableSecurityManagerException;
import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.realm.Realm;
import org.apache.shiro.realm.jdbc.JdbcRealm;
import org.apache.shiro.realm.ldap.JndiLdapContextFactory;
import org.apache.shiro.realm.ldap.JndiLdapRealm;
import org.apache.shiro.realm.text.IniRealm;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.JdbcUtils;
import org.apache.shiro.util.ThreadContext;
import org.apache.shiro.web.mgt.DefaultWebSecurityManager;
import org.apache.zeppelin.configuration.ZeppelinConfiguration;
import org.apache.zeppelin.realm.ActiveDirectoryGroupRealm;
import org.apache.zeppelin.realm.LdapRealm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;
import javax.sql.DataSource;
import java.security.Principal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/** Tools for securing Zeppelin. */
@Component(value="ShiroSecurityService")
public class ShiroSecurityService implements SecurityService {

  private final Logger LOGGER = LoggerFactory.getLogger(ShiroSecurityService.class);

  private final ZeppelinConfiguration zeppelinConfiguration;

  @Autowired
  public ShiroSecurityService(final ZeppelinConfiguration zeppelinConfiguration) throws Exception {
    LOGGER.info("NoSecurityService is initialized");
    this.zeppelinConfiguration = zeppelinConfiguration;

    if (zeppelinConfiguration.getShiroPath().length() > 0) {
      try {
        final Collection<Realm> realms =
            ((DefaultWebSecurityManager) org.apache.shiro.SecurityUtils.getSecurityManager())
                .getRealms();
        if (realms.size() > 1) {
          Boolean isIniRealmEnabled = false;
          for (final Object realm : realms) {
            if (realm instanceof IniRealm && ((IniRealm) realm).getIni().get("users") != null) {
              isIniRealmEnabled = true;
              break;
            }
          }
          if (isIniRealmEnabled) {
            throw new Exception(
                "IniRealm/password based auth mechanisms should be exclusive. "
                    + "Consider removing [users] block from shiro.ini");
          }
        }
      } catch (final UnavailableSecurityManagerException e) {
        LOGGER.error("Failed to initialise shiro configuraion", e);
      }
    }
  }

  /**
   * Return the authenticated user if any otherwise returns "anonymous".
   *
   * @return shiro principal
   */
  @Override
  public String getPrincipal() {
    final Subject subject = org.apache.shiro.SecurityUtils.getSubject();

    String principal;
    if (subject.isAuthenticated()) {
      principal = extractPrincipal(subject);
      if (zeppelinConfiguration.isUsernameForceLowerCase()) {
        LOGGER.debug("Converting principal name " + principal
            + " to lower case:" + principal.toLowerCase());
        principal = principal.toLowerCase();
      }
    } else {
      // TODO(jl): Could be better to occur error?
      principal = "anonymous";
    }
    return principal;
  }

  private String extractPrincipal(final Subject subject) {
    final String principal;
    final Object principalObject = subject.getPrincipal();
    if (principalObject instanceof Principal) {
      principal = ((Principal) principalObject).getName();
    } else {
      principal = String.valueOf(principalObject);
    }
    return principal;
  }

  @Override
  public Collection getRealmsList() {
    final DefaultWebSecurityManager defaultWebSecurityManager;
    final String key = ThreadContext.SECURITY_MANAGER_KEY;
    defaultWebSecurityManager = (DefaultWebSecurityManager) ThreadContext.get(key);
    return defaultWebSecurityManager.getRealms();
  }

  /** Checked if shiro enabled or not. */
  @Override
  public boolean isAuthenticated() {
    return org.apache.shiro.SecurityUtils.getSubject().isAuthenticated();
  }

  /**
   * Get candidated users based on searchText
   *
   * @param searchText
   * @param numUsersToFetch
   * @return
   */
  @Override
  public List<String> getMatchedUsers(final String searchText, final int numUsersToFetch) {
    final List<String> usersList = new ArrayList<>();
    try {
      final Collection<Realm> realmsList = (Collection<Realm>) getRealmsList();
      if (realmsList != null) {
        for (final Realm realm : realmsList) {
          final String name = realm.getClass().getName();
          LOGGER.debug("RealmClass.getName: " + name);
          if (name.equals("org.apache.shiro.realm.text.IniRealm")) {
            usersList.addAll(getUserList((IniRealm) realm));
          } else if (name.equals("org.apache.zeppelin.realm.LdapGroupRealm")) {
            usersList.addAll(getUserList((JndiLdapRealm) realm, searchText, numUsersToFetch));
          } else if (name.equals("org.apache.zeppelin.realm.LdapRealm")) {
            usersList.addAll(getUserList((LdapRealm) realm, searchText, numUsersToFetch));
          } else if (name.equals("org.apache.zeppelin.realm.ActiveDirectoryGroupRealm")) {
            usersList.addAll(
                getUserList((ActiveDirectoryGroupRealm) realm, searchText, numUsersToFetch));
          } else if (name.equals("org.apache.shiro.realm.jdbc.JdbcRealm")) {
            usersList.addAll(getUserList((JdbcRealm) realm));
          }
        }
      }
    } catch (final Exception e) {
      LOGGER.error("Exception in retrieving Users from realms ", e);
    }
    return usersList;
  }

  /**
   * Get matched roles.
   *
   * @return
   */
  @Override
  public List<String> getMatchedRoles() {
    final List<String> rolesList = new ArrayList<>();
    try {
      final Collection realmsList = getRealmsList();
      if (realmsList != null) {
        for (final Iterator<Realm> iterator = realmsList.iterator(); iterator.hasNext(); ) {
          final Realm realm = iterator.next();
          final String name = realm.getClass().getName();
          LOGGER.debug("RealmClass.getName: " + name);
          if (name.equals("org.apache.shiro.realm.text.IniRealm")) {
            rolesList.addAll(getRolesList((IniRealm) realm));
          } else if (name.equals("org.apache.zeppelin.realm.LdapRealm")) {
            rolesList.addAll(getRolesList((LdapRealm) realm));
          }
        }
      }
    } catch (final Exception e) {
      LOGGER.error("Exception in retrieving Users from realms ", e);
    }
    return rolesList;
  }

  /**
   * Return the roles associated with the authenticated user if any otherwise returns empty set.
   * TODO(prasadwagle) Find correct way to get user roles (see SHIRO-492)
   *
   * @return shiro roles
   */
  @Override
  public Set<String> getAssociatedRoles() {
    final Subject subject = org.apache.shiro.SecurityUtils.getSubject();
    HashSet<String> roles = new HashSet<>();
    Map allRoles = null;

    if (subject.isAuthenticated()) {
      final Collection realmsList = getRealmsList();
      for (final Iterator<Realm> iterator = realmsList.iterator(); iterator.hasNext(); ) {
        final Realm realm = iterator.next();
        final String name = realm.getClass().getName();
        if (name.equals("org.apache.shiro.realm.text.IniRealm")) {
          allRoles = ((IniRealm) realm).getIni().get("roles");
          break;
        } else if (name.equals("org.apache.zeppelin.realm.LdapRealm")) {
          try {
            final AuthorizationInfo auth =
                ((LdapRealm) realm)
                    .queryForAuthorizationInfo(
                        new SimplePrincipalCollection(subject.getPrincipal(), realm.getName()),
                        ((LdapRealm) realm).getContextFactory());
            if (auth != null) {
              roles = new HashSet<>(auth.getRoles());
            }
          } catch (final NamingException e) {
            LOGGER.error("Can't fetch roles", e);
          }
          break;
        } else if (name.equals("org.apache.zeppelin.realm.ActiveDirectoryGroupRealm")) {
          allRoles = ((ActiveDirectoryGroupRealm) realm).getListRoles();
          break;
        }
      }
      if (allRoles != null) {
        final Iterator it = allRoles.entrySet().iterator();
        while (it.hasNext()) {
          final Map.Entry pair = (Map.Entry) it.next();
          if (subject.hasRole((String) pair.getKey())) {
            roles.add((String) pair.getKey());
          }
        }
      }
    }
    return roles;
  }

  /** Function to extract users from shiro.ini. */
  private List<String> getUserList(final IniRealm r) {
    final List<String> userList = new ArrayList<>();
    final Map getIniUser = r.getIni().get("users");
    if (getIniUser != null) {
      final Iterator it = getIniUser.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry pair = (Map.Entry) it.next();
        userList.add(pair.getKey().toString().trim());
      }
    }
    return userList;
  }

  /**
   * * Get user roles from shiro.ini.
   *
   * @param r
   * @return
   */
  private List<String> getRolesList(final IniRealm r) {
    final List<String> roleList = new ArrayList<>();
    final Map getIniRoles = r.getIni().get("roles");
    if (getIniRoles != null) {
      final Iterator it = getIniRoles.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry pair = (Map.Entry) it.next();
        roleList.add(pair.getKey().toString().trim());
      }
    }
    return roleList;
  }

  /** Function to extract users from LDAP. */
  private List<String> getUserList(final JndiLdapRealm r, final String searchText, final int numUsersToFetch) {
    final List<String> userList = new ArrayList<>();
    final String userDnTemplate = r.getUserDnTemplate();
    final String[] userDn = userDnTemplate.split(",", 2);
    final String userDnPrefix = userDn[0].split("=")[0];
    final String userDnSuffix = userDn[1];
    final JndiLdapContextFactory cf = (JndiLdapContextFactory) r.getContextFactory();
    try {
      final LdapContext ctx = cf.getSystemLdapContext();
      final SearchControls constraints = new SearchControls();
      constraints.setCountLimit(numUsersToFetch);
      constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
      final String[] attrIDs = {userDnPrefix};
      constraints.setReturningAttributes(attrIDs);
      final NamingEnumeration result =
          ctx.search(userDnSuffix, "(" + userDnPrefix + "=*" + searchText + "*)", constraints);
      while (result.hasMore()) {
        final Attributes attrs = ((SearchResult) result.next()).getAttributes();
        if (attrs.get(userDnPrefix) != null) {
          final String currentUser = attrs.get(userDnPrefix).toString();
          userList.add(currentUser.split(":")[1].trim());
        }
      }
    } catch (final Exception e) {
      LOGGER.error("Error retrieving User list from Ldap Realm", e);
    }
    LOGGER.info("UserList: " + userList);
    return userList;
  }

  /** Function to extract users from Zeppelin LdapRealm. */
  private List<String> getUserList(final LdapRealm r, final String searchText, final int numUsersToFetch) {
    final List<String> userList = new ArrayList<>();
    LOGGER.debug("SearchText: " + searchText);
    final String userAttribute = r.getUserSearchAttributeName();
    final String userSearchRealm = r.getUserSearchBase();
    final String userObjectClass = r.getUserObjectClass();
    final JndiLdapContextFactory cf = (JndiLdapContextFactory) r.getContextFactory();
    try {
      final LdapContext ctx = cf.getSystemLdapContext();
      final SearchControls constraints = new SearchControls();
      constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);
      constraints.setCountLimit(numUsersToFetch);
      final String[] attrIDs = {userAttribute};
      constraints.setReturningAttributes(attrIDs);
      final NamingEnumeration result =
          ctx.search(
              userSearchRealm,
              "(&(objectclass="
                  + userObjectClass
                  + ")("
                  + userAttribute
                  + "=*"
                  + searchText
                  + "*))",
              constraints);
      while (result.hasMore()) {
        final Attributes attrs = ((SearchResult) result.next()).getAttributes();
        if (attrs.get(userAttribute) != null) {
          final String currentUser;
          if (r.getUserLowerCase()) {
            LOGGER.debug("userLowerCase true");
            currentUser = ((String) attrs.get(userAttribute).get()).toLowerCase();
          } else {
            LOGGER.debug("userLowerCase false");
            currentUser = (String) attrs.get(userAttribute).get();
          }
          LOGGER.debug("CurrentUser: " + currentUser);
          userList.add(currentUser.trim());
        }
      }
    } catch (final Exception e) {
      LOGGER.error("Error retrieving User list from Ldap Realm", e);
    }
    return userList;
  }

  /**
   * * Get user roles from shiro.ini for Zeppelin LdapRealm.
   *
   * @param r
   * @return
   */
  private List<String> getRolesList(final LdapRealm r) {
    final List<String> roleList = new ArrayList<>();
    final Map<String, String> roles = r.getListRoles();
    if (roles != null) {
      final Iterator it = roles.entrySet().iterator();
      while (it.hasNext()) {
        final Map.Entry pair = (Map.Entry) it.next();
        LOGGER.debug("RoleKeyValue: " + pair.getKey() + " = " + pair.getValue());
        roleList.add((String) pair.getKey());
      }
    }
    return roleList;
  }

  private List<String> getUserList(
          final ActiveDirectoryGroupRealm r, final String searchText, final int numUsersToFetch) {
    List<String> userList = new ArrayList<>();
    try {
      final LdapContext ctx = r.getLdapContextFactory().getSystemLdapContext();
      userList = r.searchForUserName(searchText, ctx, numUsersToFetch);
    } catch (final Exception e) {
      LOGGER.error("Error retrieving User list from ActiveDirectory Realm", e);
    }
    return userList;
  }

  /** Function to extract users from JDBCs. */
  private List<String> getUserList(final JdbcRealm obj) {
    final List<String> userlist = new ArrayList<>();
    Connection con = null;
    PreparedStatement ps = null;
    ResultSet rs = null;
    DataSource dataSource = null;
    String authQuery = "";
    String[] retval;
    String tablename = "";
    String username = "";
    final String userquery;
    try {
      dataSource = (DataSource) FieldUtils.readField(obj, "dataSource", true);
      authQuery = (String) FieldUtils.readField(obj, "authenticationQuery", true);
      LOGGER.info(authQuery);
      final String authQueryLowerCase = authQuery.toLowerCase();
      retval = authQueryLowerCase.split("from", 2);
      if (retval.length >= 2) {
        retval = retval[1].split("with|where", 2);
        tablename = retval[0];
        retval = retval[1].split("where", 2);
        if (retval.length >= 2) {
          retval = retval[1].split("=", 2);
        } else {
          retval = retval[0].split("=", 2);
        }
        username = retval[0];
      }

      if (StringUtils.isBlank(username) || StringUtils.isBlank(tablename)) {
        return userlist;
      }

      userquery = String.format("SELECT %s FROM %s", username, tablename);
    } catch (final IllegalAccessException e) {
      LOGGER.error("Error while accessing dataSource for JDBC Realm", e);
      return Lists.newArrayList();
    }

    try {
      con = dataSource.getConnection();
      ps = con.prepareStatement(userquery);
      rs = ps.executeQuery();
      while (rs.next()) {
        userlist.add(rs.getString(1).trim());
      }
    } catch (final Exception e) {
      LOGGER.error("Error retrieving User list from JDBC Realm", e);
    } finally {
      JdbcUtils.closeResultSet(rs);
      JdbcUtils.closeStatement(ps);
      JdbcUtils.closeConnection(con);
    }
    return userlist;
  }
}
