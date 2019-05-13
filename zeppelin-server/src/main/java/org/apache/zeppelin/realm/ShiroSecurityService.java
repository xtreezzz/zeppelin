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

import java.security.Principal;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.naming.ldap.LdapContext;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tools for securing Zeppelin.
 */
public class ShiroSecurityService extends ActiveDirectoryGroupRealm {

  private final Logger LOGGER = LoggerFactory.getLogger(ShiroSecurityService.class);

  String getPrincipal() {
    final Subject subject = org.apache.shiro.SecurityUtils.getSubject();

    String principal = null;
    if (subject.isAuthenticated()) {
      final Object principalObject = subject.getPrincipal();
      if (principalObject instanceof Principal) {
        principal = ((Principal) principalObject).getName().toLowerCase();
      } else {
        principal = String.valueOf(principalObject).toLowerCase();
      }
    }
    return principal;
  }


  public List<String> getMatchedUsers(final String searchText, final int numUsersToFetch) {
    final List<String> usersList = new ArrayList<>();
    try {
      final LdapContext ctx = getLdapContextFactory().getSystemLdapContext();
      usersList.addAll(searchForUserName(searchText, ctx, numUsersToFetch));
    } catch (final Exception e) {
      LOGGER.error("Error retrieving User list from ActiveDirectory Realm", e);
    }
    return usersList;
  }

  public List<String> getMatchedRoles(final String searchText, final int numUsersToFetch) {
    final List<String> rolesList = new ArrayList<>();
    try {
      final LdapContext ctx = getLdapContextFactory().getSystemLdapContext();
      rolesList.addAll(getListRoles(searchText, ctx, numUsersToFetch));
    } catch (final Exception e) {
      LOGGER.error("Error retrieving User list from ActiveDirectory Realm", e);
    }
    return rolesList;
  }


  Set<String> getAssociatedRoles(final String principal) {
    final Set<String> roles = new HashSet<>();
    try {
      roles.addAll(queryForAuthorizationInfo(principal, getLdapContextFactory()).getRoles());
    } catch (final Exception e) {
      LOGGER.error("Error retrieving User list from ActiveDirectory Realm", e);
    }
    return roles;
  }
}
