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

import org.apache.shiro.authz.AuthorizationInfo;
import org.apache.shiro.authz.SimpleAuthorizationInfo;
import org.apache.shiro.realm.ldap.JndiLdapRealm;
import org.apache.shiro.realm.ldap.LdapContextFactory;
import org.apache.shiro.subject.PrincipalCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;

/**
 * Created for org.apache.zeppelin.server.
 */
public class LdapGroupRealm extends JndiLdapRealm {
  private static final Logger LOG = LoggerFactory.getLogger(LdapGroupRealm.class);

  public AuthorizationInfo queryForAuthorizationInfo(final PrincipalCollection principals,
                                                     final LdapContextFactory ldapContextFactory) throws NamingException {
    final String username = (String) getAvailablePrincipal(principals);
    final LdapContext ldapContext = ldapContextFactory.getSystemLdapContext();
    final Set<String> roleNames = getRoleNamesForUser(username, ldapContext, getUserDnTemplate());
    return new SimpleAuthorizationInfo(roleNames);
  }

  public Set<String> getRoleNamesForUser(final String username, final LdapContext ldapContext,
                                         final String userDnTemplate) throws NamingException {
    try {
      final Set<String> roleNames = new LinkedHashSet<>();

      final SearchControls searchCtls = new SearchControls();
      searchCtls.setSearchScope(SearchControls.SUBTREE_SCOPE);

      final String searchFilter = "(&(objectClass=groupOfNames)(member=" + userDnTemplate + "))";
      final Object[] searchArguments = new Object[]{username};

      final NamingEnumeration<?> answer = ldapContext.search(
          String.valueOf(ldapContext.getEnvironment().get("ldap.searchBase")),
          searchFilter,
          searchArguments,
          searchCtls);

      while (answer.hasMoreElements()) {
        final SearchResult sr = (SearchResult) answer.next();
        final Attributes attrs = sr.getAttributes();
        if (attrs != null) {
          final NamingEnumeration<?> ae = attrs.getAll();
          while (ae.hasMore()) {
            final Attribute attr = (Attribute) ae.next();
            if (attr.getID().equals("cn")) {
              roleNames.add((String) attr.get());
            }
          }
        }
      }
      return roleNames;

    } catch (final Exception e) {
      LOG.error("Error", e);
    }

    return new HashSet<>();
  }
}
