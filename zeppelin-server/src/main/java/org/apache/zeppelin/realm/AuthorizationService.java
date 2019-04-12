package org.apache.zeppelin.realm;


import org.apache.shiro.SecurityUtils;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.session.Session;
import org.apache.shiro.session.mgt.DefaultSessionManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AuthorizationService {

  private static final ThreadLocal<String> principalThreadLocal = new  ThreadLocal<>();
  private static final Map<String, AuthenticationInfo> cache = new ConcurrentHashMap<>();

  public static void setThreadLocalPrincipal(final String principal) {
    principalThreadLocal.set(principal);
  }

  public static AuthenticationInfo getAuthenticationInfo() {
    final String principal = principalThreadLocal.get() != null
            ? principalThreadLocal.get()
            : ShiroSecurityService.get().getPrincipal();
    if(principal == null || "anonymous".equals(principal)) {
      final AuthenticationInfo authenticationInfo = new AuthenticationInfo("anonymous", new HashSet<>());
      cache.put("anonymous", authenticationInfo);
      return authenticationInfo;
    }

    if (cache.containsKey(principal)) {
      return cache.get(principal);
    } else {
      final AuthenticationInfo authenticationInfo
              = new AuthenticationInfo(principal, ShiroSecurityService.get().getAssociatedRoles(principal));

      cache.put(principal, authenticationInfo);
      return authenticationInfo;
    }
  }

  public static void drop(final String principal) {
    cache.remove(principal);

    final Serializable userSession = SecurityUtils.getSubject().getSession(false).getId();
    DefaultSecurityManager securityManager = (DefaultSecurityManager) SecurityUtils.getSecurityManager();
    DefaultSessionManager sessionManager = (DefaultSessionManager) securityManager.getSessionManager();
    Collection<Session> activeSessions = sessionManager.getSessionDAO().getActiveSessions();
    for (Session session: activeSessions){
      if (userSession.equals(session.getId())) {
        session.stop();
      }
    }
    ThreadContext.remove(ThreadContext.SUBJECT_KEY);
  }
}
