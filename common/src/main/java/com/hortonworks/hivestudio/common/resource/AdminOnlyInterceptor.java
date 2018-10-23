/*
 *
 * HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 * (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 * This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 * Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 * to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 * properly licensed third party, you do not have any rights to this code.
 *
 * If this code is provided to you under the terms of the AGPLv3:
 * (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 * (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *   LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 * (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *   FROM OR RELATED TO THE CODE; AND
 * (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *   DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *   DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *   OR LOSS OR CORRUPTION OF DATA.
 *
 */
package com.hortonworks.hivestudio.common.resource;

import com.hortonworks.hivestudio.common.AppAuthentication;
import com.hortonworks.hivestudio.common.exception.NotPermissibleException;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

/**
 *
 */
public class AdminOnlyInterceptor implements MethodInterceptor {
  /**
   * checks if arguments have {@link RequestContext}.
   * If not found throw {@link IllegalArgumentException}
   * else check user-role
   * if admin proceed
   * else throw {@link com.hortonworks.hivestudio.common.exception.NotPermissibleException}
   *
   * @param methodInvocation
   * @return
   * @throws Throwable
   */
  @Override
  public Object invoke(MethodInvocation methodInvocation) throws Throwable {
    Object[] arguments = methodInvocation.getArguments();

    // ignore interception of methods of Object class
    Method method = methodInvocation.getMethod();
    List<Method> methodsOfObjectClass = Arrays.asList(Object.class.getMethods());
    if(methodsOfObjectClass.contains(method)){
      return methodInvocation.proceed();
    }

    // check role in request context
    RequestContext requestContext = null;
    for (Object arg : arguments) {
      if (arg instanceof RequestContext) {
        requestContext = (RequestContext) arg;
      }
    }
    if (null == requestContext) {
      throw new IllegalArgumentException("No particular user was defined for this call.");
    } else {
      AppAuthentication.Role role = requestContext.getRole();
      if (role.equals(AppAuthentication.Role.ADMIN)) {
        return methodInvocation.proceed();
      } else {
        throw new NotPermissibleException(String.format("This action is not permissible for user %s", requestContext.getUsername()));
      }
    }
  }
}
