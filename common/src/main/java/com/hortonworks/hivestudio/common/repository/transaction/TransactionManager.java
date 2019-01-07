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
package com.hortonworks.hivestudio.common.repository.transaction;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import com.hortonworks.hivestudio.common.repository.Identifiable;
import com.hortonworks.hivestudio.common.repository.JdbiDao;

/**
 * Transaction manager to create dao in transaction and out of transaction context.
 *
 * Register provider for dao with guice, which should use createDao to return dao instances.
 * If a transaction is running, the dao instance will be part of the transaction and is only
 * valid until the transaction finishes.
 *
 * When no transaction is running, an independent dao object is created, which will execute
 * every sql independently, creating a connection on demand.
 *
 * The method interceptor should be used with guice to ensure that annotated methods are executed
 * within a transaction.
 */
public class TransactionManager implements MethodInterceptor {

  private final Jdbi jdbi;
  private final ThreadLocal<Handle> handleThreadLocal = new ThreadLocal<>();

  public TransactionManager(Jdbi jdbi) {
    this.jdbi = jdbi;
  }

  // TODO: Verify if we should ignore methods from object class.
  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    Handle handle = handleThreadLocal.get();
    if (handle != null) {
      return invocation.proceed();
    }
    return withTransaction(() -> {
      try {
        return invocation.proceed();
      } catch (Exception e) {
        throw e;
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    });
  }

  public <T, X extends Exception> T withTransaction(Callable<T, X> callable) throws X {
    if (handleThreadLocal.get() != null) {
      return callable.call();
    }
    return jdbi.<T, X>inTransaction(handle -> {
      handleThreadLocal.set(handle);
      try {
        return callable.call();
      } finally {
        handleThreadLocal.remove();
      }
    });
  }

  public <I, E extends Identifiable<I>, D extends JdbiDao<E, I>> D createDao(Class<D> clazz) {
    Handle handle = handleThreadLocal.get();
    // TODO: An optimization would be to reuse instances.
    //   Use a thread local for request in a transaction.
    //   A global weak hash map for requests outside a transaction.
    return handle == null ? jdbi.onDemand(clazz) : handle.attach(clazz);
  }
}
