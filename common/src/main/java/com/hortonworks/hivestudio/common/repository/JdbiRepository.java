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
package com.hortonworks.hivestudio.common.repository;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import org.jdbi.v3.core.HandleCallback;

import com.google.inject.TypeLiteral;

public abstract class JdbiRepository<Entity extends Identifiable<Id>, Id, Dao extends JdbiDao<Entity, Id>>
    implements Repository<Entity, Id> {
  protected final Dao dao;

  protected JdbiRepository(Dao dao) {
    this.dao = dao;
  }

  protected Dao getDao() {
    return this.dao;
  }

  protected <T, X extends Exception> void withHandle(HandleCallback<T, X> callback) throws X {
    dao.withHandle(callback);
  }

/*
Trying to reuse the following code across repositories has a few issues.
- java failed in runtime with save NoSuchMethodError in lambda, which went away with a full rebuild
- jdbi throws an exception "org.jdbi.v3.core.mapper.NoSuchMapperException:No mapper registered
  for type class java.lang.Object". Followed the code path in ide, it went away.

Putting this away for another time and using working around found by Niti for now.
*/

//  @Override
//  public Optional<Entity> findOne(Id id) {
//    return dao.findOne(id);
//  }
//
//  @Override
//  public Collection<Entity> findAll() {
//    return dao.findAll();
//  }
//
//  @Override
//  public Entity save(Entity entity) {
//    if (entity.getId() == null) {
//      Id id = dao.insert(entity);
//      entity.setId(id);
//    } else {
//      dao.update(entity);
//    }
//
//    return entity;
//  }
//
//  @Override
//  public boolean delete(Id id) {
//    int deleted = dao.delete(id);
//    return !(deleted == 0);
//  }

  @SuppressWarnings("unchecked")
  public static <R extends JdbiRepository<Entity, Id, Dao>, Entity extends Identifiable<Id>, Id,
      Dao extends JdbiDao<Entity, Id>> Class<Dao> getDaoClass(Class<R> jdbiRespositoryClass) {
    return (Class<Dao>) getClassOfGenericParameter(jdbiRespositoryClass, 2);
  }

  private static Class<?> getClassOfGenericParameter(Class<?> jdbiRespositoryClass, int parameterIndex) {
    Class<?> daoClass = null;
    Type type = jdbiRespositoryClass.getGenericSuperclass();
    if (type instanceof ParameterizedType) {
      ParameterizedType pType = (ParameterizedType) type;
      daoClass = (Class<?>) pType.getActualTypeArguments()[parameterIndex];
    } else {
      // Because of guice proxying the object the control will come here.
      // We try to infer the type using TypeLiterals
      Type guiceType = TypeLiteral.get(jdbiRespositoryClass).getSupertype(JdbiRepository.class).getType();
      if (guiceType instanceof ParameterizedType) {
        ParameterizedType pType = (ParameterizedType) guiceType;
        daoClass = (Class<?>) pType.getActualTypeArguments()[parameterIndex];
      } else {
        throw new IllegalArgumentException("Cannot infer the Dao class by reflection");
      }
    }
    return daoClass;
  }
}
