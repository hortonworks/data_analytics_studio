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

import java.util.Collection;
import java.util.Optional;

import org.jdbi.v3.sqlobject.SqlObject;

public interface JdbiDao<Entity extends Identifiable<Id>, Id> extends SqlObject {
  /**
   * Finds the record entity with the given id
   * @param id identity of the record
   * @return return Optional of entity if found Optional.empty if not found
   */
  Optional<Entity> findOne(Id id);

//  TODO : need another api to fetch as an iterator for really big results.
  /**
   * Returns all the records for this entity type.
   * Use with caution as the returned collection can be big.
   * This all is loaded from db to memory.
   * @return Collection of all records
   */
  Collection<Entity> findAll();

  /**
   * inserts the entity
   * @param entity Entity object to be inserted.
   * @return The Integer identity generated
   */
  Id insert(Entity entity);

  /**
   * updates the entity
   * @param entity Entity object to be updated.
   * @return The updated count. Should be 1 if the entity exists with given id
   */
  int update(Entity entity);

  /**
   * Deletes the entities
   * @return number of the deleted record. should be 1 if the entity exists else 0
   */
  int delete(Id id);

}
