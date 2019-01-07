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
package com.hortonworks.hivestudio.hive.persistence.daos;

import com.hortonworks.hivestudio.common.repository.JdbiDao;
import com.hortonworks.hivestudio.hive.persistence.entities.SuggestedSearch;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Collection;
import java.util.Optional;

/**
 * Jdbi repository for Udf
 */
@RegisterBeanMapper(SuggestedSearch.class)
public interface SuggestedSearchDao extends JdbiDao<SuggestedSearch, Integer> {

  @SqlQuery("select * from das.searches where id = :id")
  Optional<SuggestedSearch> findOne(@Bind("id") Integer id);

  @SqlQuery("select * from das.searches")
  Collection<SuggestedSearch> findAll();

  @SqlQuery("select * from das.searches where entity = :entity AND category = :category AND owner = :owner")
  Collection<SuggestedSearch> findAllByEntityCategoryOwner(@Bind("entity") String entity, @Bind("category") String category, @Bind("owner") String owner);

  @SqlUpdate("insert into das.searches (name, category, type, entity, owner, clause, facet, columns, range, sort) values (:name, :category, :type, :entity, :owner, :clause, cast(:facet as jsonb), cast(:columns as jsonb), cast(:range as jsonb), cast(:sort as jsonb)) ")
  @GetGeneratedKeys
  Integer insert(@BindBean SuggestedSearch entity);

  @SqlUpdate("delete from das.searches where id = :id")
  int delete(@Bind("id") Integer id);

  @SqlUpdate("update das.searches  set name = :name, category = :category, type = :type, entity = :entity, owner = :owner, clause = :clause, facet = cast(:facet as jsonb), columns = cast(:columns as jsonb), range = cast(:range as jsonb), sort = cast(:sort as jsonb) where id = :id" )
  int update(@BindBean SuggestedSearch savedQuery);

}