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
import com.hortonworks.hivestudio.hive.persistence.entities.SavedQuery;
import org.jdbi.v3.sqlobject.config.RegisterBeanMapper;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.Collection;
import java.util.Optional;

/**
 * Jdbi dao for SavedQuery
 */
@RegisterBeanMapper(SavedQuery.class)
public interface SavedQueryDao extends JdbiDao<SavedQuery, Integer> {
  @SqlQuery("select * from das.saved_query where id = :id")
  Optional<SavedQuery> findOne(@Bind("id") Integer id);

  @SqlQuery("select * from das.saved_query")
  Collection<SavedQuery> findAll();

  @SqlQuery("select * from das.saved_query where owner = :owner")
  Collection<SavedQuery> findAllByOwner(@Bind("owner") String owner);

  @SqlUpdate("insert into das.saved_query (owner, selected_database, title, query) values " +
      "( :owner, :selectedDatabase, :title, :query) ")
  @GetGeneratedKeys
  Integer insert(@BindBean SavedQuery entity);

  @SqlUpdate("delete from das.saved_query where id = :id")
  int delete(@Bind("id") Integer id);

  @SqlUpdate("update das.saved_query  set owner = :owner, selected_database = :selectedDatabase," +
      " title = :title, query = :query where id = :id" )
  int update(@BindBean SavedQuery savedQuery);
}
