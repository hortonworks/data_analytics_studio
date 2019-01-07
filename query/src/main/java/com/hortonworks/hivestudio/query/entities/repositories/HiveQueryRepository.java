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
package com.hortonworks.hivestudio.query.entities.repositories;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import org.jdbi.v3.core.mapper.JoinRow;
import org.jdbi.v3.core.mapper.JoinRowMapper;
import org.jdbi.v3.core.mapper.reflect.BeanMapper;
import org.jdbi.v3.core.mapper.reflect.ConstructorMapper;

import com.hortonworks.hivestudio.common.dto.FacetEntry;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.repository.JdbiRepository;
import com.hortonworks.hivestudio.query.entities.daos.HiveQueryDao;

import lombok.extern.slf4j.Slf4j;

/**
 * JDBI repository for Hive Query
 */
@Slf4j
public class HiveQueryRepository extends JdbiRepository<HiveQuery, Long, HiveQueryDao> {

  @Inject
  public HiveQueryRepository(HiveQueryDao dao){
    super(dao);
  }

  public List<JoinRow> executeSearchQuery(String query, Map<String, Object> parameters) {
    return getDao().withHandle(handle -> {
      handle.registerRowMapper(BeanMapper.factory(HiveQuery.class, "hq"));
      handle.registerRowMapper(BeanMapper.factory(DagInfo.class, "di"));
      handle.registerRowMapper(JoinRowMapper.forTypes(HiveQuery.class, DagInfo.class));
      return handle.createQuery(query).bindMap(parameters).mapTo(JoinRow.class).list();
    });
  }

  public Long executeSearchCountQuery(String countQuery, Map<String, Object> parameters) {
    return getDao().withHandle(handle -> handle.createQuery(countQuery).bindMap(parameters)
        .mapTo(Long.class).findFirst().orElse(0l));
  }

  public List<FacetEntry> executeFacetQuery(String query, Map<String, Object> parameters) {
    return getDao().withHandle(handle -> {
      handle.registerRowMapper(ConstructorMapper.factory(FacetEntry.class));
      return handle.createQuery(query).bindMap(parameters).mapTo(FacetEntry.class).list();
    });
  }

  public Optional<HiveQuery> findByHiveQueryId(String queryId) {
    return dao.findByHiveQueryId(queryId);
  }

  public void updateQueriesAsProcessed(List<Long> ids) {
    int count = dao.updateProcessed(ids);
    log.info("{} queries are updated as processed.",  count);
  }

  public void updateStats(Long id, Long endTime,Long cpuTime,
                          Long physicalMemory, Long virtualMemory, Long dataRead,
                          Long dataWritten) {
    dao.updateStats(id, endTime, cpuTime,
        physicalMemory, virtualMemory, dataRead,
        dataWritten);
  }

  @Override
  public Optional<HiveQuery> findOne(Long id) {
    return dao.findOne(id);
  }

  @Override
  public Collection<HiveQuery> findAll() {
    return dao.findAll();
  }

  @Override
  public HiveQuery save(HiveQuery entity) {
    if (entity.getId() == null) {
      Long id = dao.insert(entity);
      entity.setId(id);
    } else {
      dao.update(entity);
    }

    return entity;
  }

  @Override
  public boolean delete(Long id) {
    return !(0 == dao.delete(id));
  }
}
