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
package com.hortonworks.hivestudio.eventProcessor.pipeline;

import java.io.EOFException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.HiveHookEventProto;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.history.logging.proto.DatePartitionedLogger;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hortonworks.hivestudio.common.entities.Column;
import com.hortonworks.hivestudio.common.entities.CreationSource;
import com.hortonworks.hivestudio.common.entities.DBReplicationEntity;
import com.hortonworks.hivestudio.common.entities.DagDetails;
import com.hortonworks.hivestudio.common.entities.DagInfo;
import com.hortonworks.hivestudio.common.entities.Database;
import com.hortonworks.hivestudio.common.entities.HiveQuery;
import com.hortonworks.hivestudio.common.entities.QueryDetails;
import com.hortonworks.hivestudio.common.entities.Table;
import com.hortonworks.hivestudio.common.entities.TablePartitionInfo;
import com.hortonworks.hivestudio.common.entities.VertexInfo;
import com.hortonworks.hivestudio.common.persistence.mappers.JsonArgumentFactory;
import com.hortonworks.hivestudio.common.persistence.mappers.JsonColumnMapper;
import com.hortonworks.hivestudio.common.repository.ColumnRepository;
import com.hortonworks.hivestudio.common.repository.DBReplicationRepository;
import com.hortonworks.hivestudio.common.repository.DatabaseRepository;
import com.hortonworks.hivestudio.common.repository.JdbiDao;
import com.hortonworks.hivestudio.common.repository.JdbiRepository;
import com.hortonworks.hivestudio.common.repository.TablePartitionInfoRepository;
import com.hortonworks.hivestudio.common.repository.TableRepository;
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager;
import com.hortonworks.hivestudio.common.util.TimeHelper;
import com.hortonworks.hivestudio.eventProcessor.entities.FileStatusEntity;
import com.hortonworks.hivestudio.eventProcessor.entities.FileStatusEntity.FileStatusType;
import com.hortonworks.hivestudio.eventProcessor.entities.repository.FileStatusRepository;
import com.hortonworks.hivestudio.hive.persistence.entities.File;
import com.hortonworks.hivestudio.hive.persistence.entities.SavedQuery;
import com.hortonworks.hivestudio.hive.persistence.entities.Setting;
import com.hortonworks.hivestudio.hive.persistence.entities.Udf;
import com.hortonworks.hivestudio.hive.persistence.repositories.FileRepository;
import com.hortonworks.hivestudio.hive.persistence.repositories.SavedQueryRepository;
import com.hortonworks.hivestudio.hive.persistence.repositories.SettingRepository;
import com.hortonworks.hivestudio.hive.persistence.repositories.UdfRepository;
import com.hortonworks.hivestudio.query.entities.repositories.DagDetailsRepository;
import com.hortonworks.hivestudio.query.entities.repositories.DagInfoRepository;
import com.hortonworks.hivestudio.query.entities.repositories.HiveQueryRepository;
import com.hortonworks.hivestudio.query.entities.repositories.QueryDetailsRepository;
import com.hortonworks.hivestudio.query.entities.repositories.VertexInfoRepository;
import com.hortonworks.hivestudio.reporting.dto.JoinColumnDBResult;
import com.hortonworks.hivestudio.reporting.entities.columnstat.CSDaily;
import com.hortonworks.hivestudio.reporting.entities.joincolumnstat.JCSDaily;
import com.hortonworks.hivestudio.reporting.entities.repositories.ColumnStatRepository;
import com.hortonworks.hivestudio.reporting.entities.repositories.JoinColumnStatRepository;
import com.hortonworks.hivestudio.reporting.entities.repositories.TableStatRepository;
import com.hortonworks.hivestudio.reporting.entities.tablestat.TSDaily;

import jersey.repackaged.com.google.common.collect.Lists;

public class TestLocal {
  final Jdbi jdbi;
  final TransactionManager mgr;
  final ObjectMapper mapper;

  public TestLocal() throws Exception {
    Class.forName("org.postgresql.Driver");
    Properties props = new Properties();
    props.setProperty("user","hstest");
    props.setProperty("password","hstest");
    mapper = new ObjectMapper();
    jdbi = Jdbi.create("jdbc:postgresql://localhost/hsdev", props);
    jdbi.installPlugin(new SqlObjectPlugin());
    jdbi.registerArgument(new JsonArgumentFactory(mapper, ArrayNode.class));
    jdbi.registerArgument(new JsonArgumentFactory(mapper, ObjectNode.class));
    jdbi.registerColumnMapper(ArrayNode.class, new JsonColumnMapper<>(new ObjectMapper(), ArrayNode.class));
    jdbi.registerColumnMapper(ObjectNode.class, new JsonColumnMapper<>(new ObjectMapper(), ObjectNode.class));
    mgr = new TransactionManager(jdbi);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private <T extends JdbiRepository> T makeRepo(Class<T> clazz) {
    Class<? extends JdbiDao> daoClass = JdbiRepository.getDaoClass(clazz);
    try {
      Object obj = clazz.getConstructor(daoClass).newInstance(mgr.createDao(daoClass));
      return (T)obj;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("rawtypes")
  private <T extends JdbiRepository, D extends JdbiDao, R> R withTransaction(Function<T, R> f, Class<T> clazz) {
    return mgr.withTransaction(() -> f.apply(makeRepo(clazz)));
  }

  @SuppressWarnings("rawtypes")
  private <T1 extends JdbiRepository, D1 extends JdbiDao, T2 extends JdbiRepository, D2 extends JdbiDao, R>
      R withTransaction(BiFunction<T1, T2, R> f, Class<T1> clazz1, Class<T2> clazz2) {
    return mgr.withTransaction(() -> f.apply(makeRepo(clazz1), makeRepo(clazz2)));
  }

  public void testDagInfo() throws Exception {
    DagInfo dagInfo = new DagInfo();
    dagInfo.setDagId("dag_1");
    dagInfo.setDagName("test_name");

    HiveQuery hiveQuery = new HiveQuery();
    hiveQuery.setQuery("Test query");
    hiveQuery.setQueryId("hive_1");
    hiveQuery.setTablesWritten(new ObjectMapper().createArrayNode());

    DagInfo dag = withTransaction(repo -> repo.save(dagInfo), DagInfoRepository.class);
    dag.setAmWebserviceVer("AM version");
    withTransaction((repo, hrepo) -> {
      hrepo.save(hiveQuery);
      dag.setHiveQueryId(hiveQuery.getId());
      return repo.save(dag);
    }, DagInfoRepository.class, HiveQueryRepository.class);

    DagInfo dag2 = withTransaction(repo -> repo.findOne(dag.getId()).get(), DagInfoRepository.class);
    System.out.println(dag2.getDagName());
    System.out.println(dag2.getAmWebserviceVer());

    dag2 = withTransaction(repo -> repo.findByDagId("dag_1").get(), DagInfoRepository.class);
    System.out.println(dag2.getDagName());
    System.out.println(dag2.getAmWebserviceVer());

    dag2 = withTransaction(repo -> repo.getByHiveQueryTableId(hiveQuery.getId()).get(), DagInfoRepository.class);
    System.out.println(dag2.getDagName());
    System.out.println(dag2.getAmWebserviceVer());

    withTransaction((repo, hrepo) -> {
      repo.delete(dag.getId());
      return hrepo.delete(hiveQuery.getId());
    }, DagInfoRepository.class, HiveQueryRepository.class);
  }

  public void testFile() throws Exception {
    Class<FileRepository> repoClass = FileRepository.class;
    File repl = new File();
    repl.setName("file");
    repl.setOwner("user1");
    repl.setPath("/dev/null");

    withTransaction(r -> r.save(repl), repoClass);
    repl.setPath("/dev/null2");
    withTransaction(r -> r.save(repl), repoClass);
    withTransaction(r -> r.findOne(repl.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.findAllByOwner("user1"), repoClass).iterator().next();
    withTransaction(r -> r.delete(repl.getId()), repoClass);
  }

  public void testUdf() throws Exception {
    File file = new File();
    file.setName("file");
    file.setOwner("user1");
    file.setPath("/dev/null");
    withTransaction(r -> r.save(file), FileRepository.class);

    Class<UdfRepository> repoClass = UdfRepository.class;
    Udf udf = new Udf();
    udf.setName("udf");
    udf.setFileResource(file.getId());
    udf.setClassname("com.util.List");
    udf.setOwner("user1");

    withTransaction(r -> r.save(udf), repoClass);
    udf.setName("udf1");
    withTransaction(r -> r.save(udf), repoClass);
    withTransaction(r -> r.findOne(file.getId()), repoClass).get();
    withTransaction(r -> r.findOne(udf.getId()), repoClass).get();
    withTransaction(r -> r.findAllByOwner("user1"), repoClass).iterator().next();
    withTransaction(r -> r.findAllByOwner("user1"), repoClass).iterator().next();
    withTransaction(r -> r.delete(udf.getId()), repoClass);

    withTransaction(r -> r.delete(file.getId()), FileRepository.class);
  }

  public void testDBRepl() throws Exception {
    Class<DBReplicationRepository> repoClass = DBReplicationRepository.class;
    DBReplicationEntity repl = new DBReplicationEntity();
    repl.setDatabaseName("db1");
    repl.setLastReplicationId("10");

    withTransaction(r -> r.save(repl), repoClass);
    repl.setLastReplicationId("15");
    withTransaction(r -> r.save(repl), repoClass);
    withTransaction(r -> r.findOne(repl.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.getByDatabaseName("db1"), repoClass).get();
    withTransaction(r -> r.setReplicationId("db1", "20"), repoClass);
    withTransaction(r -> r.startReplProcessing("db1"), repoClass);
    withTransaction(r -> r.finishReplProcessing("db1", 120000, "30"), repoClass);
    withTransaction(r -> r.getByDatabaseName("db1"), repoClass).get();
    withTransaction(r -> r.delete(repl.getId()), repoClass);
  }

  public void testSetting() throws Exception {
    Class<SettingRepository> repoClass = SettingRepository.class;
    Setting setting = new Setting();
    setting.setOwner("user1");
    setting.setKey("key1");
    setting.setValue("value1");

    withTransaction(r -> r.save(setting), repoClass);
    setting.setValue("value2");
    withTransaction(r -> r.save(setting), repoClass);
    withTransaction(r -> r.findOne(setting.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.findByKeyAndOwner("key1", "user1"), repoClass).get();
    withTransaction(r -> r.findAllByOwner("user1"), repoClass).iterator().next();
    withTransaction(r -> r.delete(setting.getId()), repoClass);
  }

  public void testFileStatus() throws Exception {
    Class<FileStatusRepository> repoClass = FileStatusRepository.class;
    FileStatusEntity setting = new FileStatusEntity();
    setting.setDate(LocalDate.ofEpochDay(0));
    setting.setFileName("file1");
    setting.setFileType(FileStatusType.HIVE);
    setting.setLastEventTime(1232141241L);
    setting.setFinished(false);

    withTransaction(r -> r.save(setting), repoClass);
    setting.setPosition(100L);
    withTransaction(r -> r.save(setting), repoClass);
    withTransaction(r -> r.findOne(setting.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.findAllByType(FileStatusType.HIVE), repoClass).iterator().next();
    withTransaction(r -> r.delete(setting.getId()), repoClass);
  }

  public void testSavedQuery() throws Exception {
    Class<SavedQueryRepository> repoClass = SavedQueryRepository.class;
    SavedQuery sq = new SavedQuery();
    sq.setOwner("user1");
    sq.setQuery("select blah from blah");
    sq.setSelectedDatabase("db1");
    sq.setShortQuery("weird one");
    sq.setTitle("Awesome");

    withTransaction(r -> r.save(sq), repoClass);
    sq.setTitle("Cool");
    withTransaction(r -> r.save(sq), repoClass);
    withTransaction(r -> r.findOne(sq.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.findAllByOwner("user1"), repoClass).iterator().next();
    withTransaction(r -> r.delete(sq.getId()), repoClass);
  }

  public void testTableStats() throws Exception {
    Class<TableStatRepository.Daily> repoClass = TableStatRepository.Daily.class;
    TSDaily ts = new TSDaily();
    LocalDate date = LocalDate.now();
    LocalDate tom = date.plusDays(1);
    ts.setTableId(1);
    ts.setDate(date);
    ts.setBytesRead(10L);
    ts.setRecordsRead(1L);
    ts.setBytesWritten(10L);
    ts.setRecordsWritten(1L);
    ts.setReadCount(1);
    ts.setWriteCount(0);

    withTransaction(r -> r.save(ts), repoClass);
    ts.setBytesWritten(20L);
    withTransaction(r -> r.save(ts), repoClass);
    withTransaction(r -> r.findOne(ts.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.findByDatabaseAndTimeRange(1, date, tom), repoClass).iterator().next();
    withTransaction(r -> r.findByTablesAndTimeRange(
        Collections.singletonList(1), date, tom), repoClass).iterator().next();

    withTransaction(r -> r.rollup(date), TableStatRepository.Weekly.class);
    withTransaction(r -> r.rollup(date), TableStatRepository.Monthly.class);
    withTransaction(r -> r.rollup(date), TableStatRepository.Quarterly.class);

    withTransaction(r -> r.findByDatabaseAndTimeRange(1, TimeHelper.getWeekStartDate(date), tom),
        TableStatRepository.Weekly.class).iterator().next();

    withTransaction(r -> r.delete(ts.getId()), repoClass);
  }

  public void testColumnStats() throws Exception {
    Class<ColumnStatRepository.Daily> repoClass = ColumnStatRepository.Daily.class;
    CSDaily cs = new CSDaily();
    LocalDate date = LocalDate.now();
    LocalDate tom = date.plusDays(1);
    cs.setColumnId(1);
    cs.setDate(date);
    cs.setAggregationCount(1);
    cs.setFilterCount(2);
    cs.setJoinCount(3);
    cs.setProjectionCount(4);

    withTransaction(r -> r.save(cs), repoClass);
    cs.setAggregationCount(10);
    withTransaction(r -> r.save(cs), repoClass);
    withTransaction(r -> r.findOne(cs.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.findByDatabaseAndTimeRange(1, date, tom), repoClass).iterator().next();
    withTransaction(r -> r.findByTablesAndTimeRange(
        Collections.singletonList(1), date, tom), repoClass).iterator().next();

    withTransaction(r -> r.rollup(date), ColumnStatRepository.Weekly.class);
    withTransaction(r -> r.rollup(date), ColumnStatRepository.Monthly.class);
    withTransaction(r -> r.rollup(date), ColumnStatRepository.Quarterly.class);

    withTransaction(r -> r.findByDatabaseAndTimeRange(1, TimeHelper.getWeekStartDate(date), tom),
        ColumnStatRepository.Weekly.class).iterator().next();

    withTransaction(r -> r.delete(cs.getId()), repoClass);
  }

  public void testJoinColumnStats() throws Exception {
    Class<JoinColumnStatRepository.Daily> repoClass = JoinColumnStatRepository.Daily.class;
    JCSDaily cs = new JCSDaily();
    LocalDate date = LocalDate.now();
    LocalDate tom = date.plusDays(1);
    cs.setLeftColumn(1);
    cs.setRightColumn(2);
    cs.setDate(date);
    cs.setAlgorithm("alg1");
    cs.setInnerJoinCount(1);
    cs.setFullOuterJoinCount(2);
    cs.setLeftOuterJoinCount(3);
    cs.setRightOuterJoinCount(4);
    cs.setLeftSemiJoinCount(5);
    cs.setTotalJoinCount(6);
    cs.setLeftSemiJoinCount(7);
    cs.setUniqueJoinCount(8);
    cs.setUnknownJoinCount(0);

    withTransaction(r -> r.save(cs), repoClass);
    cs.setUnknownJoinCount(9);
    withTransaction(r -> r.save(cs), repoClass);
    withTransaction(r -> r.findOne(cs.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.findByDatabaseAndDateRange(1, date, tom, "alg1"), repoClass)
        .iterator().next();
    withTransaction(r -> r.findByTableAndDateRange(1, date, tom), repoClass).iterator().next();
    JoinColumnDBResult res = withTransaction(r -> r.findByColumnsAndTimeRange(
        Lists.newArrayList(1, 2), date, tom), repoClass).iterator().next();
    System.out.println("Left col: " + res.getDailyOptional().get().getLeftColumnName());

    withTransaction(r -> r.rollup(date), JoinColumnStatRepository.Weekly.class);
    withTransaction(r -> r.rollup(date), JoinColumnStatRepository.Monthly.class);
    withTransaction(r -> r.rollup(date), JoinColumnStatRepository.Quarterly.class);

    withTransaction(r -> r.findByDatabaseAndDateRange(1, TimeHelper.getWeekStartDate(date),
        tom, null), JoinColumnStatRepository.Weekly.class).iterator().next();

    withTransaction(r -> r.delete(cs.getId()), repoClass);
  }

  public void testVertexInfo() throws Exception {
    DagInfoRepository dagRepo = makeRepo(DagInfoRepository.class);
    DagInfo dagInfo = new DagInfo();
    dagInfo.setDagId("dag1");
    dagRepo.save(dagInfo);

    Class<VertexInfoRepository> repoClass = VertexInfoRepository.class;
    VertexInfo vi = new VertexInfo();
    vi.setVertexId("vid1");
    vi.setDagId(dagInfo.getId());
    vi.setClassName("TestVertex");
    vi.setDomainId("random_domain");
    vi.setInitRequestedTime(1L);
    vi.setStartRequestedTime(2L);
    vi.setStartTime(3L);
    vi.setEndTime(5L);
    vi.setTaskCount(10);
    vi.setSucceededTaskCount(10);
    vi.setKilledTaskAttemptCount(2);
    vi.setKilledTaskCount(2);
    vi.setFailedTaskAttemptCount(2);
    vi.setFailedTaskCount(0);
    vi.setCompletedTaskCount(10);
    vi.setCounters(mapper.createArrayNode().add("test counter"));
    vi.setEvents(mapper.createArrayNode().add("test event"));
    vi.setStats(mapper.createObjectNode().put("testKey", "test value"));

    withTransaction(r -> r.save(vi), repoClass);
    vi.setTaskCount(10);
    withTransaction(r -> r.save(vi), repoClass);
    withTransaction(r -> r.findOne(vi.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.findByVertexId("vid1"), repoClass).get();
    withTransaction(r -> r.findAllByDagId("dag1"), repoClass).iterator().next();
    withTransaction(r -> r.delete(vi.getId()), repoClass);

    dagRepo.delete(dagInfo.getId());
  }

  public void testDatabase() throws Exception {
    Class<DatabaseRepository> repoClass = DatabaseRepository.class;
    Database db = new Database();
    Date now = new Date();

    db.setName("db");
    db.setCreateTime(now);
    db.setDropped(false);
    db.setDroppedAt(now);
    db.setLastUpdatedAt(now);
    db.setCreationSource(CreationSource.EVENT_PROCESSOR);

    HashSet<String> dbNames = new HashSet<>();
    dbNames.add(db.getName());

    System.out.println("DB id: " + withTransaction(r -> r.save(db), repoClass).getId());
    db.setCreationSource(CreationSource.REPLICATION);
    withTransaction(r -> r.upsert(db), repoClass);

    withTransaction(r -> r.findOne(db.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.getAllByDatabaseNames(dbNames), repoClass).iterator().next();
    withTransaction(r -> r.getByDatabaseNameAndNotDropped(db.getName()), repoClass);
    withTransaction(r -> r.getAllNotDropped(), repoClass).iterator().next();

    withTransaction(r -> r.save(db), repoClass); // Entity with id, must call update function in DAO

    withTransaction(r -> r.delete(db.getId()), repoClass);

  }

  public void testTable() throws Exception {
    DatabaseRepository dbRepo = makeRepo(DatabaseRepository.class);
    Database db = new Database();
    db.setName("db_1");
    db.setDropped(Boolean.FALSE);
    db.setCreationSource(CreationSource.REPLICATION);
    dbRepo.save(db);

    Class<TableRepository> repoClass = TableRepository.class;
    Table table = new Table();
    table.setName("table_name");
    table.setCreationSource(CreationSource.REPLICATION);
    table.setDropped(Boolean.FALSE);
    table.setDbId(db.getId());

    withTransaction(r -> r.save(table), repoClass);

    withTransaction(r -> r.findOne(table.getId()), repoClass);
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.getAllForDatabase(table.getDbId()), repoClass).iterator().next();
    withTransaction(r -> r.getAllForDatabaseAndNotDropped(db.getId()), repoClass).iterator().next();
    withTransaction(r -> r.getAllForDatabaseAndNotDroppedAndSynced(db.getId()), repoClass).iterator().next();
    withTransaction(r -> r.getAllForTables(Arrays.asList(table.getId())), repoClass).iterator().next();
    withTransaction(r -> r.getTableAndDatabaseByNames(
        Collections.singletonMap("db_name", Collections.singleton(table.getName()))), repoClass)
        .iterator().next();
    withTransaction(r -> r.getByDBNameTableNameAndNotDropped("db1", table.getName()), repoClass);
    withTransaction(r -> r.getByDBNameTableNameAndNotDroppedAndSynced("db1", table.getId().toString()), repoClass);
    withTransaction(r -> r.getByDatabaseIdAndTableNameAndNotDroppedAndSynced(table.getDbId(), table.getName()), repoClass);

    withTransaction(r -> r.delete(table.getId()), repoClass);

    withTransaction(r -> r.upsert(table), repoClass);
    withTransaction(r -> r.delete(table.getId()), repoClass);

    dbRepo.delete(db.getId());
  }

  public void testColumn() throws Exception {

    DatabaseRepository dbRepo = makeRepo(DatabaseRepository.class);
    Database db = new Database();
    db.setName("db_1");
    db.setDropped(Boolean.FALSE);
    db.setCreationSource(CreationSource.REPLICATION);
    dbRepo.save(db);

    TableRepository tableRepo = makeRepo(TableRepository.class);
    Table table = new Table();
    table.setName("table_name");
    table.setDropped(Boolean.FALSE);
    table.setCreationSource(CreationSource.REPLICATION);
    table.setDbId(db.getId());
    tableRepo.save(table);

    Class<ColumnRepository> repoClass = ColumnRepository.class;

    Column column = new Column();
    column.setName("col_1");
    column.setTableId(table.getId());
    column.setDropped(Boolean.FALSE);
    column.setCreationSource(CreationSource.REPLICATION);

    withTransaction(r -> r.save(column), repoClass); // Test update

    withTransaction(r -> r.findOne(column.getId()), repoClass);
    withTransaction(r -> r.getAllForTableNotDropped(table.getId()), repoClass).iterator().next();
    withTransaction(r -> r.getAllForTablesGroupedByTable(Arrays.asList(table.getId())), repoClass); // Returns a map
    withTransaction(r -> r.getAllForDatabaseGroupedByTable(db.getId()), repoClass); // Returns a map
    withTransaction(r -> r.getAllByColumnAndTableAndDatabases(
      Collections.singletonMap(db.getName(),
          Collections.singletonMap(table.getName(), Collections.singleton(column.getName())))
    ), repoClass).iterator().next();
    withTransaction(r -> r.markColumnDroppedForTable(table.getId(), new Date()), repoClass);

    withTransaction(r -> r.delete(column.getId()), repoClass);

    withTransaction(r -> r.upsert(column), repoClass); // Test insert
    withTransaction(r -> r.delete(column.getId()), repoClass);

    tableRepo.delete(table.getId());
    dbRepo.delete(db.getId());
  }

  public void testTablePartitionInfo() throws Exception {
    DatabaseRepository dbRepo = makeRepo(DatabaseRepository.class);
    Database db = new Database();
    db.setName("db_1");
    db.setDropped(Boolean.FALSE);
    db.setCreationSource(CreationSource.REPLICATION);
    dbRepo.save(db);

    TableRepository tableRepo = makeRepo(TableRepository.class);
    Table table = new Table();
    table.setName("table_name");
    table.setDropped(Boolean.FALSE);
    table.setCreationSource(CreationSource.REPLICATION);
    table.setDbId(db.getId());
    tableRepo.save(table);

    Class<TablePartitionInfoRepository> repoClass = TablePartitionInfoRepository.class;
    TablePartitionInfo partitionInfo = new TablePartitionInfo();
    partitionInfo.setTableId(table.getId());
    partitionInfo.setPartitionName("/col1=val1");
    partitionInfo.setRawDataSize(10l);
    partitionInfo.setNumRows(1);
    partitionInfo.setNumFiles(1);

    withTransaction(r -> r.upsert(partitionInfo), repoClass);

    withTransaction(r -> r.findOne(partitionInfo.getId()), repoClass);
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.getOne(partitionInfo.getTableId(), partitionInfo.getPartitionName()), repoClass);
    withTransaction(r -> r.getAllForTableNotDropped(partitionInfo.getTableId()), repoClass).iterator().next();

    withTransaction(r -> r.delete(partitionInfo.getId()), repoClass);

    withTransaction(r -> r.upsert(partitionInfo), repoClass);
    withTransaction(r -> r.delete(partitionInfo.getId()), repoClass);

    tableRepo.delete(table.getId());
    dbRepo.delete(db.getId());
  }

  public void testQueryDetails() throws Exception {
    HiveQuery hiveQuery = new HiveQuery();
    hiveQuery.setQuery("Test query");
    hiveQuery.setQueryId("hive_1");
    hiveQuery.setTablesWritten(new ObjectMapper().createArrayNode());
    HiveQueryRepository hRepo = makeRepo(HiveQueryRepository.class);
    hRepo.save(hiveQuery);

    Class<QueryDetailsRepository> repoClass = QueryDetailsRepository.class;
    QueryDetailsRepository qdRepo = makeRepo(repoClass);

    QueryDetails details = new QueryDetails();
    details.setConfiguration(mapper.createObjectNode());
    details.setExplainPlan(mapper.createObjectNode());
    qdRepo.save(details);

    details.setPerf(mapper.createObjectNode());
    details.setHiveQueryId(hiveQuery.getId());
    qdRepo.save(details);

    withTransaction(r -> r.findOne(details.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.findByHiveQueryId("hive_1"), repoClass).get();
    withTransaction(r -> r.findByDagId("hive_1"), repoClass);

    qdRepo.delete(details.getId());
    hRepo.delete(hiveQuery.getId());
  }

  public void testDagDetails() throws Exception {
    DagInfo dagInfo = new DagInfo();
    dagInfo.setDagId("dag_1");
    dagInfo.setDagName("test_name");
    DagInfo dag = withTransaction(repo -> repo.save(dagInfo), DagInfoRepository.class);

    HiveQuery hiveQuery = new HiveQuery();
    hiveQuery.setQuery("Test query");
    hiveQuery.setQueryId("hive_1");
    hiveQuery.setTablesWritten(new ObjectMapper().createArrayNode());
    HiveQueryRepository hRepo = makeRepo(HiveQueryRepository.class);
    hRepo.save(hiveQuery);

    Class<DagDetailsRepository> repoClass = DagDetailsRepository.class;
    DagDetailsRepository qdRepo = makeRepo(repoClass);

    DagDetails details = new DagDetails();
    details.setDagInfoId(dag.getId());
    details.setDagPlan(mapper.createObjectNode());
    details.setVertexNameIdMapping(mapper.createObjectNode());
    details.setDiagnostics("Nothing");
    qdRepo.save(details);

    details.setCounters(mapper.createArrayNode());
    details.setHiveQueryId(hiveQuery.getId());
    qdRepo.save(details);

    withTransaction(r -> r.findOne(details.getId()), repoClass).get();
    withTransaction(r -> r.findAll(), repoClass).iterator().next();
    withTransaction(r -> r.findByHiveQueryId("hive_1"), repoClass).iterator().next();
    withTransaction(r -> r.findByDagId("dag_1"), repoClass).get();

    withTransaction(repo -> repo.delete(dag.getId()), DagInfoRepository.class);
    qdRepo.delete(details.getId());
    hRepo.delete(hiveQuery.getId());
  }

  public static void main(String[] args) throws Exception {
     readTezFile();
//    new TestLocal().testQueryDetails();
//    new TestLocal().testDagDetails();
//    new TestLocal().testVertexInfo();
//    new TestLocal().testDatabase();
//    new TestLocal().testTable();
//    new TestLocal().testColumn();
//    new TestLocal().testTablePartitionInfo();
  }

  public static void readTezFile() throws Exception {
    Path baseDir = new Path("/Users/harishjp/debug/");
    Configuration conf = new Configuration(false);
    DatePartitionedLogger<HistoryEventProto> logger = new DatePartitionedLogger<>(
        HistoryEventProto.PARSER, baseDir, conf, SystemClock.getInstance());
    ProtoMessageReader<HistoryEventProto> reader = logger.getReader(
        new Path("/Users/harishjp/debug/dag_1533865427369_0118_34_1"));
    // reader.setOffset(1858952L);
    // reader.setOffset(1858972L);
    while (true) {
      try {
        HistoryEventProto evt = reader.readEvent();
        if (evt == null) {
          System.out.println("EOF reached: " + reader.getOffset());
          break;
        }
        System.out.println(evt.getEventType());
      } catch (EOFException e) {
        System.out.println("EOF ex reached: " + reader.getOffset());
        break;
      }
    }
    reader.close();
  }

  public static void readFile() throws Exception {
    Path baseDir = new Path("/tmp/hjp-ep");
    Configuration conf = new Configuration(false);
    DatePartitionedLogger<HiveHookEventProto> logger = new DatePartitionedLogger<>(
        HiveHookEventProto.PARSER, baseDir, conf, SystemClock.getInstance());
    ProtoMessageReader<HiveHookEventProto> reader = logger.getReader(
        new Path("/Users/harishjp/devel/das_private/saved/dt/hive_4d678d65-500e-455b-b07d-e1bdac10aae5"));
    // reader.setOffset(1858952L);
    reader.setOffset(1858972L);
    while (true) {
      try {
        HiveHookEventProto evt = reader.readEvent();
        if (evt == null) {
          System.out.println("EOF reached: " + reader.getOffset());
          break;
        }
        if (evt.getHiveQueryId().equals("hive_20180731142429_50196cad-636d-4ec6-bdb6-38e781108bd2") ) {
          System.out.println(evt.getEventType() + ":" + evt.getHiveQueryId() + ": ======");
        } else {
          System.out.println(evt.getEventType() + ":" + evt.getHiveQueryId() + " : " + reader.getOffset());
        }
      } catch (EOFException e) {
        System.out.println("EOF ex reached: " + reader.getOffset());
        break;
      }
    }
    reader.close();
  }

  public static void testFileReader() throws Exception {
    Path baseDir = new Path("/Users/harishjp/test_data");
    Configuration conf = new Configuration(false);
    DatePartitionedLogger<HiveHookEventProto> logger = new DatePartitionedLogger<>(
        HiveHookEventProto.PARSER, baseDir, conf, SystemClock.getInstance());
    List<FileStatus> newFiles = logger.scanForChangedFiles(
        "date=1970-01-01", Collections.emptyMap());
    for (FileStatus status : newFiles) {
      Path path = status.getPath();
      System.out.println("opening Path: " + path);
      ProtoMessageReader<HiveHookEventProto> reader = logger.getReader(path);
      // reader.setOffset(35605);
      List<Long> offsets = new ArrayList<>();
      offsets.add(reader.getOffset());
      HiveHookEventProto evt = reader.readEvent();
      while (evt != null) {
        System.out.println(evt.getEventType() + ", " + reader.getOffset());
        offsets.add(reader.getOffset());
        evt = reader.readEvent();
        baseDir.getFileSystem(conf).close();
      }
      System.out.println("finished Path: " + path);
      reader.close();

      reader = logger.getReader(path);
      for (Long offset : offsets) {
        reader.setOffset(offset);
        reader.readEvent();
      }
    }
  }
}
