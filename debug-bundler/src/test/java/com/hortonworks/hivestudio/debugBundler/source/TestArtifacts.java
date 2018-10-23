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

package com.hortonworks.hivestudio.debugBundler.source;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.debugBundler.framework.Artifact;
import com.hortonworks.hivestudio.debugBundler.framework.Params;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestArtifacts {
  private final Injector injector;
  private final List<Path> tempFiles;

  public TestArtifacts() {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    injector = Guice.createInjector(new Module() {
      @Override
      public void configure(Binder binder) {
        binder.bind(HttpClient.class).toInstance(httpClient);
        binder.bind(Configuration.class).toInstance(new Configuration(new Properties()));
      }
    });
    tempFiles = new ArrayList<>();
  }

  @After
  public void teardown() throws Exception {
    for (Path path : tempFiles) {
      Files.delete(path);
    }
    tempFiles.clear();
  }

  private void checkArtifacts(List<Artifact> artifacts, String ... names) {
    Set<String> pending = Sets.newHashSet(names);
    for (Artifact artifact : artifacts) {
      assertTrue("Unexpected artifact: " + artifact.getName(), pending.remove(artifact.getName()));
    }
    assertTrue("Artifact(s) expected but not found: " + pending, pending.isEmpty());
  }

  private Artifact findArtifact(List<Artifact> artifacts, String name) {
    for (Artifact artifact : artifacts) {
      if (artifact.getName().equals(name)) {
        return artifact;
      }
    }
    return null;
  }

  private Path getTempPath(String content) throws IOException {
    Path path = Files.createTempFile("Test-tmp", ".tmp");
    tempFiles.add(path);
    Files.copy(new ByteArrayInputStream(content.getBytes()), path,
        StandardCopyOption.REPLACE_EXISTING);
    return path;
  }

  @Test(timeout=5000)
  public void testHiveATSArtifacts() throws Exception {
    HiveATSArtifacts source = injector.getInstance(HiveATSArtifacts.class);
    Params params = new Params();

    assertFalse(source.hasRequiredParams(params));

    params.setHiveQueryId("test-hive-queryid");
    assertTrue(source.hasRequiredParams(params));

    List<Artifact> artifacts = source.getArtifacts(params);
    checkArtifacts(artifacts, "ATS/HIVE_QUERY");

    Path path = getTempPath(
        "{\"entitytype\":\"HIVE_QUERY_ID\",\"entity\":\"hive-query-test-1\",\"starttime\":" +
        "1498800924974,\"primaryfilters\":{\"executionmode\":[\"LLAP\"]},\"otherinfo\":" +
        "{\"APP_ID\":\"application_test_1\",\"DAG_ID\":\"dag_test_1_1\"},\"events\":" +
        "[{\"timestamp\":1498800925974,\"eventtype\":\"QUERY_SUBMITTED\"},{\"timestamp\":" +
        "1498800927974,\"eventtype\":\"QUERY_COMPLETED\"}]}");
    source.updateParams(params, artifacts.get(0), path);

    assertEquals("dag_test_1_1", params.getTezDagId());
    assertEquals("application_test_1", params.getTezAmAppId());
    assertEquals("LLAP", params.getAppType());
    assertEquals(1498800925974L, params.getStartTime());
    assertEquals(1498800927974L, params.getEndTime());
  }

  @Test(timeout=5000)
  public void testTezATSArtifacts() throws Exception {
    TezATSArtifacts source = injector.getInstance(TezATSArtifacts.class);
    Params params = new Params();

    assertFalse(source.hasRequiredParams(params));

    params.setTezDagId("test-tez-dag-id");
    assertTrue(source.hasRequiredParams(params));

    List<Artifact> artifacts = source.getArtifacts(params);
    checkArtifacts(artifacts, "ATS/TEZ_DAG", "ATS/TEZ_DAG_EXTRA_INFO", "ATS/TEZ_VERTEX",
        "ATS/TEZ_TASK", "ATS/TEZ_TASK_ATTEMPT");

    Path path = getTempPath(
        "{\"entitytype\":\"TEZ_DAG_ID\",\"entity\":\"dag_test_1_1\",\"starttime\":1498800924974," +
        "\"otherinfo\":{\"applicationId\":\"app_test_1\",\"callerType\":\"HIVE_QUERY_ID\"," +
        "\"callerId\":\"query_test_1\"},\"events\":[{\"timestamp\":1498800925974,\"eventtype\":" +
        "\"DAG_SUBMITTED\"},{\"timestamp\":1498800927974,\"eventtype\":\"DAG_FINISHED\"}]}");
    source.updateParams(params, findArtifact(artifacts, "ATS/TEZ_DAG"), path);

    assertEquals("app_test_1", params.getTezAmAppId());
    assertEquals("query_test_1", params.getHiveQueryId());
    assertEquals(1498800925974L, params.getStartTime());
    assertEquals(1498800927974L, params.getEndTime());

    assertFalse(params.getTezTaskLogs().isFinishedContainers());
    path = getTempPath("{\"entities\":[{\"entitytype\":\"TEZ_TASK_ATTEMPT_ID\"," +
        "\"entity\":\"attempt_1_1_2\",\"starttime\":1498800924974,\"otherinfo\":" +
        "{\"completedLogsURL\":\"http://host/path/applicationhistory/containers/" +
        "container_1/logs\\\\?nm.id=node_1:1234\"}},{\"entitytype\":\"TEZ_TASK_ATTEMPT_" +
        "ID\",\"entity\":\"attempt_1_1_3\",\"starttime\":1498800924974,\"otherinfo\":" +
        "{\"containerId\":\"container_2\",\"nodeId\":\"node_2:3456\"}}]}");
    source.updateParams(params, findArtifact(artifacts, "ATS/TEZ_TASK_ATTEMPT"), path);
    assertTrue(params.getTezTaskLogs().isFinishedContainers());
  }

  @Test(timeout=5000)
  public void testLlapDeamonLogsListArtifacts() throws Exception {
    LlapDeamonLogsListArtifacts source = injector.getInstance(
        LlapDeamonLogsListArtifacts.class);
    Params params = new Params();
    params.setHiveQueryId("hqid-1");

    assertFalse(source.hasRequiredParams(params));
    params.setAppType("LLAP");
    Params.AppLogs taskLogs = params.getTezTaskLogs();
    taskLogs.addContainer("test-node-id-1:8888", "test-container-id-1");
    taskLogs.addContainer("test-node-id-1:8888", "test-container-id-2");
    taskLogs.addContainer("test-node-id-2:8888", "test-container-id-3");
    taskLogs.finishContainers();

    assertTrue(source.hasRequiredParams(params));

    List<Artifact> artifacts = source.getArtifacts(params);
    assertEquals(3, artifacts.size());

    Path path = getTempPath("{\"containerLogsInfo\":{\"containerLogInfo\":[{\"fileName\":\"launch_container.sh\",\"fileSize\":\"4245\",\"lastModifiedTime\":\"Tue Jul 04 05:51:44 +0000 2017\"},{\"fileName\":\"directory.info\",\"fileSize\":\"10843\",\"lastModifiedTime\":\"Tue Jul 04 05:51:44 +0000 2017\"},{\"fileName\":\"slider-agent.out\",\"fileSize\":\"45\",\"lastModifiedTime\":\"Tue Jul 04 05:51:45 +0000 2017\"},{\"fileName\":\"slider-agent.log\",\"fileSize\":\"29998\",\"lastModifiedTime\":\"Tue Jul 04 05:52:02 +0000 2017\"},{\"fileName\":\"command-1.json\",\"fileSize\":\"6974\",\"lastModifiedTime\":\"Tue Jul 04 05:51:56 +0000 2017\"},{\"fileName\":\"output-1.txt\",\"fileSize\":\"1656\",\"lastModifiedTime\":\"Tue Jul 04 05:51:56 +0000 2017\"},{\"fileName\":\"errors-1.txt\",\"fileSize\":\"0\",\"lastModifiedTime\":\"Tue Jul 04 05:51:56 +0000 2017\"},{\"fileName\":\"command-2.json\",\"fileSize\":\"6937\",\"lastModifiedTime\":\"Tue Jul 04 05:52:00 +0000 2017\"},{\"fileName\":\"output-2.txt\",\"fileSize\":\"162\",\"lastModifiedTime\":\"Tue Jul 04 05:52:00 +0000 2017\"},{\"fileName\":\"errors-2.txt\",\"fileSize\":\"0\",\"lastModifiedTime\":\"Tue Jul 04 05:52:00 +0000 2017\"},{\"fileName\":\"shell.out\",\"fileSize\":\"2999\",\"lastModifiedTime\":\"Tue Jul 04 05:52:00 +0000 2017\"},{\"fileName\":\"llap-daemon-hive-ctr-e133-1493418528701-155152-01-000004.hwx.site.out\",\"fileSize\":\"14021\",\"lastModifiedTime\":\"Tue Jul 04 06:10:26 +0000 2017\"},{\"fileName\":\"status_command_stdout.txt\",\"fileSize\":\"0\",\"lastModifiedTime\":\"Tue Jul 04 06:41:27 +0000 2017\"},{\"fileName\":\"status_command_stderr.txt\",\"fileSize\":\"0\",\"lastModifiedTime\":\"Tue Jul 04 06:41:27 +0000 2017\"},{\"fileName\":\"gc.log.0.current\",\"fileSize\":\"8559\",\"lastModifiedTime\":\"Tue Jul 04 06:09:46 +0000 2017\"},{\"fileName\":\"llapdaemon_history.log\",\"fileSize\":\"2132\",\"lastModifiedTime\":\"Tue Jul 04 06:10:26 +0000 2017\"},{\"fileName\":\"llap-daemon-hive-ctr-e133-1493418528701-155152-01-000004.hwx.site.log_2017-07-04-05_1.done\",\"fileSize\":\"41968\",\"lastModifiedTime\":\"Tue Jul 04 05:53:13 +0000 2017\"},{\"fileName\":\"llap-daemon-hive-ctr-e133-1493418528701-155152-01-000004.hwx.site.log\",\"fileSize\":\"16357\",\"lastModifiedTime\":\"Tue Jul 04 06:10:26 +0000 2017\"},{\"fileName\":\"hive_20170704060941_28dd6d01-2d6c-46f8-964b-e57f26e720ff-dag_1499147207464_0004_1.log.done\",\"fileSize\":\"16967\",\"lastModifiedTime\":\"Tue Jul 04 06:09:47 +0000 2017\"},{\"fileName\":\"hive_20170704061024_bf72f7f6-5684-4e16-95ff-8725e7eba4cf-dag_1499147207464_0004_2.log.done\",\"fileSize\":\"34530\",\"lastModifiedTime\":\"Tue Jul 04 06:10:26 +0000 2017\"},{\"fileName\":\"hive_20170704061024_bf72f7f6-5684-4e16-95ff-8725e7eba4cf-dag_1499147207464_0004_2.log\",\"fileSize\":\"176\",\"lastModifiedTime\":\"Tue Jul 04 06:10:26 +0000 2017\"},{\"fileName\":\"status_command.json\",\"fileSize\":\"2896\",\"lastModifiedTime\":\"Tue Jul 04 06:41:27 +0000 2017\"}],\"logAggregationType\":\"LOCAL\",\"containerId\":\"container_1499147207464_0003_01_000002\",\"nodeId\":\"ctr-e133-1493418528701-155152-01-000004.hwx.site:45454\"}}");
    source.updateParams(params, artifacts.get(0), path);
    Params.AppLogs logs = params.getTezTaskLogs();
    assertTrue(logs.isFinishedLogs());
  }

  @Test(timeout=5000)
  public void testLlapDeamonLogsArtifacts() throws Exception {
    LlapDeamonLogsArtifacts source = injector.getInstance(LlapDeamonLogsArtifacts.class);
    Params params = new Params();

    assertFalse(source.hasRequiredParams(params));
    params.setAppType("LLAP");
    Params.AppLogs taskLogs = params.getTezTaskLogs();
    taskLogs.addLog("test-node-id-1:8888", "test-container-id-1",
        Lists.newArrayList(new Params.ContainerLogInfo("test-1", 10, "01-01-2017")));
    taskLogs.addLog("test-node-id-2:8888", "test-container-id-2",
        Lists.newArrayList(new Params.ContainerLogInfo("test-2", 10, "01-01-2017"),
            new Params.ContainerLogInfo("test-3", 10, "01-01-2017")));
    taskLogs.finishLogs();

    assertTrue(source.hasRequiredParams(params));

    List<Artifact> artifacts = source.getArtifacts(params);
    assertEquals(3, artifacts.size());
  }
}
