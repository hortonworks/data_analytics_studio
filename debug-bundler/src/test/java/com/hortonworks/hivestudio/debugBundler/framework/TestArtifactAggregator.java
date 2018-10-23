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

package com.hortonworks.hivestudio.debugBundler.framework;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.query.services.DagInfoService;
import com.hortonworks.hivestudio.query.services.HiveQueryService;
import com.hortonworks.hivestudio.query.services.QueryDetailsService;
import com.hortonworks.hivestudio.query.services.VertexInfoService;

public class TestArtifactAggregator {
  private ExecutorService service;
  private Configuration conf;
  private Params params;
  private File file;
  private ZipFile zipFile;

  private final HiveQueryService hiveQueryService;
  private final QueryDetailsService queryDetailsService;
  private final DagInfoService dagInfoService;
  private final VertexInfoService vertexInfoService;


  @Inject
  public TestArtifactAggregator() {
    this.hiveQueryService = mock(HiveQueryService.class);
    this.queryDetailsService = mock(QueryDetailsService.class);
    this.dagInfoService = mock(DagInfoService.class);
    this.vertexInfoService = mock(VertexInfoService.class);
  }

  @Before
  public void setup() throws Exception {
    service = Executors.newSingleThreadExecutor();
    conf = new Configuration(new Properties());
    params = new Params();

    file = File.createTempFile("test-tmp", ".zip");
    file.delete();
  }

  @After
  public void teardown() throws Exception {
    service.shutdownNow();
    if (zipFile != null) {
      zipFile.close();
    }
    if (file != null) {
      file.delete();
    }
  }

  private void run(ArtifactSource... sources) throws Exception {
    ArrayList<ArtifactSourceCreator> creators = new ArrayList<>(sources.length);
    for (ArtifactSource source : sources) {
      creators.add(new TestSourceCreator<>(source));
    }
    try (ArtifactAggregator aggregator = new ArtifactAggregator(conf, service, params,
        file.getAbsolutePath(), creators, hiveQueryService, queryDetailsService,
        dagInfoService, vertexInfoService)) {
      aggregator.aggregate();
    }
    zipFile = new ZipFile(file);
  }

  @Test(timeout=5000)
  public void testEmptySources() throws Exception {
    run();
    assertNotNull(zipFile);
    assertEquals(0, zipFile.size());
  }

  @Test(timeout=5000)
  public void testSingleSourceSingleArtifactSuccess() throws Exception {
    TestArtifactSource source = new TestArtifactSource()
        .setRequiredParams(true)
        .addArtifact(new TestArtifact("Artifact1", "Content1", false));
    run(source);
    assertEquals(1, zipFile.size());
    assertEquals("Content1", readEntry("Artifact1"));
  }

  @Test(timeout=5000)
  public void testSingleSourceSingleArtifactFailure() throws Exception {
    TestArtifactSource source = new TestArtifactSource()
        .setRequiredParams(true)
        .addArtifact(new TestArtifact("Test1", new ArtifactDownloadException("Err1"), false));
    run(source);
    assertEquals(1, zipFile.size());
    JsonNode node = new ObjectMapper().readTree(readEntry(ArtifactAggregator.ERRORS_ARTIFACT));
    assertError(node, "Test1", "Err1");
  }

  @Test(timeout=5000)
  public void testSingleSourceMulipleArtifactsSuccess() throws Exception {
    TestArtifactSource source = new TestArtifactSource()
        .setRequiredParams(true)
        .addArtifact(new TestArtifact("Test1", "Content1", false))
        .addArtifact(new TestArtifact("Test2", "Content2", false));
    run(source);
    assertEquals(2, zipFile.size());
    assertEquals("Content1", readEntry("Test1"));
    assertEquals("Content2", readEntry("Test2"));
  }

  @Test(timeout=5000)
  public void testMultipleSourceChainingSuccess() throws Exception {
    TestArtifactSource source2 = new TestArtifactSource()
        .addArtifact(new TestArtifact("Test2", "Content2", false));
    TestArtifactSource source1 = new TestArtifactSource()
        .setRequiredParams(true)
        .addArtifact(new TestArtifact("Test1", "Content1", false))
        .addSource(source2);
    run(source1, source2);
    assertEquals(2, zipFile.size());
    assertEquals("Content1", readEntry("Test1"));
    assertEquals("Content2", readEntry("Test2"));
  }

  @Test(timeout=5000)
  public void testAll() throws Exception {
    TestArtifactSource source4 = new TestArtifactSource()
        .addArtifact(new TestArtifact("Test7", "Content7", false));
    TestArtifactSource source3 = new TestArtifactSource()
        .addArtifact(new TestArtifact("Test6", new ArtifactDownloadException("Err6"), false))
        .addSource(source4);
    TestArtifactSource source2 = new TestArtifactSource()
        .addArtifact(new TestArtifact("Test5", "Content5", false));
    TestArtifactSource source1 = new TestArtifactSource()
        .setRequiredParams(true)
        .addArtifact(new TestArtifact("Test1", "Content1", false))
        .addArtifact(new TestArtifact("Test2", "Content2", true))
        .addArtifact(new TestArtifact("Test3", new ArtifactDownloadException("Err3"), false))
        .addArtifact(new TestArtifact("Test4", new ArtifactDownloadException("Err4"), true))
        .addSource(source2)
        .addSource(source3);
    run(source4, source1, source2, source3);

    assertEquals(3, zipFile.size());
    JsonNode node = new ObjectMapper().readTree(readEntry(ArtifactAggregator.ERRORS_ARTIFACT));

    assertEquals("Content1", readEntry("Test1"));
    assertNull(zipFile.getEntry("Test2"));
    assertError(node, "Test3", "Err3");
    assertError(node, "Test4", "Err4");
    assertEquals("Content5", readEntry("Test5"));
    assertError(node, "Test6", "Err6");
    assertNull(zipFile.getEntry("Test7"));
  }

  private void assertError(JsonNode node, String name, String err) {
    assertTrue(node.path(name).asText().contains(err));
  }

  private String readEntry(String entryName) throws IOException {
    ZipEntry entry = zipFile.getEntry(entryName);
    assertNotNull(entry);
    byte[] buffer = new byte[(int) entry.getSize()];
    IOUtils.readFully(zipFile.getInputStream(entry), buffer);
    return new String(buffer);
  }

  private static class TestSourceCreator<T extends ArtifactSource>
      implements ArtifactSourceCreator {
    final ArtifactSource source;

    TestSourceCreator(ArtifactSource source) {
      this.source = source;
    }

    @Override
    public ArtifactSource getSource(Injector injector) {
      assertNotNull(injector);
      return source;
    }
  }

  private static class TestArtifact implements Artifact {
    private boolean isTemp;
    private Object content;
    private String name;

    TestArtifact(String name, Object content, boolean isTemp) {
      this.name = name;
      this.content = content;
      this.isTemp = isTemp;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void downloadInto(Path path) throws ArtifactDownloadException {
      if (content instanceof ArtifactDownloadException) {
        throw (ArtifactDownloadException)content;
      }
      try {
        Files.write(path, content.toString().getBytes(), StandardOpenOption.CREATE_NEW,
            StandardOpenOption.WRITE);
      } catch (IOException e) {
        throw new ArtifactDownloadException(e);
      }
    }

    @Override
    public boolean isTemp() {
      return isTemp;
    }
  }

  private static class TestArtifactSource implements ArtifactSource {
    private boolean hasRequiredParams = false;
    private List<Artifact> artifacts = new ArrayList<>();
    private List<TestArtifactSource> sources = new ArrayList<>();

    public TestArtifactSource setRequiredParams(boolean val) {
      hasRequiredParams = val;
      return this;
    }

    public TestArtifactSource addArtifact(Artifact artifact) {
      artifacts.add(artifact);
      return this;
    }

    public TestArtifactSource addSource(TestArtifactSource source) {
      sources.add(source);
      return this;
    }

    @Override
    public boolean hasRequiredParams(Params params) {
      return hasRequiredParams;
    }

    @Override
    public List<Artifact> getArtifacts(Params params) {
      return artifacts;
    }

    @Override
    public void updateParams(Params params, Artifact artifact, Path path)
        throws ArtifactDownloadException {
      for (TestArtifactSource source : sources) {
        source.setRequiredParams(true);
      }
    }
  }
}
