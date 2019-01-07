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

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.query.services.DagInfoService;
import com.hortonworks.hivestudio.query.services.HiveQueryService;
import com.hortonworks.hivestudio.query.services.QueryDetailsService;
import com.hortonworks.hivestudio.query.services.VertexInfoService;

/**
 * Class to download and aggregate all the logs into a zip file.
 */
public class ArtifactAggregator implements AutoCloseable {
  public static final String ERRORS_ARTIFACT = "error-reports.json";
  private final ExecutorService service;
  private final Params params;
  private final CloseableHttpClient httpClient;
  private final FileSystem zipfs;
  private final List<ArtifactSource> pendingSources;
  private final Map<Artifact, ArtifactSource> artifactSource;

  public ArtifactAggregator(final Configuration conf, ExecutorService service, Params params,
      String zipFilePath, List<ArtifactSourceCreator> sourceTypes,
      HiveQueryService hiveQueryService, QueryDetailsService queryDetailsService,
      DagInfoService dagInfoService, VertexInfoService vertexInfoService)
          throws IOException {
    this.service = service;
    this.params = params;
    this.httpClient = HttpClients.createDefault();
    this.artifactSource = new HashMap<>();
    this.zipfs = FileSystems.newFileSystem(URI.create("jar:file:" + zipFilePath),
        ImmutableMap.of("create", "true", "encoding", "UTF-8"));

    Injector injector = Guice.createInjector(new Module() {
      @Override
      public void configure(Binder binder) {
        // TODO: Use the same HTTPClient used by the other part of the codebase
        binder.bind(HttpClient.class).toInstance(httpClient);
        binder.bind(Configuration.class).toInstance(conf);

        binder.bind(HiveQueryService.class).toInstance(hiveQueryService);
        binder.bind(QueryDetailsService.class).toInstance(queryDetailsService);
        binder.bind(DagInfoService.class).toInstance(dagInfoService);
        binder.bind(VertexInfoService.class).toInstance(vertexInfoService);
      }
    });
    this.pendingSources = new ArrayList<>(sourceTypes.size());
    for (ArtifactSourceCreator sourceType : sourceTypes) {
      pendingSources.add(sourceType.getSource(injector));
    }
  }

  public void aggregate() throws IOException {
    final Map<String, Throwable> errors = new HashMap<>();
    while (!pendingSources.isEmpty()) {
      List<Artifact> artifacts = collectDownloadableArtifacts(errors);
      if (artifacts.isEmpty() && !pendingSources.isEmpty()) {
        // Artifacts is empty, but some sources are pending.
        // Can be because dagId was given, queryId could not be found, etc ...
        break;
      }
      List<Future<?>> futures = new ArrayList<>(artifacts.size());
      for (final Artifact artifact : artifacts) {
        final Path path = getArtifactPath(artifact.getName());
        futures.add(service.submit(new Runnable() {
          public void run() {
            try {
              artifact.downloadInto(path);
              artifactSource.get(artifact).updateParams(params, artifact, path);
            } catch (Throwable t) {
              errors.put(artifact.getName(), t);
            } finally {
              if (artifact.isTemp()) {
                try {
                  Files.delete(path);
                } catch (IOException ignore) {
                }
              }
            }
          }
        }));
      }
      // Its important that we wait for all futures in this stage, there are some cases where all
      // downloads/updates of one stage should finish before we start the next stage.
      for (Future<?> future : futures) {
        try {
          future.get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (ExecutionException e) {
          // ignore, this should not happen, we catch all throwable and serialize into error
        }
      }
    }
    writeErrors(errors);
  }

  private Path getArtifactPath(String artifactName) throws IOException {
    final Path path = zipfs.getPath("/", artifactName.split("/"));
    Path parent = path.getParent();
    if (parent != null) {
      Files.createDirectories(parent);
    }
    return path;
  }

  @Override
  public void close() throws IOException {
    try {
      if (zipfs != null) {
        zipfs.close();
      }
    } finally {
      if (httpClient != null) {
        httpClient.close();
      }
    }
  }

  private List<Artifact> collectDownloadableArtifacts(Map<String, Throwable> errors) {
    List<Artifact> artifacts = new ArrayList<>();
    Iterator<ArtifactSource> iter = pendingSources.iterator();
    while (iter.hasNext()) {
      ArtifactSource source = iter.next();
      if (source.hasRequiredParams(params)) {
        try {
          for (Artifact artifact : source.getArtifacts(params)) {
            artifacts.add(artifact);
            artifactSource.put(artifact, source);
          }
        } catch (ArtifactDownloadException e) {
          errors.put(source.getClass().getSimpleName(), e);
        }
        iter.remove();
      }
    }
    return artifacts;
  }

  private void writeErrors(Map<String, Throwable> errors) throws IOException {
    if (errors.isEmpty()) {
      return;
    }
    // TODO: Add extra informations - hivetools version, user and other meta data
    // TODO: Add ACL exception in the report.json
    Path path = zipfs.getPath(ERRORS_ARTIFACT);
    OutputStream stream = null;
    try {
      stream = Files.newOutputStream(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

      JsonGenerator generator = objectMapper.getFactory().createGenerator(stream);
      generator.writeStartObject();
      for (Entry<String, Throwable> entry : errors.entrySet()) {
        StringWriter writer = new StringWriter();
        entry.getValue().printStackTrace(new PrintWriter(writer));
        generator.writeStringField(entry.getKey(), writer.toString());
      }
      generator.writeEndObject();
      generator.close();
    } finally {
      // We should not close a stream from ZipFileSystem, I have no clue why.
      // IOUtils.closeQuietly(stream);
    }
  }
}
