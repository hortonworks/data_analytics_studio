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

package com.hortonworks.hivestudio.debugBundler;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Pattern;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.collect.ImmutableMap;
import com.hortonworks.hivestudio.common.Constants;
import com.hortonworks.hivestudio.common.config.Configuration;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactAggregator;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactSourceCreator;
import com.hortonworks.hivestudio.debugBundler.framework.ArtifactSourceType;
import com.hortonworks.hivestudio.debugBundler.framework.Params;
import com.hortonworks.hivestudio.query.services.DagInfoService;
import com.hortonworks.hivestudio.query.services.HiveQueryService;
import com.hortonworks.hivestudio.query.services.QueryDetailsService;
import com.hortonworks.hivestudio.query.services.VertexInfoService;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class BundlerService {
  private static final String CONF_NUM_EXECUTORS_KEY = "hivestudio.artifact-hivetools.num-executors";
  private static final String CONF_TMP_DIR_KEY = "hivestudio.artifact-hivetools.tmp-dir";

  private static final String CONF_NUM_EXECUTORS_DEFAULT = "10";

  private final ExecutorService executor;
  private final Configuration configuration;

  private final HiveQueryService hiveQueryService;
  private final QueryDetailsService queryDetailsService;
  private final DagInfoService dagInfoService;
  private final VertexInfoService vertexInfoService;

  private String activeDirPath, doneDirPath;

  @Inject
  public BundlerService(Configuration configuration, HiveQueryService hiveQueryService,
      QueryDetailsService queryDetailsService, DagInfoService dagInfoService,
      VertexInfoService vertexInfoService) {
    this.configuration = configuration;

    this.hiveQueryService = hiveQueryService;
    this.queryDetailsService = queryDetailsService;
    this.dagInfoService = dagInfoService;
    this.vertexInfoService = vertexInfoService;

    // TODO: Change to the web-server's temp dir
    String tmpDir = configuration.get(CONF_TMP_DIR_KEY, System.getProperty("java.io.tmpdir"));
    initDirectories(tmpDir);

    String numExecutors = configuration.get(CONF_NUM_EXECUTORS_KEY, CONF_NUM_EXECUTORS_DEFAULT);
    executor = Executors.newFixedThreadPool(Integer.parseInt(numExecutors));
    ((ThreadPoolExecutor)executor).setRejectedExecutionHandler(
      new ThreadPoolExecutor.CallerRunsPolicy());
  }

  private void initDirectories(String tmpDir) {
    activeDirPath = Paths.get(tmpDir, "active").toString();
    doneDirPath = Paths.get(tmpDir, "done").toString();

    // Create the directories of not available
    new File(activeDirPath).mkdirs();
    new File(doneDirPath).mkdirs();
  }

  // Only allow alpha-numerics, '-' and '_' in the fileName.
  private static final Pattern validFileName = Pattern.compile("^[0-9a-zA-Z\\-_]+$");
  public String constructFileName(String queryID, String suffix) {
    String fileName = queryID;

    if (!validFileName.matcher(fileName).matches()) {
      fileName = "artifact_bundle";
    }

    return String.format("%s-%s.zip", fileName, suffix);
  }

  public HashMap<String, String> getBundleDetails(String queryID, String user) throws IOException {
    String fileName = constructFileName(queryID, user);
    File bundleFile;

    HashMap<String, String> details = new HashMap<>();
    BundleStatus status = BundleStatus.NOT_AVAILABLE;

    bundleFile = new File(doneDirPath, fileName);
    if(bundleFile.exists()) {
      status = BundleStatus.AVAILABLE;
    }
    else {
      bundleFile = new File(activeDirPath, fileName);
      if(bundleFile.exists()) {
        status = BundleStatus.IN_PROGRESS;
      }
    }

    if(bundleFile.exists()) {
      BasicFileAttributes attr = Files.getFileAttributeView(bundleFile.toPath(), BasicFileAttributeView.class).readAttributes();
      details.put("creationTime", Long.toString(attr.creationTime().toMillis()));
    }

    details.put("queryID", queryID);
    details.put("status", status.name());

    return details;
  }

  public boolean createBundle(String queryID, String user) throws IOException {
    String fileName = constructFileName(queryID, user);
    return startBundling(queryID, user, fileName, false) != null;
  }

  public boolean deleteBundle(String queryID, String user) {
    String fileName = constructFileName(queryID, user);
    File bundleFile = new File(doneDirPath, fileName);

    // As of now we are expecting the file to be only in doneDirectory.
    // There is no UI use case for deleting an in-progress bundling.
    return bundleFile.delete();
  }

  public File getBundleFile(String queryID, String user) {
    String fileName = constructFileName(queryID, user);
    return new File(doneDirPath, fileName);
  }

  public File startBundling(String queryID, String user, String fileName, boolean synch) throws IOException {
    File activeFile = new File(activeDirPath, fileName);
    File doneFile = new File(doneDirPath, fileName);

    Params params = new Params();
    params.setHiveQueryId(queryID);

    String extractLogs = configuration.get(Constants.HIVESTUDIO_DEBUG_BUNDLER_EXTRACT_LOGS,
      Constants.DEFAULT_HIVESTUDIO_DEBUG_BUNDLER_EXTRACT_LOGS);
    params.setEnableLogExtraction(!extractLogs.equals("false"));

    // Set user
    params.setRemoteUser(user);

    if(doneFile.exists()) {
      return doneFile;
    }
    else if(activeFile.exists()) {
      return null;
    }

    // To touch the zip file so that getBundleDetails returns IN_PROGRESS instead of NOT_AVAILABLE
    // we use file to manage status.
    FileSystems.newFileSystem(URI.create("jar:file:" + activeFile.getCanonicalPath()),
      ImmutableMap.of("create", "true", "encoding", "UTF-8")).close();

    Future<?> future = executor.submit(() -> {
      List<ArtifactSourceCreator> sourceTypes = Arrays.asList(ArtifactSourceType.values());

      try (ArtifactAggregator aggregator = new ArtifactAggregator(configuration, executor,
          params, activeFile.getCanonicalPath(), sourceTypes, hiveQueryService,
          queryDetailsService, dagInfoService, vertexInfoService)) {
        aggregator.aggregate();
      } catch (Exception e) {
        log.error("Error while bundling!", e);
      } finally {
        if (activeFile.exists()) {
          activeFile.renameTo(doneFile);
          log.info("Bundled file was written into " + doneFile.getPath());
        }
      }
    });

    if(synch) {
      // Waits till the bundling is complete
      try {
        future.get();
      }
      catch(Exception e) {}
    }

    return doneFile;
  }
}
