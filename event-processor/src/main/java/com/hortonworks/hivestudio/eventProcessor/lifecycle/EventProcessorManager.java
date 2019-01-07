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
package com.hortonworks.hivestudio.eventProcessor.lifecycle;

import java.io.IOException;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.hooks.proto.HiveHookEvents.HiveHookEventProto;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.history.logging.proto.DatePartitionedLogger;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.TezProtoLoggers;

import com.hortonworks.hivestudio.common.actor.GuiceAkkaExtension;
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager;
import com.hortonworks.hivestudio.eventProcessor.actors.Monitor;
import com.hortonworks.hivestudio.eventProcessor.actors.scheduler.MetaInfoRefresher;
import com.hortonworks.hivestudio.eventProcessor.configuration.Constants;
import com.hortonworks.hivestudio.eventProcessor.configuration.EventProcessingConfig;
import com.hortonworks.hivestudio.eventProcessor.entities.FileStatusEntity.FileStatusType;
import com.hortonworks.hivestudio.eventProcessor.entities.repository.FileStatusPersistenceManager;
import com.hortonworks.hivestudio.eventProcessor.pipeline.EventProcessorPipeline;
import com.hortonworks.hivestudio.eventProcessor.processors.HiveHSEventProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.TezHSEventProcessor;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import io.dropwizard.lifecycle.Managed;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class EventProcessorManager implements Managed {

  private final Provider<ActorSystem> actorSystemProvider;
  private final Provider<GuiceAkkaExtension.AkkaExtensionProvider> guiceExtProvider;
  private final Configuration hadoopConfiguration;
  private final EventProcessingConfig eventProcessingConfig;
  private final TezHSEventProcessor tezEventProcessor;
  private final HiveHSEventProcessor hiveEventProcessor;
  private final Provider<FileStatusPersistenceManager> fsPersistenceManager;
  private final TransactionManager txnManager;

  private ActorRef metaInfoRefresher;
  private ActorRef monitor;
  private EventProcessorPipeline<HistoryEventProto> tezEventsPipeline;
  private EventProcessorPipeline<HiveHookEventProto> hiveEventsPipeline;

  @Inject
  public EventProcessorManager(Provider<ActorSystem> actorSystemProvider,
                               Provider<GuiceAkkaExtension.AkkaExtensionProvider> extensionProvider,
                               EventProcessingConfig eventProcessingConfig,
                               Configuration hadoopConfiguration,
                               TezHSEventProcessor tezEventProcessor,
                               HiveHSEventProcessor hiveEventProcessor,
                               Provider<FileStatusPersistenceManager> fsPersistenceManager,
                               TransactionManager txnManager) {
    this.actorSystemProvider = actorSystemProvider;
    this.guiceExtProvider = extensionProvider;
    this.eventProcessingConfig = eventProcessingConfig;
    this.hadoopConfiguration = hadoopConfiguration;
    this.tezEventProcessor = tezEventProcessor;
    this.hiveEventProcessor = hiveEventProcessor;
    this.fsPersistenceManager = fsPersistenceManager;
    this.txnManager = txnManager;
  }

  @Override
  public void start() {
    log.info("Starting");
    startTezPipeline();
    startHivePipeline();
    startMetaInfoRefresher();
    log.info("Started");
  }

  @Override
  public void stop() {
    log.info("EventProcessorManager: stopping");
    tezEventsPipeline.shutdown();
    hiveEventsPipeline.shutdown();
    tezEventsPipeline.awaitTermination();
    hiveEventsPipeline.awaitTermination();
    log.info("EventProcessorManager: stopped");
  }

  private void startTezPipeline() {
    log.info("Starting tez events pipeline");
    try {
      // TODO: This should be read from tez-site.xml
      String tezBaseDir = eventProcessingConfig.getAsString(
          TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_BASE_DIR, null);
      if (tezBaseDir == null) {
        throw new RuntimeException("Please configure: " +
            TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_BASE_DIR);
      }
      hadoopConfiguration.set(TezConfiguration.TEZ_HISTORY_LOGGING_PROTO_BASE_DIR, tezBaseDir);

      TezProtoLoggers loggers = new TezProtoLoggers();
      SystemClock clock = SystemClock.getInstance();
      if (!loggers.setup(hadoopConfiguration, clock)) {
        throw new RuntimeException("Failed to create tez events pipeline, loggers setup failed");
      }
      tezEventsPipeline = new EventProcessorPipeline<>(clock, loggers.getDagEventsLogger(),
          tezEventProcessor, txnManager, fsPersistenceManager, FileStatusType.TEZ,
          eventProcessingConfig);
      tezEventsPipeline.start();

      log.info("Started tez events pipeline");
    } catch (IOException e) {
      log.error("Failed to start tez events pipeline");
      throw new RuntimeException("Failed to create tez events pipeline, got exception:", e);
    }
  }

  private void startHivePipeline() {
    log.info("Starting hive events pipeline");
    try {
      log.info("Creating hive events pipeline");
      // TODO: This should read from hive-site.xml, and logger should be created in hive codebase.
      String hiveBaseDir = eventProcessingConfig.getAsString(
          ConfVars.HIVE_PROTO_EVENTS_BASE_PATH.varname, null);
      if (hiveBaseDir == null) {
        throw new RuntimeException("Failed to create hive events pipeline, invalid hive config. " +
            "Please set: " + ConfVars.HIVE_PROTO_EVENTS_BASE_PATH.varname);
      }
      log.info("Hive base dir: " + hiveBaseDir);

      SystemClock clock = SystemClock.getInstance();
      DatePartitionedLogger<HiveHookEventProto> logger = new DatePartitionedLogger<>(
          HiveHookEventProto.PARSER, new Path(hiveBaseDir), hadoopConfiguration, clock);
      hiveEventsPipeline = new EventProcessorPipeline<>(clock, logger, hiveEventProcessor,
          txnManager, fsPersistenceManager, FileStatusType.HIVE, eventProcessingConfig);
      hiveEventsPipeline.start();

      log.info("Started hive events pipeline");
    } catch (IOException e) {
      log.error("Failed to start hive events pipeline");
      throw new RuntimeException("Failed to create hive events pipeline, got exception: ", e);
    }
  }

  private void startMetaInfoRefresher() {
    boolean refreshServiceEnabled = eventProcessingConfig.getAsBoolean(
        Constants.ENABLE_REFRESH_META_INFO_SERVICE,
        Constants.ENABLE_REFRESH_META_INFO_SERVICE_DEFAULT);

    if (refreshServiceEnabled) {
      ActorSystem actorSystem = actorSystemProvider.get();
      GuiceAkkaExtension.AkkaExtensionProvider guiceExtension = guiceExtProvider.get();

      monitor = actorSystem.actorOf(guiceExtension.props(Monitor.class), "Monitor");
      // start the actor
      log.info("Starting the MetaInfoRefresher ");
      GuiceAkkaExtension.AkkaExtensionProvider akkaExtensionProvider = guiceExtProvider.get();
      metaInfoRefresher = actorSystem.actorOf(
          akkaExtensionProvider.props(MetaInfoRefresher.class), "MetaInfoRefresher");

      // start the actor
      Long delay = eventProcessingConfig.getAsLong(Constants.META_INFO_SYNC_SERVICE_DELAY_MILLIS,
          Constants.DEFAULT_META_INFO_SYNC_SERVICE_DELAY_MILLIS);
      MetaInfoRefresher.Init init = new MetaInfoRefresher.Init(delay);
      log.info("Sending Init to MetaInfoRefresher : {}", init);
      metaInfoRefresher.tell(init, ActorRef.noSender());

      // register with DeathWatcher
      log.info("registering metaInfoRefresher : {} with death watcher.", metaInfoRefresher);
      this.monitor.tell( new Monitor.RegisterActor(metaInfoRefresher), ActorRef.noSender());
    }
  }
}
