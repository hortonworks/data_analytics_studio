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

import com.google.common.annotations.VisibleForTesting;
import com.hortonworks.hivestudio.debugBundler.entities.history.HistoryEntity;
import com.hortonworks.hivestudio.debugBundler.entities.history.HistoryEntityType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.history.logging.proto.DatePartitionedLogger;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;

import java.io.EOFException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class HistoryEventsArtifact implements Artifact {
  private static final String FILE_EXT = ".json";

  private final org.apache.hadoop.fs.Path sourceFile;
  private final HistoryEntityType entityType;

  @VisibleForTesting
  DatePartitionedLogger<HistoryLoggerProtos.HistoryEventProto> partitionedLogger;

  public HistoryEventsArtifact(HistoryEntityType entityType, org.apache.hadoop.fs.Path sourceFile) {
    this.entityType = entityType;
    this.sourceFile = sourceFile;

    initLogger();
  }

  private void initLogger() {
    try {
      SystemClock clock = SystemClock.getInstance();

      partitionedLogger = new DatePartitionedLogger<>(
        HistoryLoggerProtos.HistoryEventProto.PARSER, new org.apache.hadoop.fs.Path("/"),
        entityType.getConfiguration(), clock);
      // As we don't do any writes to HDFS it's fine to pass the base path.
    } catch(IOException e) {
      log.error("Failed to start init logger {}:", e);
    }
  }

  private void writeData(Path path, Map<String, Object> jsonObject) throws IOException {
    PrintWriter writer = new PrintWriter(Files.newBufferedWriter(path));
    entityType.getObjectMapper().writeValue(writer, jsonObject);
    writer.flush();
    writer.close();
  }

  @VisibleForTesting
  Collection<HistoryEntity> readAndCompactEntities() {

    HashMap<String, HistoryEntity> entities = new HashMap<>();

    ProtoMessageReader<HistoryLoggerProtos.HistoryEventProto> reader = null;
    try {
      reader = partitionedLogger.getReader(sourceFile);

      while (true) {
        HistoryLoggerProtos.HistoryEventProto event = null;
        try {
          event = reader.readEvent();
        } catch (EOFException e) {
          // Nothing to do, ignore this, event will be null.
        }
        if (event == null) {
          break;
        }

        String eventName = event.getEventType();
        if(entityType.isRelatedEvent(eventName)) {
          String entityId = entityType.getEntityId(event);

          HistoryEntity entity = entities.get(entityId);
          if(entity == null) {
            entity = new HistoryEntity(entityType, entityId);
            entities.put(entityId, entity);
          }

          HashMap<String, String> dataMap = constructDataMap(event.getEventDataList());
          dataMap.put("eventType", eventName);
          entity.addEvent(dataMap);
        }

      }

    } catch (IOException e) {

      log.error("Error trying to create reader {}: ", e);

    } finally {

      if (reader != null) {
        IOUtils.closeQuietly(reader);
      }

    }

    return entities.values();
  }

  private HashMap<String, String> constructDataMap(List<HistoryLoggerProtos.KVPair> dataMap) {
    HashMap<String, String> map = new HashMap<>();

    for (HistoryLoggerProtos.KVPair kvPair : dataMap) {
      map.put(kvPair.getKey(), kvPair.getValue());
    }

    return map;
  }

  @Override
  public boolean isTemp() {
    return false;
  }

  @Override
  public String getName() {
    return entityType.getPath() + FILE_EXT;
  }

  @Override
  public void downloadInto(Path path) throws ArtifactDownloadException {
    String entityTypeName = entityType.getName().toLowerCase();
    try {
      Collection<HistoryEntity> entities = readAndCompactEntities();
      Map<String, Object> jsonObject = new HashMap<>();
      jsonObject.put(entityTypeName, entities);
      writeData(path, jsonObject);
    }
    catch(Exception e) {
      throw new ArtifactDownloadException("Error trying to fetch " + entityTypeName + " details.", e);
    }
  }


}
