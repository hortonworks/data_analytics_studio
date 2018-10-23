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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import javax.inject.Provider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.history.logging.proto.DatePartitionedLogger;
import org.apache.tez.dag.history.logging.proto.HistoryLoggerProtos.HistoryEventProto;
import org.apache.tez.dag.history.logging.proto.ProtoMessageReader;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.internal.verification.VerificationModeFactory;

import com.google.common.collect.Lists;
import com.hortonworks.hivestudio.common.repository.transaction.Callable;
import com.hortonworks.hivestudio.common.repository.transaction.TransactionManager;
import com.hortonworks.hivestudio.eventProcessor.configuration.Constants;
import com.hortonworks.hivestudio.eventProcessor.configuration.EventProcessingConfig;
import com.hortonworks.hivestudio.eventProcessor.entities.FileStatusEntity.FileStatusType;
import com.hortonworks.hivestudio.eventProcessor.entities.repository.FileStatusPersistenceManager;
import com.hortonworks.hivestudio.eventProcessor.processors.EventProcessor;
import com.hortonworks.hivestudio.eventProcessor.processors.ProcessingStatus;
import com.hortonworks.hivestudio.eventProcessor.processors.ProcessingStatus.Status;

import lombok.Data;

public class EventProcessorPipelineTest {

  DatePartitionedLogger<HistoryEventProto> logger;
  @Mock EventProcessor<HistoryEventProto> processor;
  TransactionManager txnManager;
  @Mock FileStatusPersistenceManager fsPersistenceManager;
  Provider<FileStatusPersistenceManager> fsPersistenceProvider;
  TestClock clock;
  EventProcessorPipeline<HistoryEventProto> pipeline;
  @Mock ProtoMessageReader<HistoryEventProto> reader;

  @Before
  public void setup() throws Exception {
    MockitoAnnotations.initMocks(this);

    clock = new TestClock();
    logger = mock(TestLogger.class);
    when(logger.getDateFromDir(anyString())).thenCallRealMethod();
    when(logger.getDirForDate(any())).thenCallRealMethod();
    when(logger.getPathForDate(any(), any())).thenCallRealMethod();
    when(logger.getConfig()).thenReturn(new Configuration());
    when(logger.getNow()).then(
        a -> LocalDateTime.ofEpochSecond(clock.getTime() / 1000, 0, ZoneOffset.UTC));

    txnManager = new TransactionManager(null) {
      @Override
      public <T, X extends Exception> T withTransaction(Callable<T, X> callable) throws X {
        return callable.call();
      };
    };

    fsPersistenceProvider = new TestProvider<>(fsPersistenceManager);

    EventProcessingConfig config = new EventProcessingConfig();
    config.put(Constants.SCAN_FOLDER_DELAY_MILLIS, 100L);
    pipeline = new EventProcessorPipeline<>(clock, logger, processor, txnManager,
        fsPersistenceProvider, FileStatusType.TEZ, true, config);
  }

  @Test
  public void testPipelineDoNothing() throws Exception {
    when(fsPersistenceManager.getFileOfType(FileStatusType.TEZ))
        .thenReturn(Collections.emptyList());
    when(logger.scanForChangedFiles(eq("date=1970-01-01"), any()))
        .thenReturn(Collections.emptyList());

    pipeline.start();
    Thread.sleep(100);
    pipeline.shutdown();

    verify(logger, VerificationModeFactory.atLeastOnce())
        .scanForChangedFiles(eq("date=1970-01-01"), any());
  }

  private List<FileStatus> fromPaths(List<Path> paths) {
    List<FileStatus> status = new ArrayList<>();
    for (Path path : paths) {
      status.add(new FileStatus(1l, false, 3, 1l, 0L, 0L, null, "user1", "group1", path));
    }
    return status;
  }

  @SuppressWarnings("unchecked")
  private void scanAdd(String date, List<Path> paths1, List<Path> paths2) throws IOException {
    when(logger.scanForChangedFiles(eq(date), any())).thenReturn(fromPaths(paths1), fromPaths(paths2));
  }

  @Test
  public void testPipeline() throws Exception {
    clock.setTime(3 * 24 * 60 * 60 * 1000L);
    when(fsPersistenceManager.getFileOfType(FileStatusType.TEZ))
        .thenReturn(Collections.emptyList());
    Path path = new Path("/basedir/date=1970-01-01/id1");
    when(logger.getReader(eq(path))).thenReturn(reader);
    scanAdd("date=1970-01-01", Lists.newArrayList(path), Lists.newArrayList());
    when(logger.getNextDirectory(eq("date=1970-01-01")))
        .thenReturn("date=1970-01-02");
    when(logger.scanForChangedFiles(eq("date=1970-01-02"), any()))
        .thenReturn(Lists.newArrayList());
    when(logger.getNextDirectory(eq("date=1970-01-02")))
        .thenReturn("date=1970-01-03");
    when(logger.scanForChangedFiles(eq("date=1970-01-03"), any()))
        .thenReturn(Lists.newArrayList());
    when(fsPersistenceManager.create(any())).thenAnswer(m-> m.getArgument(0));
    when(processor.process(eq(HistoryEventProto.getDefaultInstance()), any()))
        .thenReturn(new ProcessingStatus(Status.SUCCESS, Optional.empty()));

    when(reader.readEvent()).thenReturn(HistoryEventProto.getDefaultInstance(),
        (HistoryEventProto)null);

    pipeline.start();
    Thread.sleep(1000);

    // Invoke twice, because we found a file.
    verify(logger, times(2)).scanForChangedFiles(eq("date=1970-01-01"), any());
    verify(logger).getNextDirectory(eq("date=1970-01-01"));

    // Read and processed one event.
    verify(logger).getReader(eq(path));
    verify(reader, times(2)).readEvent();
    verify(reader, times(2)).getOffset();
    verify(processor).process(eq(HistoryEventProto.getDefaultInstance()), any());

    // It should have created and updated one entity.
    verify(fsPersistenceManager).create(any());
    verify(fsPersistenceManager).update(any());

    // Invoke once because no new files.
    verify(logger).scanForChangedFiles(eq("date=1970-01-02"), any());
    verify(logger).getNextDirectory(eq("date=1970-01-02"));

    // Keep invoke because current day is same.
    verify(logger, VerificationModeFactory.atLeastOnce())
      .scanForChangedFiles(eq("date=1970-01-03"), any());
    verify(logger, times(0)).getNextDirectory(eq("date=1970-01-03"));

    // No file should be deleted until now.
    verify(fsPersistenceManager, times(0)).delete(any());

    // Move to next and sleep again.
    clock.setTime(4 * 24 * 60 * 60 * 1000L);
    Thread.sleep(1000);

    // Now date has changed it should advance and also delete the old files.
    verify(logger, VerificationModeFactory.atLeastOnce()).getNextDirectory(eq("date=1970-01-03"));
    verify(fsPersistenceManager).delete(any());

    pipeline.shutdown();
  }

  abstract static class TestLogger extends DatePartitionedLogger<HistoryEventProto> {
    public TestLogger() throws IOException {
      // This is never invoked, just for compiler to be happy.
      super(HistoryEventProto.PARSER, null, new Configuration(), new TestClock());
    }

    @Override
    public Path getPathForDate(LocalDate date, String fileName) throws IOException {
      return new Path("/basedir/" + getDirForDate(date) + "/" + fileName);
    }

    @Override
    public LocalDate getDateFromDir(String dirName) {
      if (!dirName.startsWith("date=")) {
        throw new IllegalArgumentException("Invalid directory: "+ dirName);
      }
      return LocalDate.parse(dirName.substring(5), DateTimeFormatter.ISO_LOCAL_DATE);
    }

    @Override
    public String getDirForDate(LocalDate date) {
      return "date=" + DateTimeFormatter.ISO_LOCAL_DATE.format(date);
    }
  }

  @Data
  static class TestClock implements Clock {
    volatile long time = 0;
  }

  static class TestProvider<T> implements Provider<T> {
    private T instance;

    TestProvider(T inst) {
      this.instance = inst;
    }

    @Override
    public T get() {
      return instance;
    }
  }
}
