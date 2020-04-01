/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See License in the project root for license
 * information.
 */
package com.airbnb.spinaltap.mysql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.airbnb.spinaltap.common.source.SourceState;
import com.airbnb.spinaltap.common.util.Repository;
import com.airbnb.spinaltap.mysql.exception.InvalidBinlogPositionException;
import com.airbnb.spinaltap.mysql.mutation.schema.Table;
import com.airbnb.spinaltap.mysql.schema.MysqlSchemaTracker;
import com.airbnb.spinaltap.mysql.schema.SchemaTracker;
import com.google.common.collect.Sets;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;
import org.junit.Test;

public class MysqlSourceTest {
  private static final String SOURCE_NAME = "test";
  private static final DataSource DATA_SOURCE = new DataSource("test_host", 1, "test");
  private static final Set<String> TABLE_NAMES =
      Sets.newHashSet(Table.canonicalNameOf("db", "users"));

  private static final long SAVED_OFFSET = 12L;
  private static final long SAVED_TIMESTAMP = 12L;
  private static final BinlogFilePos BINLOG_FILE_POS = new BinlogFilePos("test.txt", 14, 100);

  private final TableCache tableCache = mock(TableCache.class);
  private final MysqlSourceMetrics mysqlMetrics = mock(MysqlSourceMetrics.class);
  private final StateRepository stateRepository = mock(StateRepository.class);
  private final MysqlSource.Listener listener = mock(MysqlSource.Listener.class);
  private final SchemaTracker schemaTracker = mock(MysqlSchemaTracker.class);
  private final AtomicLong currentLeaderEpoch = new AtomicLong(0L);
  private final MysqlStateManager stateManager =
      new MysqlStateManager(
          SOURCE_NAME,
          stateRepository,
          createTestStateHistory(),
          MysqlStateManager.LATEST_BINLOG_POS,
          currentLeaderEpoch,
          mysqlMetrics);

  @Test
  public void testOpenClose() throws Exception {
    TestSource source = new TestSource();
    SourceState savedState = new SourceState(SAVED_TIMESTAMP, SAVED_OFFSET, 0L, BINLOG_FILE_POS);

    when(stateRepository.read()).thenReturn(savedState);

    source.open();

    Transaction lastTransaction =
        new Transaction(
            savedState.getLastTimestamp(),
            savedState.getLastOffset(),
            savedState.getLastPosition());

    assertEquals(savedState, stateManager.getLastSavedState().get());
    assertEquals(lastTransaction, stateManager.getLastTransaction().get());
    assertEquals(BINLOG_FILE_POS, source.getPosition());
    verify(tableCache, times(1)).clear();
  }

  @Test
  public void testOnCommunicationError() throws Exception {
    TestSource source = new TestSource();

    source.addListener(listener);
    source.setPosition(null);
    try {
      source.onCommunicationError(new RuntimeException());
      fail("Should not reach here");
    } catch (Exception ex) {

    }
    assertNull(stateManager.getLastSavedState().get());

    try {
      source.onCommunicationError(new InvalidBinlogPositionException(""));
      fail("Should not reach here");
    } catch (Exception ex) {

    }
    assertEquals(
        MysqlStateManager.EARLIEST_BINLOG_POS,
        stateManager.getLastSavedState().get().getLastPosition());
  }

  private StateHistory createTestStateHistory() {
    return new StateHistory("", 10, mock(Repository.class), mysqlMetrics);
  }

  @Getter
  class TestSource extends MysqlSource {
    private boolean isConnected;
    private BinlogFilePos position;

    TestSource() {
      this(createTestStateHistory());
    }

    TestSource(StateHistory stateHistory) {
      super(
          SOURCE_NAME,
          DATA_SOURCE,
          TABLE_NAMES,
          tableCache,
          stateManager,
          schemaTracker,
          mysqlMetrics,
          currentLeaderEpoch);
    }

    public void setPosition(BinlogFilePos pos) {
      position = pos;
    }

    public void connect() {
      isConnected = true;
    }

    public void disconnect() {
      isConnected = false;
    }

    public boolean isConnected() {
      return isConnected;
    }
  }
}
