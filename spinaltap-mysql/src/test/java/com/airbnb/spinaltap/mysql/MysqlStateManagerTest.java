/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See License in the project root for license
 * information.
 */
package com.airbnb.spinaltap.mysql;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import com.airbnb.spinaltap.common.source.SourceState;
import com.airbnb.spinaltap.common.util.Repository;
import com.airbnb.spinaltap.mysql.mutation.MysqlInsertMutation;
import com.airbnb.spinaltap.mysql.mutation.MysqlMutation;
import com.airbnb.spinaltap.mysql.mutation.MysqlMutationMetadata;
import com.airbnb.spinaltap.mysql.mutation.schema.Row;
import com.google.common.collect.ImmutableMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

public class MysqlStateManagerTest {
  private static final String SOURCE_NAME = "test";
  private static final long SAVED_OFFSET = 12L;
  private static final long SAVED_TIMESTAMP = 12L;
  private static final BinlogFilePos BINLOG_FILE_POS = new BinlogFilePos("test.txt", 14, 100);
  private final StateRepository stateRepository = mock(StateRepository.class);
  private final MysqlSourceMetrics mysqlMetrics = mock(MysqlSourceMetrics.class);

  @Test
  public void testSaveState() throws Exception {

    MysqlStateManager stateManager =
        new MysqlStateManager(
            SOURCE_NAME,
            stateRepository,
            createTestStateHistory(),
            MysqlStateManager.LATEST_BINLOG_POS,
            new AtomicLong(0L),
            mysqlMetrics);

    SourceState savedState = mock(SourceState.class);
    SourceState newState = mock(SourceState.class);

    when(stateRepository.read()).thenReturn(savedState);

    stateManager.saveState(newState);

    verify(stateRepository, times(1)).save(newState);
    assertEquals(newState, stateManager.getLastSavedState().get());
  }

  @Test
  public void testGetState() throws Exception {
    MysqlStateManager stateManager =
        new MysqlStateManager(
            SOURCE_NAME,
            stateRepository,
            createTestStateHistory(),
            MysqlStateManager.LATEST_BINLOG_POS,
            new AtomicLong(0L),
            mysqlMetrics);
    SourceState savedState = mock(SourceState.class);

    when(stateRepository.read()).thenReturn(savedState);

    stateManager.initialize();

    SourceState state = stateManager.getSavedState();

    assertEquals(savedState, state);

    when(stateRepository.read()).thenReturn(null);

    state = stateManager.getSavedState();

    assertEquals(0L, state.getLastOffset());
    assertEquals(0L, state.getLastTimestamp());
    assertEquals(MysqlStateManager.LATEST_BINLOG_POS, state.getLastPosition());
  }

  @Test
  public void testResetToLastValidState() throws Exception {
    StateHistory stateHistory = createTestStateHistory();
    MysqlStateManager stateManager =
        new MysqlStateManager(
            SOURCE_NAME,
            stateRepository,
            stateHistory,
            MysqlStateManager.LATEST_BINLOG_POS,
            new AtomicLong(0L),
            mysqlMetrics);

    SourceState savedState = mock(SourceState.class);
    SourceState earliestState = new SourceState(0L, 0L, 0L, MysqlStateManager.EARLIEST_BINLOG_POS);

    when(stateRepository.read()).thenReturn(savedState);

    SourceState firstState = mock(SourceState.class);
    SourceState secondState = mock(SourceState.class);
    SourceState thirdState = mock(SourceState.class);
    SourceState fourthState = mock(SourceState.class);

    stateHistory.add(firstState);
    stateHistory.add(secondState);
    stateHistory.add(thirdState);

    stateManager.initialize();

    stateManager.resetToLastValidState();
    assertEquals(thirdState, stateManager.getLastSavedState().get());

    stateManager.resetToLastValidState();
    assertEquals(firstState, stateManager.getLastSavedState().get());
    assertTrue(stateHistory.isEmpty());

    stateManager.resetToLastValidState();
    assertEquals(earliestState, stateManager.getLastSavedState().get());

    stateHistory.add(firstState);
    stateHistory.add(secondState);
    stateHistory.add(thirdState);
    stateHistory.add(fourthState);

    stateManager.resetToLastValidState();
    assertEquals(firstState, stateManager.getLastSavedState().get());

    stateHistory.add(firstState);
    stateHistory.add(secondState);

    stateManager.resetToLastValidState();
    assertEquals(earliestState, stateManager.getLastSavedState().get());
    assertTrue(stateHistory.isEmpty());

    BinlogFilePos filePos = new BinlogFilePos("test.txt", 18, 156);
    Transaction lastTransaction = new Transaction(0L, 0L, filePos);
    MysqlMutationMetadata metadata =
        new MysqlMutationMetadata(null, null, null, 0L, 1L, 23L, null, lastTransaction, 0L, 0);

    stateManager.commitCheckpoint(new MysqlInsertMutation(metadata, null));

    assertFalse(stateHistory.isEmpty());

    stateManager.resetToLastValidState();
    assertEquals(new SourceState(23L, 1L, 0L, filePos), stateManager.getLastSavedState().get());
  }

  @Test
  public void testCommitCheckpoint() throws Exception {
    StateHistory stateHistory = createTestStateHistory();
    MysqlStateManager stateManager =
        new MysqlStateManager(
            SOURCE_NAME,
            stateRepository,
            stateHistory,
            MysqlStateManager.LATEST_BINLOG_POS,
            new AtomicLong(0L),
            mysqlMetrics);

    Row row = new Row(null, ImmutableMap.of());
    BinlogFilePos filePos = new BinlogFilePos("test.txt", 18, 156);
    Transaction lastTransaction = new Transaction(0L, 0L, filePos);
    MysqlMutationMetadata metadata =
        new MysqlMutationMetadata(null, filePos, null, 0L, 0L, 0L, null, lastTransaction, 0, 0);
    MysqlMutation mutation = new MysqlInsertMutation(metadata, row);
    SourceState savedState = new SourceState(SAVED_TIMESTAMP, SAVED_OFFSET, 0L, BINLOG_FILE_POS);

    when(stateRepository.read()).thenReturn(savedState);

    stateManager.initialize();

    stateManager.commitCheckpoint(mutation);
    assertEquals(savedState, stateManager.getLastSavedState().get());

    stateManager.commitCheckpoint(null);
    assertEquals(savedState, stateManager.getLastSavedState().get());

    long newOffset = SAVED_OFFSET + 1;
    metadata =
        new MysqlMutationMetadata(
            null, filePos, null, 0L, newOffset, 23L, null, lastTransaction, 0, 0);
    mutation = new MysqlInsertMutation(metadata, row);

    stateManager.commitCheckpoint(mutation);
    assertEquals(
        new SourceState(23L, newOffset, 0L, filePos), stateManager.getLastSavedState().get());
    assertEquals(stateHistory.removeLast(), stateManager.getLastSavedState().get());
  }

  private StateHistory createTestStateHistory() {
    return new StateHistory("", 10, mock(Repository.class), mysqlMetrics);
  }
}
