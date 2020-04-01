/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See License in the project root for license
 * information.
 */
package com.airbnb.spinaltap.mysql;

import com.airbnb.spinaltap.Mutation;
import com.airbnb.spinaltap.common.source.SourceState;
import com.airbnb.spinaltap.mysql.mutation.MysqlMutation;
import com.airbnb.spinaltap.mysql.mutation.MysqlMutationMetadata;
import com.google.common.base.Preconditions;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Responsible for tracking processed MySQL binlog offset and committing checkpoints */
@Slf4j
@RequiredArgsConstructor
public class MysqlStateManager {
  /** Represents the latest binlog position in the mysql-binlog-connector client. */
  public static final BinlogFilePos LATEST_BINLOG_POS = new BinlogFilePos(null, 0, 0);

  /** Represents the earliest binlog position in the mysql-binlog-connector client. */
  public static final BinlogFilePos EARLIEST_BINLOG_POS = new BinlogFilePos("", 4, 4);

  /** The backoff rate when conducting rollback in the {@link StateHistory}. */
  private static final int STATE_ROLLBACK_BACKOFF_RATE = 2;

  private final String sourceName;
  private final StateRepository stateRepository;
  private final StateHistory stateHistory;
  private final BinlogFilePos initialBinlogFilePosition;
  private final AtomicLong currentLeaderEpoch;
  private final MysqlSourceMetrics metrics;
  @Getter private final AtomicReference<SourceState> lastSavedState = new AtomicReference<>();
  @Getter private final AtomicReference<Transaction> lastTransaction = new AtomicReference<>();

  /** The number of {@link SourceState} entries to remove from {@link StateHistory} on rollback. */
  private final AtomicInteger stateRollbackCount = new AtomicInteger(1);

  public void initialize() {
    SourceState state = getSavedState();
    lastSavedState.set(state);
    lastTransaction.set(
        new Transaction(state.getLastTimestamp(), state.getLastOffset(), state.getLastPosition()));
  }

  /** Resets to the last valid {@link SourceState} recorded in the {@link StateHistory}. */
  public void resetToLastValidState() {
    if (stateHistory.size() >= stateRollbackCount.get()) {
      final SourceState newState = stateHistory.removeLast(stateRollbackCount.get());
      saveState(newState);

      metrics.resetSourcePosition();
      log.info("Reset source {} position to {}.", sourceName, newState.getLastPosition());

      stateRollbackCount.accumulateAndGet(
          STATE_ROLLBACK_BACKOFF_RATE, (value, rate) -> value * rate);

    } else {
      stateHistory.clear();
      saveState(getEarliestState());

      metrics.resetEarliestPosition();
      log.info("Reset source {} position to earliest.", sourceName);
    }
  }

  /** Checkpoints the {@link SourceState} for the source at the given {@link Mutation} position. */
  public void commitCheckpoint(final Mutation<?> mutation) {
    final SourceState savedState = lastSavedState.get();
    if (mutation == null || savedState == null) {
      return;
    }

    Preconditions.checkState(mutation instanceof MysqlMutation);
    final MysqlMutationMetadata metadata = ((MysqlMutation) mutation).getMetadata();

    // Make sure we are saving at a higher watermark
    if (savedState.getLastOffset() >= metadata.getId()) {
      return;
    }

    final SourceState newState =
        new SourceState(
            metadata.getTimestamp(),
            metadata.getId(),
            currentLeaderEpoch.get(),
            metadata.getLastTransaction().getPosition());

    saveState(newState);

    stateHistory.add(newState);
    stateRollbackCount.set(1);
  }

  void saveState(@NonNull final SourceState state) {
    stateRepository.save(state);
    lastSavedState.set(state);
  }

  public BinlogFilePos getLastPosition() {
    return lastSavedState.get().getLastPosition();
  }

  private SourceState getEarliestState() {
    return new SourceState(0L, 0L, currentLeaderEpoch.get(), EARLIEST_BINLOG_POS);
  }

  SourceState getSavedState() {
    return Optional.ofNullable(stateRepository.read())
        .orElse(new SourceState(0L, 0L, currentLeaderEpoch.get(), initialBinlogFilePosition));
  }
}
