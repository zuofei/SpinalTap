/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See License in the project root for license
 * information.
 */
package com.airbnb.spinaltap.mysql;

import com.airbnb.spinaltap.Mutation;
import com.airbnb.spinaltap.common.source.AbstractDataStoreSource;
import com.airbnb.spinaltap.common.source.SourceState;
import com.airbnb.spinaltap.mysql.event.BinlogEvent;
import com.airbnb.spinaltap.mysql.event.filter.MysqlEventFilter;
import com.airbnb.spinaltap.mysql.event.mapper.MysqlMutationMapper;
import com.airbnb.spinaltap.mysql.exception.InvalidBinlogPositionException;
import com.airbnb.spinaltap.mysql.schema.SchemaTracker;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Base implement of a MySQL {@link com.airbnb.spinaltap.common.source.Source} that streams events
 * from a given binlog for a specified database host, and transforms them to {@link Mutation}s.
 */
@Slf4j
public abstract class MysqlSource extends AbstractDataStoreSource<BinlogEvent> {

  /** The {@link DataSource} representing the database host the source is streaming events from. */
  @NonNull @Getter private final DataSource dataSource;

  /**
   * The {@link TableCache} tracking {@link com.airbnb.spinaltap.mysql.mutation.schema.Table}
   * metadata for the streamed source events.
   */
  @NonNull private final TableCache tableCache;

  private final MysqlStateManager stateManager;

  @NonNull protected final MysqlSourceMetrics metrics;

  public MysqlSource(
      @NonNull final String name,
      @NonNull final DataSource dataSource,
      @NonNull final Set<String> tableNames,
      @NonNull final TableCache tableCache,
      @NonNull final MysqlStateManager stateManager,
      @NonNull final SchemaTracker schemaTracker,
      @NonNull final MysqlSourceMetrics metrics,
      @NonNull final AtomicLong currentLeaderEpoch) {
    super(
        name,
        metrics,
        MysqlMutationMapper.create(
            dataSource,
            tableCache,
            schemaTracker,
            currentLeaderEpoch,
            new AtomicReference<>(),
            stateManager.getLastTransaction(),
            metrics),
        MysqlEventFilter.create(tableCache, tableNames, stateManager.getLastSavedState()));
    this.dataSource = dataSource;
    this.tableCache = tableCache;
    this.stateManager = stateManager;
    this.metrics = metrics;
  }

  public abstract void setPosition(BinlogFilePos pos);

  /** Initializes the source and prepares to start streaming. */
  protected void initialize() {
    tableCache.clear();
    stateManager.initialize();
    setPosition(stateManager.getLastPosition());
  }

  protected void onDeserializationError(final Exception ex) {
    metrics.deserializationFailure(ex);

    // Fail on deserialization errors and restart source from last checkpoint
    throw new RuntimeException(ex);
  }

  protected void onCommunicationError(final Exception ex) {
    metrics.communicationFailure(ex);

    if (ex instanceof InvalidBinlogPositionException) {
      stateManager.resetToLastValidState();
    }
    throw new RuntimeException(ex);
  }

  /** Checkpoints the {@link SourceState} for the source at the given {@link Mutation} position. */
  public void commitCheckpoint(final Mutation<?> mutation) {
    stateManager.commitCheckpoint(mutation);
  }
}
