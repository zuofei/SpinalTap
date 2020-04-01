/**
 * Copyright 2019 Airbnb. Licensed under Apache-2.0. See License in the project root for license
 * information.
 */
package com.airbnb.spinaltap.mysql.event;

import com.airbnb.spinaltap.mysql.BinlogFilePos;
import com.github.shyiko.mysql.binlog.event.EventType;
import lombok.Getter;

/** Events that are not to be processed */
@Getter
public class IgnoredEvent extends BinlogEvent {
  private final EventType eventType;

  public IgnoredEvent(long serverId, long timestamp, BinlogFilePos filePos, EventType eventType) {
    super(0L, serverId, timestamp, filePos);
    this.eventType = eventType;
  }
}
