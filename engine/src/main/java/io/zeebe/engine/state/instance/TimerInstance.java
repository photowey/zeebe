/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.instance;

import io.zeebe.db.DbValue;
import io.zeebe.msgpack.UnpackedObject;
import io.zeebe.msgpack.property.IntegerProperty;
import io.zeebe.msgpack.property.LongProperty;
import io.zeebe.msgpack.property.StringProperty;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class TimerInstance extends UnpackedObject implements DbValue {

  public static final int NO_ELEMENT_INSTANCE = -1;

  private final StringProperty handlerNodeIdProp = new StringProperty("handlerNodeId", "");
  private final LongProperty processKeyProp = new LongProperty("processKey", 0L);
  private final LongProperty keyProp = new LongProperty("key", 0L);
  private final LongProperty elementInstanceKeyProp = new LongProperty("elementInstanceKey", 0L);
  private final LongProperty processInstanceKeyProp = new LongProperty("processInstanceKey", 0L);
  private final LongProperty dueDateProp = new LongProperty("dueDate", 0L);
  private final IntegerProperty repetitionsProp = new IntegerProperty("repetitions", 0);

  public TimerInstance() {
    declareProperty(handlerNodeIdProp)
        .declareProperty(processKeyProp)
        .declareProperty(keyProp)
        .declareProperty(elementInstanceKeyProp)
        .declareProperty(processInstanceKeyProp)
        .declareProperty(dueDateProp)
        .declareProperty(repetitionsProp);
  }

  public long getElementInstanceKey() {
    return elementInstanceKeyProp.getValue();
  }

  public void setElementInstanceKey(final long elementInstanceKey) {
    elementInstanceKeyProp.setValue(elementInstanceKey);
  }

  public long getDueDate() {
    return dueDateProp.getValue();
  }

  public void setDueDate(final long dueDate) {
    dueDateProp.setValue(dueDate);
  }

  public long getKey() {
    return keyProp.getValue();
  }

  public void setKey(final long key) {
    keyProp.setValue(key);
  }

  public DirectBuffer getHandlerNodeId() {
    return handlerNodeIdProp.getValue();
  }

  public void setHandlerNodeId(final DirectBuffer handlerNodeId) {
    handlerNodeIdProp.setValue(handlerNodeId);
  }

  public int getRepetitions() {
    return repetitionsProp.getValue();
  }

  public void setRepetitions(final int repetitions) {
    repetitionsProp.setValue(repetitions);
  }

  public long getProcessKey() {
    return processKeyProp.getValue();
  }

  public void setProcessKey(final long processKey) {
    processKeyProp.setValue(processKey);
  }

  public long getProcessInstanceKey() {
    return processInstanceKeyProp.getValue();
  }

  public void setProcessInstanceKey(final long processInstanceKey) {
    processInstanceKeyProp.setValue(processInstanceKey);
  }

  @Override
  public void wrap(final DirectBuffer buffer, int offset, final int length) {
    final byte[] bytes = new byte[length];
    final UnsafeBuffer mutableBuffer = new UnsafeBuffer(bytes);
    buffer.getBytes(offset, bytes, 0, length);
    super.wrap(mutableBuffer, 0, length);
  }
}
