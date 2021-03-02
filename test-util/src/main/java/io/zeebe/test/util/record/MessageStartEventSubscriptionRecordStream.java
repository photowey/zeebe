/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test.util.record;

import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.value.MessageStartEventSubscriptionRecordValue;
import java.util.stream.Stream;

public final class MessageStartEventSubscriptionRecordStream
    extends ExporterRecordStream<
        MessageStartEventSubscriptionRecordValue, MessageStartEventSubscriptionRecordStream> {

  public MessageStartEventSubscriptionRecordStream(
      final Stream<Record<MessageStartEventSubscriptionRecordValue>> wrappedStream) {
    super(wrappedStream);
  }

  @Override
  protected MessageStartEventSubscriptionRecordStream supply(
      final Stream<Record<MessageStartEventSubscriptionRecordValue>> wrappedStream) {
    return new MessageStartEventSubscriptionRecordStream((wrappedStream));
  }

  public MessageStartEventSubscriptionRecordStream withWorkfloKey(final long processKey) {
    return valueFilter(v -> v.getProcessKey() == processKey);
  }

  public MessageStartEventSubscriptionRecordStream withStartEventId(final String startEventId) {
    return valueFilter(v -> startEventId.equals(v.getStartEventId()));
  }

  public MessageStartEventSubscriptionRecordStream withMessageName(final String messageName) {
    return valueFilter(v -> messageName.equals(v.getMessageName()));
  }
}
