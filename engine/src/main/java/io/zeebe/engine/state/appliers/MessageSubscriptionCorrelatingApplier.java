/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.appliers;

import io.zeebe.engine.state.TypedEventApplier;
import io.zeebe.engine.state.message.MessageSubscription;
import io.zeebe.engine.state.mutable.MutableMessageState;
import io.zeebe.engine.state.mutable.MutableMessageSubscriptionState;
import io.zeebe.protocol.impl.record.value.message.MessageSubscriptionRecord;
import io.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.zeebe.util.sched.clock.ActorClock;

public final class MessageSubscriptionCorrelatingApplier
    implements TypedEventApplier<MessageSubscriptionIntent, MessageSubscriptionRecord> {

  private final MutableMessageSubscriptionState messageSubscriptionState;
  private final MutableMessageState messageState;

  public MessageSubscriptionCorrelatingApplier(
      final MutableMessageSubscriptionState messageSubscriptionState,
      final MutableMessageState messageState) {
    this.messageSubscriptionState = messageSubscriptionState;
    this.messageState = messageState;
  }

  @Override
  public void applyState(final long key, final MessageSubscriptionRecord value) {
    final var subscription =
        new MessageSubscription(
            value.getWorkflowInstanceKey(),
            value.getElementInstanceKey(),
            value.getBpmnProcessIdBuffer(),
            value.getMessageNameBuffer(),
            value.getCorrelationKeyBuffer(),
            value.shouldCloseOnCorrelate());

    // TODO (saig0): the send time for the retry should be deterministic (#6364)
    final var sentTime = ActorClock.currentTimeMillis();
    messageSubscriptionState.updateToCorrelatingState(
        subscription, value.getVariablesBuffer(), sentTime, value.getMessageKey());

    // avoid correlating this message to one instance of this workflow again
    messageState.putMessageCorrelation(value.getMessageKey(), value.getBpmnProcessIdBuffer());
  }
}
