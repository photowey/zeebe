/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.message;

import io.zeebe.engine.processing.message.command.SubscriptionCommandSender;
import io.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.zeebe.engine.processing.streamprocessor.TypedRecordProcessor;
import io.zeebe.engine.processing.streamprocessor.sideeffect.SideEffectProducer;
import io.zeebe.engine.processing.streamprocessor.writers.StateWriter;
import io.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.zeebe.engine.processing.streamprocessor.writers.TypedStreamWriter;
import io.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.zeebe.engine.state.immutable.MessageState;
import io.zeebe.engine.state.immutable.MessageSubscriptionState;
import io.zeebe.protocol.impl.record.value.message.MessageSubscriptionRecord;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.zeebe.util.buffer.BufferUtil;
import java.util.function.Consumer;

public final class MessageSubscriptionCreateProcessor
    implements TypedRecordProcessor<MessageSubscriptionRecord> {

  private static final String SUBSCRIPTION_ALREADY_OPENED_MESSAGE =
      "Expected to open a new message subscription for element with key '%d' and message name '%s', "
          + "but there is already a message subscription for that element key and message name opened";

  private final MessageCorrelator messageCorrelator;
  private final MessageSubscriptionState subscriptionState;
  private final SubscriptionCommandSender commandSender;
  private final StateWriter stateWriter;

  private MessageSubscriptionRecord subscriptionRecord;

  public MessageSubscriptionCreateProcessor(
      final MessageState messageState,
      final MessageSubscriptionState subscriptionState,
      final SubscriptionCommandSender commandSender,
      final Writers writers) {
    this.subscriptionState = subscriptionState;
    this.commandSender = commandSender;
    stateWriter = writers.state();
    messageCorrelator = new MessageCorrelator(messageState, commandSender, stateWriter);
  }

  @Override
  public void processRecord(
      final TypedRecord<MessageSubscriptionRecord> record,
      final TypedResponseWriter responseWriter,
      final TypedStreamWriter streamWriter,
      final Consumer<SideEffectProducer> sideEffect) {
    subscriptionRecord = record.getValue();

    if (subscriptionState.existSubscriptionForElementInstance(
        subscriptionRecord.getElementInstanceKey(), subscriptionRecord.getMessageNameBuffer())) {
      sideEffect.accept(this::sendAcknowledgeCommand);

      streamWriter.appendRejection(
          record,
          RejectionType.INVALID_STATE,
          String.format(
              SUBSCRIPTION_ALREADY_OPENED_MESSAGE,
              subscriptionRecord.getElementInstanceKey(),
              BufferUtil.bufferAsString(subscriptionRecord.getMessageNameBuffer())));
      return;
    }

    handleNewSubscription(sideEffect);
  }

  private void handleNewSubscription(final Consumer<SideEffectProducer> sideEffect) {

    // TODO (saig0): the subscription should have a key (#2805)
    stateWriter.appendFollowUpEvent(-1L, MessageSubscriptionIntent.CREATED, subscriptionRecord);

    final var isMessageCorrelated =
        messageCorrelator.correlateNextMessage(subscriptionRecord, sideEffect);

    if (!isMessageCorrelated) {
      sideEffect.accept(this::sendAcknowledgeCommand);
    }
  }

  private boolean sendAcknowledgeCommand() {
    return commandSender.openWorkflowInstanceSubscription(
        subscriptionRecord.getWorkflowInstanceKey(),
        subscriptionRecord.getElementInstanceKey(),
        subscriptionRecord.getMessageNameBuffer(),
        subscriptionRecord.shouldCloseOnCorrelate());
  }
}
