/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.message;

import static io.zeebe.test.util.MsgPackUtil.asMsgPack;
import static io.zeebe.test.util.TestUtil.waitUntil;
import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.zeebe.engine.processing.message.command.SubscriptionCommandSender;
import io.zeebe.engine.util.StreamProcessorRule;
import io.zeebe.protocol.impl.record.value.message.MessageRecord;
import io.zeebe.protocol.impl.record.value.message.MessageSubscriptionRecord;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.intent.MessageIntent;
import io.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.zeebe.util.buffer.BufferUtil;
import java.time.Duration;
import org.agrona.DirectBuffer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public final class MessageStreamProcessorTest {

  @Rule public final StreamProcessorRule rule = new StreamProcessorRule();

  @Mock private SubscriptionCommandSender mockSubscriptionCommandSender;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(mockSubscriptionCommandSender.openProcessInstanceSubscription(
            anyLong(), anyLong(), any(), anyBoolean()))
        .thenReturn(true);

    when(mockSubscriptionCommandSender.correlateProcessInstanceSubscription(
            anyLong(), anyLong(), any(), any(), anyLong(), any(), any()))
        .thenReturn(true);

    when(mockSubscriptionCommandSender.closeProcessInstanceSubscription(
            anyLong(), anyLong(), any(DirectBuffer.class)))
        .thenReturn(true);

    rule.startTypedStreamProcessor(
        (typedRecordProcessors, processingContext) -> {
          final var zeebeState = processingContext.getZeebeState();
          MessageEventProcessors.addMessageProcessors(
              typedRecordProcessors,
              zeebeState,
              mockSubscriptionCommandSender,
              processingContext.getWriters());
          return typedRecordProcessors;
        });
  }

  @Test
  public void shouldRejectDuplicatedOpenMessageSubscription() {
    // given
    final MessageSubscriptionRecord subscription = messageSubscription();

    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscription);

    // when
    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscription);

    // then
    final Record<MessageSubscriptionRecord> rejection = awaitAndGetFirstSubscriptionRejection();

    assertThat(rejection.getIntent()).isEqualTo(MessageSubscriptionIntent.OPEN);
    assertThat(rejection.getRejectionType()).isEqualTo(RejectionType.INVALID_STATE);

    verify(mockSubscriptionCommandSender, timeout(5_000).times(2))
        .openProcessInstanceSubscription(
            eq(subscription.getProcessInstanceKey()),
            eq(subscription.getElementInstanceKey()),
            any(),
            anyBoolean());
  }

  @Test
  public void shouldRetryToCorrelateMessageSubscriptionAfterPublishedMessage() {
    // given
    final MessageSubscriptionRecord subscription = messageSubscription();
    final MessageRecord message = message();

    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscription);
    rule.writeCommand(MessageIntent.PUBLISH, message);
    waitUntil(
        () -> rule.events().onlyMessageRecords().withIntent(MessageIntent.PUBLISHED).exists());

    // when
    rule.getClock()
        .addTime(
            MessageObserver.SUBSCRIPTION_CHECK_INTERVAL.plus(MessageObserver.SUBSCRIPTION_TIMEOUT));

    // then
    final long messageKey =
        rule.events().onlyMessageRecords().withIntent(MessageIntent.PUBLISHED).getFirst().getKey();
    verify(mockSubscriptionCommandSender, timeout(5_000).times(2))
        .correlateProcessInstanceSubscription(
            subscription.getProcessInstanceKey(),
            subscription.getElementInstanceKey(),
            subscription.getBpmnProcessIdBuffer(),
            subscription.getMessageNameBuffer(),
            messageKey,
            message.getVariablesBuffer(),
            subscription.getCorrelationKeyBuffer());
  }

  @Test
  public void shouldRetryToCorrelateMessageSubscriptionAfterOpenedSubscription() {
    // given
    final MessageSubscriptionRecord subscription = messageSubscription();
    final MessageRecord message = message();

    rule.writeCommand(MessageIntent.PUBLISH, message);
    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscription);

    waitUntil(
        () ->
            rule.events()
                .onlyMessageSubscriptionRecords()
                .withIntent(MessageSubscriptionIntent.OPENED)
                .exists());

    // when
    rule.getClock()
        .addTime(
            MessageObserver.SUBSCRIPTION_CHECK_INTERVAL.plus(MessageObserver.SUBSCRIPTION_TIMEOUT));

    // then
    final long messageKey =
        rule.events().onlyMessageRecords().withIntent(MessageIntent.PUBLISHED).getFirst().getKey();

    verify(mockSubscriptionCommandSender, timeout(5_000).times(2))
        .correlateProcessInstanceSubscription(
            subscription.getProcessInstanceKey(),
            subscription.getElementInstanceKey(),
            subscription.getBpmnProcessIdBuffer(),
            subscription.getMessageNameBuffer(),
            messageKey,
            message.getVariablesBuffer(),
            subscription.getCorrelationKeyBuffer());
  }

  @Test
  public void shouldRejectCorrelateIfMessageSubscriptionClosed() {
    // given
    final MessageSubscriptionRecord subscription = messageSubscription();
    final MessageRecord message = message();

    rule.writeCommand(MessageIntent.PUBLISH, message);
    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscription);
    waitUntil(
        () ->
            rule.events()
                .onlyMessageSubscriptionRecords()
                .withIntent(MessageSubscriptionIntent.OPENED)
                .exists());

    // when
    rule.writeCommand(MessageSubscriptionIntent.CLOSE, subscription);
    rule.writeCommand(MessageSubscriptionIntent.CORRELATE, subscription);

    // then
    final Record<MessageSubscriptionRecord> rejection = awaitAndGetFirstSubscriptionRejection();

    assertThat(rejection.getIntent()).isEqualTo(MessageSubscriptionIntent.CORRELATE);
    assertThat(rejection.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldRejectDuplicatedCloseMessageSubscription() {
    // given
    final MessageSubscriptionRecord subscription = messageSubscription();
    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscription);

    waitUntil(
        () ->
            rule.events()
                .onlyMessageSubscriptionRecords()
                .withIntent(MessageSubscriptionIntent.OPENED)
                .exists());

    // when
    rule.writeCommand(MessageSubscriptionIntent.CLOSE, subscription);
    rule.writeCommand(MessageSubscriptionIntent.CLOSE, subscription);

    // then
    final Record<MessageSubscriptionRecord> rejection = awaitAndGetFirstSubscriptionRejection();

    assertThat(rejection.getIntent()).isEqualTo(MessageSubscriptionIntent.CLOSE);
    assertThat(rejection.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);

    // cannot verify messageName buffer since it is a view around another buffer which is changed
    // by the time we perform the verification.
    verify(mockSubscriptionCommandSender, timeout(5_000).times(2))
        .closeProcessInstanceSubscription(
            eq(subscription.getProcessInstanceKey()),
            eq(subscription.getElementInstanceKey()),
            any(DirectBuffer.class));
  }

  @Test
  public void shouldNotCorrelateNewMessagesIfSubscriptionNotCorrelatable() {
    // given
    final MessageSubscriptionRecord subscription = messageSubscription();
    final MessageRecord message = message();

    // when
    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscription);
    rule.writeCommand(MessageIntent.PUBLISH, message);
    waitUntil(
        () -> rule.events().onlyMessageRecords().withIntent(MessageIntent.PUBLISHED).exists());
    rule.writeCommand(MessageIntent.PUBLISH, message);

    // then
    final long messageKey =
        rule.events().onlyMessageRecords().withIntent(MessageIntent.PUBLISHED).getFirst().getKey();

    verify(mockSubscriptionCommandSender, timeout(5_000).times(1))
        .correlateProcessInstanceSubscription(
            eq(subscription.getProcessInstanceKey()),
            eq(subscription.getElementInstanceKey()),
            any(),
            any(),
            eq(messageKey),
            any(),
            any());
  }

  @Test
  public void shouldCorrelateNewMessagesIfSubscriptionIsReusable() {
    // given
    final MessageSubscriptionRecord subscription = messageSubscription();
    final MessageRecord message = message();
    subscription.setCloseOnCorrelate(false);

    // when
    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscription);
    rule.writeCommand(MessageIntent.PUBLISH, message);
    rule.writeCommand(MessageSubscriptionIntent.CORRELATE, subscription);
    rule.writeCommand(MessageIntent.PUBLISH, message);

    // then
    waitUntil(
        () ->
            rule.events().onlyMessageRecords().withIntent(MessageIntent.PUBLISHED).limit(2).count()
                == 2);
    final long firstMessageKey =
        rule.events().onlyMessageRecords().withIntent(MessageIntent.PUBLISHED).getFirst().getKey();
    final long lastMessageKey =
        rule.events()
            .onlyMessageRecords()
            .withIntent(MessageIntent.PUBLISHED)
            .skip(1)
            .getFirst()
            .getKey();

    verify(mockSubscriptionCommandSender, timeout(5_000))
        .correlateProcessInstanceSubscription(
            eq(subscription.getProcessInstanceKey()),
            eq(subscription.getElementInstanceKey()),
            eq(subscription.getBpmnProcessIdBuffer()),
            any(),
            eq(firstMessageKey),
            any(),
            any());

    verify(mockSubscriptionCommandSender, timeout(5_000))
        .correlateProcessInstanceSubscription(
            eq(subscription.getProcessInstanceKey()),
            eq(subscription.getElementInstanceKey()),
            eq(subscription.getBpmnProcessIdBuffer()),
            any(),
            eq(lastMessageKey),
            any(),
            any());
  }

  @Test
  public void shouldCorrelateMultipleMessagesOneBeforeOpenOneAfter() {
    // given
    final MessageSubscriptionRecord subscription = messageSubscription().setCloseOnCorrelate(false);
    final MessageRecord first = message().setVariables(asMsgPack("foo", "bar"));
    final MessageRecord second = message().setVariables(asMsgPack("foo", "baz"));

    // when
    rule.writeCommand(MessageIntent.PUBLISH, first);
    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscription);

    waitUntil(
        () ->
            rule.events()
                .onlyMessageSubscriptionRecords()
                .withIntent(MessageSubscriptionIntent.OPENED)
                .exists());

    rule.writeCommand(MessageSubscriptionIntent.CORRELATE, subscription);
    rule.writeCommand(MessageIntent.PUBLISH, second);

    // then
    assertAllMessagesReceived(subscription, first, second);
  }

  @Test
  public void shouldCorrelateMultipleMessagesTwoBeforeOpen() {
    // given
    final MessageSubscriptionRecord subscription = messageSubscription().setCloseOnCorrelate(false);
    final MessageRecord first = message().setVariables(asMsgPack("foo", "bar"));
    final MessageRecord second = message().setVariables(asMsgPack("foo", "baz"));

    // when
    rule.writeCommand(MessageIntent.PUBLISH, first);
    rule.writeCommand(MessageIntent.PUBLISH, second);
    rule.writeCommand(MessageSubscriptionIntent.OPEN, subscription);

    waitUntil(
        () ->
            rule.events()
                .onlyMessageSubscriptionRecords()
                .withIntent(MessageSubscriptionIntent.OPENED)
                .exists());
    rule.writeCommand(MessageSubscriptionIntent.CORRELATE, subscription);

    // then
    assertAllMessagesReceived(subscription, first, second);
  }

  @Test
  public void shouldCorrelateToFirstSubscriptionAfterRejection() {
    // given
    final MessageRecord message = message();
    final MessageSubscriptionRecord firstSubscription =
        messageSubscription().setElementInstanceKey(5L);
    final MessageSubscriptionRecord secondSubscription =
        messageSubscription().setElementInstanceKey(10L);

    // when
    rule.writeCommand(MessageIntent.PUBLISH, message);
    rule.writeCommand(MessageSubscriptionIntent.OPEN, firstSubscription);
    rule.writeCommand(MessageSubscriptionIntent.OPEN, secondSubscription);
    waitUntil(
        () ->
            rule.events()
                .onlyMessageSubscriptionRecords()
                .withIntent(MessageSubscriptionIntent.OPENED)
                .filter(
                    r ->
                        r.getValue().getElementInstanceKey()
                            == secondSubscription.getElementInstanceKey())
                .exists());

    final long messageKey =
        rule.events().onlyMessageRecords().withIntent(MessageIntent.PUBLISHED).getFirst().getKey();
    firstSubscription.setMessageKey(messageKey);
    rule.writeCommand(MessageSubscriptionIntent.REJECT, firstSubscription);

    // then
    verify(mockSubscriptionCommandSender, timeout(5_000))
        .correlateProcessInstanceSubscription(
            eq(firstSubscription.getProcessInstanceKey()),
            eq(firstSubscription.getElementInstanceKey()),
            eq(firstSubscription.getBpmnProcessIdBuffer()),
            any(DirectBuffer.class),
            eq(messageKey),
            any(DirectBuffer.class),
            eq(firstSubscription.getCorrelationKeyBuffer()));

    verify(mockSubscriptionCommandSender, timeout(5_000))
        .correlateProcessInstanceSubscription(
            eq(secondSubscription.getProcessInstanceKey()),
            eq(secondSubscription.getElementInstanceKey()),
            eq(secondSubscription.getBpmnProcessIdBuffer()),
            any(DirectBuffer.class),
            eq(messageKey),
            any(DirectBuffer.class),
            eq(secondSubscription.getCorrelationKeyBuffer()));
  }

  private void assertAllMessagesReceived(
      final MessageSubscriptionRecord subscription,
      final MessageRecord first,
      final MessageRecord second) {
    final ArgumentCaptor<DirectBuffer> nameCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
    final ArgumentCaptor<DirectBuffer> variablesCaptor =
        ArgumentCaptor.forClass(DirectBuffer.class);

    waitUntil(
        () ->
            rule.events().onlyMessageRecords().withIntent(MessageIntent.PUBLISHED).limit(2).count()
                == 2);
    final long firstMessageKey =
        rule.events().onlyMessageRecords().withIntent(MessageIntent.PUBLISHED).getFirst().getKey();
    final long lastMessageKey =
        rule.events()
            .onlyMessageRecords()
            .withIntent(MessageIntent.PUBLISHED)
            .skip(1)
            .getFirst()
            .getKey();

    verify(mockSubscriptionCommandSender, timeout(5_000))
        .correlateProcessInstanceSubscription(
            eq(subscription.getProcessInstanceKey()),
            eq(subscription.getElementInstanceKey()),
            eq(subscription.getBpmnProcessIdBuffer()),
            nameCaptor.capture(),
            eq(firstMessageKey),
            variablesCaptor.capture(),
            eq(subscription.getCorrelationKeyBuffer()));

    verify(mockSubscriptionCommandSender, timeout(5_000))
        .correlateProcessInstanceSubscription(
            eq(subscription.getProcessInstanceKey()),
            eq(subscription.getElementInstanceKey()),
            eq(subscription.getBpmnProcessIdBuffer()),
            nameCaptor.capture(),
            eq(lastMessageKey),
            variablesCaptor.capture(),
            eq(subscription.getCorrelationKeyBuffer()));

    assertThat(variablesCaptor.getAllValues().get(0)).isEqualTo(first.getVariablesBuffer());
    assertThat(nameCaptor.getValue()).isEqualTo(subscription.getMessageNameBuffer());
    assertThat(BufferUtil.equals(nameCaptor.getAllValues().get(1), second.getNameBuffer()))
        .isTrue();
    assertThat(
            BufferUtil.equals(variablesCaptor.getAllValues().get(1), second.getVariablesBuffer()))
        .isTrue();
  }

  private MessageSubscriptionRecord messageSubscription() {
    final MessageSubscriptionRecord subscription = new MessageSubscriptionRecord();
    subscription
        .setProcessInstanceKey(1L)
        .setElementInstanceKey(2L)
        .setBpmnProcessId(wrapString("process"))
        .setMessageKey(-1L)
        .setMessageName(wrapString("order canceled"))
        .setCorrelationKey(wrapString("order-123"))
        .setCloseOnCorrelate(true);

    return subscription;
  }

  private MessageRecord message() {
    final MessageRecord message = new MessageRecord();
    message
        .setName(wrapString("order canceled"))
        .setCorrelationKey(wrapString("order-123"))
        .setTimeToLive(Duration.ofSeconds(10).toMillis())
        .setVariables(asMsgPack("orderId", "order-123"));

    return message;
  }

  private Record<MessageSubscriptionRecord> awaitAndGetFirstSubscriptionRejection() {
    waitUntil(
        () ->
            rule.events()
                .onlyMessageSubscriptionRecords()
                .onlyRejections()
                .findFirst()
                .isPresent());

    return rule.events().onlyMessageSubscriptionRecords().onlyRejections().findFirst().get();
  }
}
