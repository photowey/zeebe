/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.message;

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.mutable.MutableMessageSubscriptionState;
import io.zeebe.engine.util.ZeebeStateRule;
import java.util.ArrayList;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public final class MessageSubscriptionStateTest {

  @Rule public final ZeebeStateRule stateRule = new ZeebeStateRule();

  private MutableMessageSubscriptionState state;

  @Before
  public void setUp() {

    final ZeebeState zeebeState = stateRule.getZeebeState();
    state = zeebeState.getMessageSubscriptionState();
  }

  @Test
  public void shouldNotExistWithDifferentElementKey() {
    // given
    final MessageSubscription subscription = subscriptionWithElementInstanceKey(1);
    state.put(subscription);

    // when
    final boolean exist =
        state.existSubscriptionForElementInstance(2, subscription.getMessageName());

    // then
    assertThat(exist).isFalse();
  }

  @Test
  public void shouldNotExistWithDifferentMessageName() {
    // given
    final MessageSubscription subscription = subscriptionWithElementInstanceKey(1);
    state.put(subscription);

    // when
    final boolean exist =
        state.existSubscriptionForElementInstance(
            subscription.getElementInstanceKey(), wrapString("\0"));

    // then
    assertThat(exist).isFalse();
  }

  @Test
  public void shouldExistSubscription() {
    // given
    final MessageSubscription subscription = subscriptionWithElementInstanceKey(1);
    state.put(subscription);

    // when
    final boolean exist =
        state.existSubscriptionForElementInstance(
            subscription.getElementInstanceKey(), subscription.getMessageName());

    // then
    assertThat(exist).isTrue();
  }

  @Test
  public void shouldVisitSubscription() {
    // given
    final MessageSubscription subscription = subscription("messageName", "correlationKey", 1);
    state.put(subscription);

    // when
    final List<MessageSubscription> subscriptions = new ArrayList<>();
    state.visitSubscriptions(
        wrapString("messageName"), wrapString("correlationKey"), subscriptions::add);

    // then
    assertThat(subscriptions).hasSize(1);
    assertThat(subscriptions.get(0).getProcessInstanceKey())
        .isEqualTo(subscription.getProcessInstanceKey());
    assertThat(subscriptions.get(0).getElementInstanceKey())
        .isEqualTo(subscription.getElementInstanceKey());
    assertThat(subscriptions.get(0).getMessageName()).isEqualTo(subscription.getMessageName());
    assertThat(subscriptions.get(0).getCorrelationKey())
        .isEqualTo(subscription.getCorrelationKey());
    assertThat(subscriptions.get(0).getMessageVariables())
        .isEqualTo(subscription.getMessageVariables());
    assertThat(subscriptions.get(0).getCommandSentTime())
        .isEqualTo(subscription.getCommandSentTime());
  }

  @Test
  public void shouldVisitSubscriptionsInOrder() {
    // given
    state.put(subscription("messageName", "correlationKey", 1));
    state.put(subscription("messageName", "correlationKey", 2));
    state.put(subscription("otherMessageName", "correlationKey", 3));
    state.put(subscription("messageName", "otherCorrelationKey", 4));

    // when
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptions(
        wrapString("messageName"),
        wrapString("correlationKey"),
        s -> keys.add(s.getElementInstanceKey()));

    // then
    assertThat(keys).hasSize(2).containsExactly(1L, 2L);
  }

  @Test
  public void shouldVisitSubsctionsUntilStop() {
    // given
    state.put(subscription("messageName", "correlationKey", 1));
    state.put(subscription("messageName", "correlationKey", 2));

    // when
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptions(
        wrapString("messageName"),
        wrapString("correlationKey"),
        s -> {
          keys.add(s.getElementInstanceKey());
          return false;
        });

    // then
    assertThat(keys).hasSize(1).contains(1L);
  }

  @Test
  public void shouldNoVisitMessageSubscriptionBeforeTime() {
    // given
    final MessageSubscription subscription1 = subscriptionWithElementInstanceKey(1L);
    state.put(subscription1);
    state.updateSentTime(subscription1, 1_000);

    final MessageSubscription subscription2 = subscriptionWithElementInstanceKey(2L);
    state.put(subscription2);
    state.updateSentTime(subscription2, 3_000);

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(1_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).isEmpty();
  }

  @Test
  public void shouldVisitMessageSubscriptionBeforeTime() {
    // given
    final MessageSubscription subscription1 = subscriptionWithElementInstanceKey(1L);
    state.put(subscription1);
    state.updateSentTime(subscription1, 1_000);

    final MessageSubscription subscription2 = subscriptionWithElementInstanceKey(2L);
    state.put(subscription2);
    state.updateSentTime(subscription2, 3_000);

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(1).contains(1L);
  }

  @Test
  public void shouldFindMessageSubscriptionBeforeTimeInOrder() {
    // given
    final MessageSubscription subscription1 = subscriptionWithElementInstanceKey(1L);
    state.put(subscription1);
    state.updateSentTime(subscription1, 1_000);

    final MessageSubscription subscription2 = subscriptionWithElementInstanceKey(2L);
    state.put(subscription2);
    state.updateSentTime(subscription2, 2_000);

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(3_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(2).containsExactly(1L, 2L);
  }

  @Test
  public void shouldNotVisitMessageSubscriptionIfSentTimeNotSet() {
    // given
    final MessageSubscription subscription1 = subscriptionWithElementInstanceKey(1L);
    state.put(subscription1);
    state.updateSentTime(subscription1, 1_000);

    final MessageSubscription subscription2 = subscriptionWithElementInstanceKey(2L);
    state.put(subscription2);

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(1).contains(1L);
  }

  @Test
  public void shouldUpdateMessageSubscriptionSentTime() {
    // given
    final MessageSubscription subscription = subscriptionWithElementInstanceKey(1L);
    state.put(subscription);

    // when
    state.updateSentTime(subscription, 1_000);

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(1).contains(1L);

    // and
    state.updateSentTime(subscription, 1_500);

    keys.clear();
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(1).contains(1L);
  }

  @Test
  public void shouldUpdateCorrelationState() {
    // given
    final MessageSubscription subscription = subscriptionWithElementInstanceKey(1L);
    state.put(subscription);

    assertThat(subscription.isCorrelating()).isFalse();

    // when
    state.updateToCorrelatingState(subscription, wrapString("{\"foo\":\"bar\"}"), 1_000, 5);

    // then
    assertThat(subscription.isCorrelating()).isTrue();

    // and
    final List<MessageSubscription> subscriptions = new ArrayList<>();
    state.visitSubscriptions(
        subscription.getMessageName(), subscription.getCorrelationKey(), subscriptions::add);

    assertThat(subscriptions).hasSize(1);
    assertThat(subscriptions.get(0).getMessageVariables())
        .isEqualTo(subscription.getMessageVariables());
    assertThat(subscriptions.get(0).getMessageKey()).isEqualTo(subscription.getMessageKey());

    // and
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(1).contains(1L);
  }

  @Test
  public void shouldRemoveSubscription() {
    // given
    final MessageSubscription subscription = subscriptionWithElementInstanceKey(1L);
    state.put(subscription);
    state.updateSentTime(subscription, 1_000);

    // when
    state.remove(1L, subscription.getMessageName());

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptions(
        subscription.getMessageName(),
        subscription.getCorrelationKey(),
        s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).isEmpty();

    // and
    state.visitSubscriptionBefore(2_000, s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).isEmpty();

    // and
    assertThat(state.existSubscriptionForElementInstance(1L, subscription.getMessageName()))
        .isFalse();
  }

  @Test
  public void shouldNotFailOnRemoveSubscriptionTwice() {
    // given
    final MessageSubscription subscription = subscriptionWithElementInstanceKey(1L);
    state.put(subscription);

    // when
    state.remove(1L, subscription.getMessageName());
    state.remove(1L, subscription.getMessageName());

    // then
    assertThat(state.existSubscriptionForElementInstance(1L, subscription.getMessageName()))
        .isFalse();
  }

  @Test
  public void shouldNotRemoveSubscriptionOnDifferentKey() {
    // given
    state.put(subscription("messageName", "correlationKey", 1L));
    state.put(subscription("messageName", "correlationKey", 2L));

    // when
    state.remove(2L, wrapString("messageName"));

    // then
    final List<Long> keys = new ArrayList<>();
    state.visitSubscriptions(
        wrapString("messageName"),
        wrapString("correlationKey"),
        s -> keys.add(s.getElementInstanceKey()));

    assertThat(keys).hasSize(1).contains(1L);
  }

  private MessageSubscription subscriptionWithElementInstanceKey(final long elementInstanceKey) {
    return subscription("messageName", "correlationKey", elementInstanceKey);
  }

  private MessageSubscription subscription(
      final String name, final String correlationKey, final long elementInstanceKey) {
    return new MessageSubscription(
        1L,
        elementInstanceKey,
        wrapString("process"),
        wrapString(name),
        wrapString(correlationKey),
        true);
  }
}
