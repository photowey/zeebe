/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.message;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.TransactionContext;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.DbCompositeKey;
import io.zeebe.db.impl.DbLong;
import io.zeebe.db.impl.DbNil;
import io.zeebe.db.impl.DbString;
import io.zeebe.engine.state.ZbColumnFamilies;
import io.zeebe.engine.state.mutable.MutableMessageStartEventSubscriptionState;
import io.zeebe.protocol.impl.record.value.message.MessageStartEventSubscriptionRecord;
import java.util.function.Consumer;
import org.agrona.DirectBuffer;

public final class DbMessageStartEventSubscriptionState
    implements MutableMessageStartEventSubscriptionState {

  private final DbString messageName;
  private final DbLong processKey;

  // (messageName, processKey => MessageSubscription)
  private final DbCompositeKey<DbString, DbLong> messageNameAndProcessKey;
  private final ColumnFamily<DbCompositeKey<DbString, DbLong>, SubscriptionValue>
      subscriptionsColumnFamily;
  private final SubscriptionValue subscriptionValue = new SubscriptionValue();

  // (processKey, messageName) => \0  : to find existing subscriptions of a process
  private final DbCompositeKey<DbLong, DbString> processKeyAndMessageName;
  private final ColumnFamily<DbCompositeKey<DbLong, DbString>, DbNil>
      subscriptionsOfProcessKeyColumnFamily;

  public DbMessageStartEventSubscriptionState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final TransactionContext transactionContext) {
    messageName = new DbString();
    processKey = new DbLong();
    messageNameAndProcessKey = new DbCompositeKey<>(messageName, processKey);
    subscriptionsColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_START_EVENT_SUBSCRIPTION_BY_NAME_AND_KEY,
            transactionContext,
            messageNameAndProcessKey,
            subscriptionValue);

    processKeyAndMessageName = new DbCompositeKey<>(processKey, messageName);
    subscriptionsOfProcessKeyColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.MESSAGE_START_EVENT_SUBSCRIPTION_BY_KEY_AND_NAME,
            transactionContext,
            processKeyAndMessageName,
            DbNil.INSTANCE);
  }

  @Override
  public void put(final MessageStartEventSubscriptionRecord subscription) {
    subscriptionValue.set(subscription);

    messageName.wrapBuffer(subscription.getMessageNameBuffer());
    processKey.wrapLong(subscription.getProcessKey());
    subscriptionsColumnFamily.put(messageNameAndProcessKey, subscriptionValue);
    subscriptionsOfProcessKeyColumnFamily.put(processKeyAndMessageName, DbNil.INSTANCE);
  }

  @Override
  public void removeSubscriptionsOfProcess(final long processKey) {
    this.processKey.wrapLong(processKey);

    subscriptionsOfProcessKeyColumnFamily.whileEqualPrefix(
        this.processKey,
        (key, value) -> {
          subscriptionsColumnFamily.delete(messageNameAndProcessKey);
          subscriptionsOfProcessKeyColumnFamily.delete(key);
        });
  }

  @Override
  public boolean exists(final MessageStartEventSubscriptionRecord subscription) {
    messageName.wrapBuffer(subscription.getMessageNameBuffer());
    processKey.wrapLong(subscription.getProcessKey());

    return subscriptionsColumnFamily.exists(messageNameAndProcessKey);
  }

  @Override
  public void visitSubscriptionsByMessageName(
      final DirectBuffer messageName, final Consumer<MessageStartEventSubscriptionRecord> visitor) {

    this.messageName.wrapBuffer(messageName);
    subscriptionsColumnFamily.whileEqualPrefix(
        this.messageName,
        (key, value) -> {
          visitor.accept(value.get());
        });
  }
}
