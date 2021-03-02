/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.message;

import static io.zeebe.protocol.Protocol.START_PARTITION_ID;
import static io.zeebe.test.util.MsgPackUtil.asMsgPack;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.assertj.core.data.MapEntry.entry;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.engine.util.client.ProcessInstanceClient.ProcessInstanceCreationClient;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.impl.SubscriptionUtil;
import io.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.zeebe.test.util.collection.Maps;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.ProcessInstances;
import io.zeebe.util.buffer.BufferUtil;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public final class MessageCorrelationMultiplePartitionsTest {

  private static final Map<Integer, String> CORRELATION_KEYS =
      Maps.of(
          entry(START_PARTITION_ID, "item-2"),
          entry(START_PARTITION_ID + 1, "item-1"),
          entry(START_PARTITION_ID + 2, "item-0"));

  private static final String PROCESS_ID = "process";

  private static final BpmnModelInstance PROCESS =
      Bpmn.createExecutableProcess(PROCESS_ID)
          .startEvent()
          .intermediateCatchEvent("receive-message")
          .message(m -> m.name("message").zeebeCorrelationKeyExpression("key"))
          .endEvent("end")
          .done();

  @Rule public final EngineRule engine = EngineRule.multiplePartition(3);

  @Before
  public void init() {
    assertThat(getPartitionId(CORRELATION_KEYS.get(START_PARTITION_ID)))
        .isEqualTo(START_PARTITION_ID);
    assertThat(getPartitionId(CORRELATION_KEYS.get(START_PARTITION_ID + 1)))
        .isEqualTo(START_PARTITION_ID + 1);
    assertThat(getPartitionId(CORRELATION_KEYS.get(START_PARTITION_ID + 2)))
        .isEqualTo(START_PARTITION_ID + 2);

    engine.deployment().withXmlResource(PROCESS).deploy();
  }

  @Test
  public void shouldOpenMessageSubscriptionsOnDifferentPartitions() {
    // when
    IntStream.range(0, 10)
        .forEach(
            i -> {
              final ProcessInstanceCreationClient processInstanceCreationClient =
                  engine.processInstance().ofBpmnProcessId(PROCESS_ID);
              processInstanceCreationClient
                  .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID))
                  .create();
              processInstanceCreationClient
                  .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID + 1))
                  .create();
              processInstanceCreationClient
                  .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID + 2))
                  .create();
            });

    // then
    assertThat(
            RecordingExporter.messageSubscriptionRecords(MessageSubscriptionIntent.OPENED)
                .limit(30))
        .extracting(r -> tuple(r.getPartitionId(), r.getValue().getCorrelationKey()))
        .containsOnly(
            tuple(START_PARTITION_ID, CORRELATION_KEYS.get(START_PARTITION_ID)),
            tuple(START_PARTITION_ID + 1, CORRELATION_KEYS.get(START_PARTITION_ID + 1)),
            tuple(START_PARTITION_ID + 2, CORRELATION_KEYS.get(START_PARTITION_ID + 2)));
  }

  @Test
  public void shouldCorrelateMessageOnDifferentPartitions() {
    // given
    engine.forEachPartition(
        partitionId ->
            engine
                .message()
                .onPartition(partitionId)
                .withName("message")
                .withCorrelationKey(CORRELATION_KEYS.get(partitionId))
                .withVariables(asMsgPack("p", "p" + partitionId))
                .publish());

    // when
    final ProcessInstanceCreationClient processInstanceCreationClient =
        engine.processInstance().ofBpmnProcessId(PROCESS_ID);
    final long wfiKey1 =
        processInstanceCreationClient
            .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID))
            .create();
    final long wfiKey2 =
        processInstanceCreationClient
            .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID + 1))
            .create();
    final long wfiKey3 =
        processInstanceCreationClient
            .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID + 2))
            .create();

    // then
    final List<String> correlatedValues =
        Arrays.asList(
            ProcessInstances.getCurrentVariables(wfiKey1).get("p"),
            ProcessInstances.getCurrentVariables(wfiKey2).get("p"),
            ProcessInstances.getCurrentVariables(wfiKey3).get("p"));

    assertThat(correlatedValues).contains("\"p1\"", "\"p2\"", "\"p3\"");
  }

  @Test
  public void shouldOpenMessageSubscriptionsOnSamePartitionsAfterRestart() {
    // given
    final ProcessInstanceCreationClient processInstanceCreationClient =
        engine.processInstance().ofBpmnProcessId(PROCESS_ID);

    IntStream.range(0, 5)
        .forEach(
            i -> {
              processInstanceCreationClient
                  .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID))
                  .create();
              processInstanceCreationClient
                  .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID + 1))
                  .create();
              processInstanceCreationClient
                  .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID + 2))
                  .create();
            });

    assertThat(
            RecordingExporter.messageSubscriptionRecords(MessageSubscriptionIntent.OPENED)
                .limit(15)
                .count())
        .isEqualTo(15);

    // when
    engine.stop();
    RecordingExporter.reset();
    engine.start();

    IntStream.range(0, 5)
        .forEach(
            i -> {
              processInstanceCreationClient
                  .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID))
                  .create();
              processInstanceCreationClient
                  .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID + 1))
                  .create();
              processInstanceCreationClient
                  .withVariable("key", CORRELATION_KEYS.get(START_PARTITION_ID + 2))
                  .create();
            });

    // then
    assertThat(
            RecordingExporter.messageSubscriptionRecords(MessageSubscriptionIntent.OPENED)
                .limit(30))
        .extracting(r -> tuple(r.getPartitionId(), r.getValue().getCorrelationKey()))
        .hasSize(30)
        .containsOnly(
            tuple(START_PARTITION_ID, CORRELATION_KEYS.get(START_PARTITION_ID)),
            tuple(START_PARTITION_ID + 1, CORRELATION_KEYS.get(START_PARTITION_ID + 1)),
            tuple(START_PARTITION_ID + 2, CORRELATION_KEYS.get(START_PARTITION_ID + 2)));
  }

  private int getPartitionId(final String correlationKey) {
    final List<Integer> partitionIds = engine.getPartitionIds();
    return SubscriptionUtil.getSubscriptionPartitionId(
        BufferUtil.wrapString(correlationKey), partitionIds.size());
  }
}
