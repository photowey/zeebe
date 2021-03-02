/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.client.multipartition;

import static io.zeebe.protocol.Protocol.START_PARTITION_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.broker.it.clustering.ClusteringRule;
import io.zeebe.broker.it.util.GrpcClientRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.protocol.record.intent.MessageIntent;
import io.zeebe.test.util.BrokerClassRuleHelper;
import io.zeebe.test.util.record.ProcessInstances;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public final class MessageCorrelationMultiplePartitionsTest {

  private static final String CORRELATION_KEY_PARTITION_0 = "item-2";
  private static final String CORRELATION_KEY_PARTITION_1 = "item-1";
  private static final String CORRELATION_KEY_PARTITION_2 = "item-0";

  private static final int PARTITION_COUNT = 3;

  private static final ClusteringRule CLUSTERING_RULE = new ClusteringRule(PARTITION_COUNT, 1, 1);
  private static final GrpcClientRule CLIENT_RULE = new GrpcClientRule(CLUSTERING_RULE);

  @ClassRule
  public static RuleChain ruleChain = RuleChain.outerRule(CLUSTERING_RULE).around(CLIENT_RULE);

  @Rule public final BrokerClassRuleHelper helper = new BrokerClassRuleHelper();

  private long processKey;
  private String messageName;

  @Before
  public void init() {
    messageName = helper.getMessageName();

    processKey =
        CLIENT_RULE.deployProcess(
            Bpmn.createExecutableProcess("process")
                .startEvent()
                .intermediateCatchEvent()
                .message(m -> m.name(messageName).zeebeCorrelationKeyExpression("key"))
                .endEvent("end")
                .done());
  }

  @Test
  public void shouldPublishMessageOnDifferentPartitions() {
    // when
    IntStream.range(0, 10)
        .forEach(
            i -> {
              publishMessage(CORRELATION_KEY_PARTITION_0, Map.of("p", "p0"));
              publishMessage(CORRELATION_KEY_PARTITION_1, Map.of("p", "p1"));
              publishMessage(CORRELATION_KEY_PARTITION_2, Map.of("p", "p2"));
            });

    // then
    assertThat(RecordingExporter.messageRecords(MessageIntent.PUBLISHED).limit(30))
        .extracting(r -> tuple(r.getPartitionId(), r.getValue().getCorrelationKey()))
        .hasSize(30)
        .containsOnly(
            tuple(START_PARTITION_ID, CORRELATION_KEY_PARTITION_0),
            tuple(START_PARTITION_ID + 1, CORRELATION_KEY_PARTITION_1),
            tuple(START_PARTITION_ID + 2, CORRELATION_KEY_PARTITION_2));
  }

  @Test
  public void shouldCorrelateMessageOnDifferentPartitions() {
    // given
    publishMessage(CORRELATION_KEY_PARTITION_0, Map.of("p", "p0"));
    publishMessage(CORRELATION_KEY_PARTITION_1, Map.of("p", "p1"));
    publishMessage(CORRELATION_KEY_PARTITION_2, Map.of("p", "p2"));

    // when
    final long wfiKey1 = createProcessInstance(Map.of("key", CORRELATION_KEY_PARTITION_0));
    final long wfiKey2 = createProcessInstance(Map.of("key", CORRELATION_KEY_PARTITION_1));
    final long wfiKey3 = createProcessInstance(Map.of("key", CORRELATION_KEY_PARTITION_2));

    // then
    final List<String> correlatedValues =
        Arrays.asList(
            ProcessInstances.getCurrentVariables(wfiKey1).get("p"),
            ProcessInstances.getCurrentVariables(wfiKey2).get("p"),
            ProcessInstances.getCurrentVariables(wfiKey3).get("p"));

    assertThat(correlatedValues).contains("\"p0\"", "\"p1\"", "\"p2\"");
  }

  private long createProcessInstance(final Object variables) {
    return CLIENT_RULE
        .getClient()
        .newCreateInstanceCommand()
        .processKey(processKey)
        .variables(variables)
        .send()
        .join()
        .getProcessInstanceKey();
  }

  private void publishMessage(final String correlationKey, final Object variables) {
    CLIENT_RULE
        .getClient()
        .newPublishMessageCommand()
        .messageName(messageName)
        .correlationKey(correlationKey)
        .variables(variables)
        .send()
        .join();
  }
}
