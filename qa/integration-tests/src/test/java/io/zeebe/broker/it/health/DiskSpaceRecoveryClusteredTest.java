/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.health;

import static io.zeebe.broker.it.util.ZeebeAssertHelper.assertProcessInstanceCompleted;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.broker.Broker;
import io.zeebe.broker.it.clustering.ClusteringRule;
import io.zeebe.broker.it.util.GrpcClientRule;
import io.zeebe.broker.system.monitoring.DiskSpaceUsageListener;
import io.zeebe.client.api.response.DeploymentEvent;
import io.zeebe.engine.processing.message.MessageObserver;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import io.zeebe.protocol.record.intent.MessageSubscriptionIntent;
import io.zeebe.protocol.record.intent.ProcessInstanceSubscriptionIntent;
import io.zeebe.test.util.record.RecordingExporter;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;
import org.springframework.util.unit.DataSize;

public class DiskSpaceRecoveryClusteredTest {
  private static final String CORRELATION_KEY = "item-2";
  private final String messageName = "test";
  private final Timeout testTimeout = Timeout.seconds(120);
  private final ClusteringRule clusteringRule =
      new ClusteringRule(
          3,
          1,
          3,
          cfg -> {
            cfg.getData().setDiskUsageMonitoringInterval(Duration.ofSeconds(1));
          });
  private final GrpcClientRule clientRule = new GrpcClientRule(clusteringRule);

  @Rule
  public RuleChain ruleChain =
      RuleChain.outerRule(testTimeout).around(clusteringRule).around(clientRule);

  @Test
  public void shouldDistributeDeploymentAfterDiskSpaceAvailableAgain() throws InterruptedException {
    // given
    final var failingBroker =
        clusteringRule.getBroker(clusteringRule.getLeaderForPartition(3).getNodeId());
    waitUntilDiskSpaceNotAvailable(failingBroker);

    final long deploymentKey =
        deployProcess(Bpmn.createExecutableProcess("test").startEvent().endEvent().done());

    // when
    Awaitility.await()
        .timeout(Duration.ofSeconds(60))
        .until(
            () ->
                RecordingExporter.deploymentRecords(DeploymentIntent.DISTRIBUTE).limit(1).exists());

    waitUntilDiskSpaceAvailable(failingBroker);

    // then
    clientRule.waitUntilDeploymentIsDone(deploymentKey);
  }

  @Test
  public void shouldCorrelateMessageAfterDiskSpaceAvailableAgain() throws InterruptedException {
    // given
    final var failingBroker =
        clusteringRule.getBroker(clusteringRule.getLeaderForPartition(3).getNodeId());
    final long processKey = deployProcessWithMessage("process1");

    final long processInstanceKey1 =
        createProcessInstance(Map.of("key", CORRELATION_KEY), processKey);
    final long processInstanceKey2 =
        createProcessInstance(Map.of("key", CORRELATION_KEY), processKey);
    final long processInstanceKey3 =
        createProcessInstance(Map.of("key", CORRELATION_KEY), processKey);

    Awaitility.await()
        .timeout(Duration.ofSeconds(60))
        .until(
            () ->
                RecordingExporter.messageSubscriptionRecords(MessageSubscriptionIntent.OPENED)
                        .limit(3)
                        .count()
                    == 3);
    waitUntilDiskSpaceNotAvailable(failingBroker);

    // when
    publishMessage(CORRELATION_KEY, "1");
    publishMessage(CORRELATION_KEY, "2");
    publishMessage(CORRELATION_KEY, "3");
    Awaitility.await()
        .timeout(Duration.ofSeconds(60))
        .untilAsserted(
            () ->
                assertThat(
                        RecordingExporter.processInstanceSubscriptionRecords(
                                ProcessInstanceSubscriptionIntent.CORRELATED)
                            .limit(2)
                            .count())
                    .isEqualTo(2));

    waitUntilDiskSpaceAvailable(failingBroker);

    final var timeout = MessageObserver.SUBSCRIPTION_CHECK_INTERVAL.multipliedBy(2);
    clusteringRule.getClock().addTime(timeout);

    // then
    Awaitility.await()
        .timeout(Duration.ofSeconds(60))
        .untilAsserted(() -> assertProcessInstanceCompleted(processInstanceKey1));
    Awaitility.await()
        .timeout(Duration.ofSeconds(60))
        .untilAsserted(() -> assertProcessInstanceCompleted(processInstanceKey2));
    Awaitility.await()
        .timeout(Duration.ofSeconds(60))
        .untilAsserted(() -> assertProcessInstanceCompleted(processInstanceKey3));
  }

  private void publishMessage(final String correlationKey, final String messageId) {
    clientRule
        .getClient()
        .newPublishMessageCommand()
        .messageName(messageName)
        .correlationKey(correlationKey)
        .messageId(messageId)
        .timeToLive(Duration.ofMinutes(1))
        .send()
        .join();
  }

  private long deployProcess(final BpmnModelInstance modelInstance) {
    final DeploymentEvent deploymentEvent =
        clientRule
            .getClient()
            .newDeployCommand()
            .addProcessModel(modelInstance, "process.bpmn")
            .send()
            .join();
    return deploymentEvent.getKey();
  }

  private long deployProcessWithMessage(final String processId) {
    final BpmnModelInstance process =
        Bpmn.createExecutableProcess(processId)
            .startEvent()
            .intermediateCatchEvent()
            .message(m -> m.name(messageName).zeebeCorrelationKeyExpression("key"))
            .endEvent("end")
            .done();
    return clientRule.deployProcess(process);
  }

  private void waitUntilDiskSpaceNotAvailable(final Broker broker) throws InterruptedException {
    final var diskSpaceMonitor = broker.getDiskSpaceUsageMonitor();

    final CountDownLatch diskSpaceNotAvailable = new CountDownLatch(1);
    diskSpaceMonitor.addDiskUsageListener(
        new DiskSpaceUsageListener() {
          @Override
          public void onDiskSpaceNotAvailable() {
            diskSpaceNotAvailable.countDown();
          }

          @Override
          public void onDiskSpaceAvailable() {}
        });

    diskSpaceMonitor.setFreeDiskSpaceSupplier(() -> DataSize.ofGigabytes(0).toBytes());

    clusteringRule.getClock().addTime(Duration.ofSeconds(1));

    // when
    assertThat(diskSpaceNotAvailable.await(2, TimeUnit.SECONDS)).isTrue();
  }

  private void waitUntilDiskSpaceAvailable(final Broker broker) throws InterruptedException {
    final var diskSpaceMonitor = broker.getDiskSpaceUsageMonitor();
    final CountDownLatch diskSpaceAvailableAgain = new CountDownLatch(1);
    diskSpaceMonitor.addDiskUsageListener(
        new DiskSpaceUsageListener() {
          @Override
          public void onDiskSpaceAvailable() {
            diskSpaceAvailableAgain.countDown();
          }
        });

    diskSpaceMonitor.setFreeDiskSpaceSupplier(() -> DataSize.ofGigabytes(100).toBytes());
    clusteringRule.getClock().addTime(Duration.ofSeconds(1));
    assertThat(diskSpaceAvailableAgain.await(2, TimeUnit.SECONDS)).isTrue();
  }

  private long createProcessInstance(final Object variables, final long processKey) {
    return clientRule
        .getClient()
        .newCreateInstanceCommand()
        .processKey(processKey)
        .variables(variables)
        .send()
        .join()
        .getProcessInstanceKey();
  }
}
