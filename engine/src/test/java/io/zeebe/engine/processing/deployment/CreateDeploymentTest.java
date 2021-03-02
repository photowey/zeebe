/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.deployment;

import static io.zeebe.protocol.Protocol.DEPLOYMENT_PARTITION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.builder.ProcessBuilder;
import io.zeebe.model.bpmn.instance.Message;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.ExecuteCommandResponseDecoder;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.intent.DeploymentIntent;
import io.zeebe.protocol.record.value.DeploymentRecordValue;
import io.zeebe.protocol.record.value.deployment.DeployedProcess;
import io.zeebe.protocol.record.value.deployment.DeploymentResource;
import io.zeebe.test.util.Strings;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class CreateDeploymentTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  private String processId;
  private String processId2;
  private BpmnModelInstance process;
  private BpmnModelInstance process2;
  private BpmnModelInstance process_V2;
  private BpmnModelInstance process2_V2;

  private BpmnModelInstance createProcess(String processId, String startEventId) {
    return Bpmn.createExecutableProcess(processId).startEvent(startEventId).endEvent().done();
  }

  @Before
  public void init() {
    processId = Strings.newRandomValidBpmnId();
    processId2 = Strings.newRandomValidBpmnId();
    process = createProcess(processId, "v1");
    process2 = createProcess(processId2, "v1");
    process_V2 = createProcess(processId, "v2");
    process2_V2 = createProcess(processId2, "v2");
  }

  @Test
  public void shouldCreateDeploymentWithBpmnXml() {
    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource(process).deploy();

    // then
    assertThat(deployment.getKey()).isNotNegative();

    Assertions.assertThat(deployment)
        .hasPartitionId(DEPLOYMENT_PARTITION)
        .hasRecordType(RecordType.EVENT)
        .hasIntent(DeploymentIntent.CREATED);
  }

  @Test
  public void shouldCreateDeploymentWithProcessWhichHaveUniqueKeys() {
    // given
    final BpmnModelInstance process =
        Bpmn.createExecutableProcess("process").startEvent().endEvent().done();

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource(process).deploy();

    // then
    final long processKey = deployment.getValue().getDeployedProcesses().get(0).getProcessKey();
    final long deploymentKey = deployment.getKey();
    assertThat(processKey).isNotEqualTo(deploymentKey);
  }

  @Test
  public void shouldReturnDeployedProcessDefinitions() {
    // when
    final Record<DeploymentRecordValue> firstDeployment =
        ENGINE.deployment().withXmlResource("wf1.bpmn", process).deploy();
    final Record<DeploymentRecordValue> secondDeployment =
        ENGINE.deployment().withXmlResource("wf2.bpmn", process).deploy();

    // then
    List<DeployedProcess> deployedProcesses = firstDeployment.getValue().getDeployedProcesses();
    assertThat(deployedProcesses).hasSize(1);

    DeployedProcess deployedProcess = deployedProcesses.get(0);
    assertThat(deployedProcess.getBpmnProcessId()).isEqualTo(processId);
    assertThat(deployedProcess.getResourceName()).isEqualTo("wf1.bpmn");

    deployedProcesses = secondDeployment.getValue().getDeployedProcesses();
    assertThat(deployedProcesses).hasSize(1);

    deployedProcess = deployedProcesses.get(0);
    assertThat(deployedProcess.getBpmnProcessId()).isEqualTo(processId);
    assertThat(deployedProcess.getResourceName()).isEqualTo("wf2.bpmn");
  }

  @Test
  public void shouldCreateDeploymentResourceWithCollaboration() {
    // given
    final InputStream resourceAsStream =
        getClass().getResourceAsStream("/processes/collaboration.bpmn");
    final BpmnModelInstance modelInstance = Bpmn.readModelFromStream(resourceAsStream);

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource("collaboration.bpmn", modelInstance).deploy();

    // then
    assertThat(deployment.getValue().getDeployedProcesses())
        .extracting(DeployedProcess::getBpmnProcessId)
        .contains("process1", "process2");
  }

  @Test
  public void shouldCreateDeploymentResourceWithMultipleProcesses() {
    // given

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE
            .deployment()
            .withXmlResource("process.bpmn", process)
            .withXmlResource("process2.bpmn", process2)
            .deploy();

    // then
    assertThat(deployment.getValue().getDeployedProcesses())
        .extracting(DeployedProcess::getBpmnProcessId)
        .contains(processId, processId2);

    assertThat(deployment.getValue().getResources())
        .extracting(DeploymentResource::getResourceName)
        .contains("process.bpmn", "process2.bpmn");

    assertThat(deployment.getValue().getResources())
        .extracting(DeploymentResource::getResource)
        .contains(
            Bpmn.convertToString(process).getBytes(), Bpmn.convertToString(process2).getBytes());
  }

  @Test
  public void shouldWriteProcessRecordsOnDeployment() {
    // given

    // when
    final var deployment =
        ENGINE
            .deployment()
            .withXmlResource("process.bpmn", process)
            .withXmlResource("process2.bpmn", process2)
            .deploy()
            .getValue();

    // then
    final var processKeyList =
        deployment.getDeployedProcesses().stream()
            .map(DeployedProcess::getProcessKey)
            .collect(Collectors.toList());

    final var processRecordKeys =
        RecordingExporter.processRecords()
            .limit(2)
            .map(Record::getKey)
            .collect(Collectors.toList());
    assertThat(processKeyList).hasSameElementsAs(processRecordKeys);

    final var firstProcessRecord =
        RecordingExporter.processRecords().withBpmnProcessId(processId).getFirst();
    assertThat(firstProcessRecord).isNotNull();
    assertThat(firstProcessRecord.getValue().getResourceName()).isEqualTo("process.bpmn");
    assertThat(firstProcessRecord.getValue().getVersion()).isEqualTo(1);
    assertThat(firstProcessRecord.getKey())
        .isEqualTo(firstProcessRecord.getValue().getProcessKey());

    final var secondProcessRecord =
        RecordingExporter.processRecords().withBpmnProcessId(processId2).getFirst();
    assertThat(secondProcessRecord).isNotNull();
    assertThat(secondProcessRecord.getValue().getResourceName()).isEqualTo("process2.bpmn");
    assertThat(secondProcessRecord.getValue().getVersion()).isEqualTo(1);
    assertThat(secondProcessRecord.getKey())
        .isEqualTo(secondProcessRecord.getValue().getProcessKey());
  }

  @Test
  public void shouldCreateDeploymentIfUnusedInvalidMessage() {
    // given
    final BpmnModelInstance process = Bpmn.createExecutableProcess().startEvent().done();
    process.getDefinitions().addChildElement(process.newInstance(Message.class));

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource(process).deploy();

    // then
    assertThat(deployment.getIntent()).isEqualTo(DeploymentIntent.CREATED);
  }

  @Test
  public void shouldCreateDeploymentWithMessageStartEvent() {
    // given
    final ProcessBuilder processBuilder = Bpmn.createExecutableProcess();
    final BpmnModelInstance process =
        processBuilder.startEvent().message(m -> m.name("startMessage")).endEvent().done();

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource(process).deploy();

    // then
    assertThat(deployment.getIntent()).isEqualTo(DeploymentIntent.CREATED);
  }

  @Test
  public void shouldCreateDeploymentWithMultipleMessageStartEvent() {
    // given
    final ProcessBuilder processBuilder =
        Bpmn.createExecutableProcess("processWithMultipleMsgStartEvent");
    processBuilder.startEvent().message(m -> m.name("startMessage1")).endEvent().done();
    final BpmnModelInstance process =
        processBuilder.startEvent().message(m -> m.name("startMessage2")).endEvent().done();

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource(process).deploy();

    // then
    assertThat(deployment.getIntent()).isEqualTo(DeploymentIntent.CREATED);
  }

  @Test
  public void shouldRejectDeploymentIfUsedInvalidMessage() {
    // given
    final BpmnModelInstance process =
        Bpmn.createExecutableProcess().startEvent().intermediateCatchEvent("invalidMessage").done();

    // when
    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE.deployment().withXmlResource(process).expectRejection().deploy();

    // then
    Assertions.assertThat(rejectedDeployment.getRecordType())
        .isEqualTo(RecordType.COMMAND_REJECTION);
  }

  @Test
  public void shouldRejectDeploymentIfNotValidDesignTimeAspect() throws Exception {
    // given
    final Path path = Paths.get(getClass().getResource("/processes/invalid_process.bpmn").toURI());
    final byte[] resource = Files.readAllBytes(path);

    // when
    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE.deployment().withXmlResource(resource).expectRejection().deploy();

    // then
    Assertions.assertThat(rejectedDeployment)
        .hasKey(ExecuteCommandResponseDecoder.keyNullValue())
        .hasRecordType(RecordType.COMMAND_REJECTION)
        .hasIntent(DeploymentIntent.CREATE)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT);
    assertThat(rejectedDeployment.getRejectionReason())
        .contains("ERROR: Must have at least one start event");
  }

  @Test
  public void shouldRejectDeploymentIfNotValidRuntimeAspect() throws Exception {
    // given
    final Path path =
        Paths.get(getClass().getResource("/processes/invalid_process_condition.bpmn").toURI());
    final byte[] resource = Files.readAllBytes(path);

    // when
    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE.deployment().withXmlResource(resource).expectRejection().deploy();

    // then
    Assertions.assertThat(rejectedDeployment)
        .hasKey(ExecuteCommandResponseDecoder.keyNullValue())
        .hasRecordType(RecordType.COMMAND_REJECTION)
        .hasIntent(DeploymentIntent.CREATE)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT);
    assertThat(rejectedDeployment.getRejectionReason())
        .contains("Element: flow2 > conditionExpression")
        .contains("ERROR: failed to parse expression");
  }

  @Test
  public void shouldRejectDeploymentIfOneResourceIsNotValid() throws Exception {
    // given
    final Path path1 = Paths.get(getClass().getResource("/processes/invalid_process.bpmn").toURI());
    final Path path2 = Paths.get(getClass().getResource("/processes/collaboration.bpmn").toURI());
    final byte[] resource1 = Files.readAllBytes(path1);
    final byte[] resource2 = Files.readAllBytes(path2);

    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE
            .deployment()
            .withXmlResource(resource1)
            .withXmlResource(resource2)
            .expectRejection()
            .deploy();

    // then
    Assertions.assertThat(rejectedDeployment)
        .hasKey(ExecuteCommandResponseDecoder.keyNullValue())
        .hasRecordType(RecordType.COMMAND_REJECTION)
        .hasIntent(DeploymentIntent.CREATE)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT);
  }

  @Test
  public void shouldRejectDeploymentIfNoResources() {
    // when
    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE.deployment().expectRejection().deploy();

    // then
    Assertions.assertThat(rejectedDeployment)
        .hasKey(ExecuteCommandResponseDecoder.keyNullValue())
        .hasRecordType(RecordType.COMMAND_REJECTION)
        .hasIntent(DeploymentIntent.CREATE)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT);
  }

  @Test
  public void shouldRejectDeploymentIfNotParsable() {
    // when
    final Record<DeploymentRecordValue> rejectedDeployment =
        ENGINE
            .deployment()
            .withXmlResource("not a process".getBytes(UTF_8))
            .expectRejection()
            .deploy();

    // then
    Assertions.assertThat(rejectedDeployment)
        .hasKey(ExecuteCommandResponseDecoder.keyNullValue())
        .hasRecordType(RecordType.COMMAND_REJECTION)
        .hasIntent(DeploymentIntent.CREATE)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT);
  }

  @Test
  public void shouldIncrementProcessVersions() {
    // given
    final BpmnModelInstance modelInstance =
        Bpmn.createExecutableProcess("shouldIncrementProcessVersions")
            .startEvent()
            .endEvent()
            .done();

    // when
    final Record<DeploymentRecordValue> deployment =
        ENGINE.deployment().withXmlResource("process1", modelInstance).deploy();
    final Record<DeploymentRecordValue> deployment2 =
        ENGINE.deployment().withXmlResource("process2", modelInstance).deploy();

    // then
    assertThat(deployment.getValue().getDeployedProcesses().get(0).getVersion()).isEqualTo(1L);
    assertThat(deployment2.getValue().getDeployedProcesses().get(0).getVersion()).isEqualTo(2L);
  }

  @Test
  public void shouldFilterDuplicateProcess() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE.deployment().withXmlResource("process.bpmn", process).deploy();

    // when
    final Record<DeploymentRecordValue> repeated =
        ENGINE.deployment().withXmlResource("process.bpmn", process).deploy();

    // then
    assertThat(repeated.getKey()).isGreaterThan(original.getKey());

    final List<DeployedProcess> originalProcesses = original.getValue().getDeployedProcesses();
    final List<DeployedProcess> repeatedProcesses = repeated.getValue().getDeployedProcesses();
    assertThat(repeatedProcesses.size()).isEqualTo(originalProcesses.size()).isOne();

    assertSameResource(originalProcesses.get(0), repeatedProcesses.get(0));
  }

  @Test
  public void shouldNotFilterWithDifferentResourceName() {
    // given
    final String originalResourceName = "process-1.bpmn";
    final Record<DeploymentRecordValue> original =
        ENGINE.deployment().withXmlResource(originalResourceName, process).deploy();

    // when
    final String repeatedResourceName = "process-2.bpmn";
    final Record<DeploymentRecordValue> repeated =
        ENGINE.deployment().withXmlResource(repeatedResourceName, process).deploy();

    // then
    final List<DeployedProcess> originalProcesses = original.getValue().getDeployedProcesses();
    final List<DeployedProcess> repeatedProcesses = repeated.getValue().getDeployedProcesses();
    assertThat(repeatedProcesses.size()).isEqualTo(originalProcesses.size()).isOne();

    assertDifferentResources(originalProcesses.get(0), repeatedProcesses.get(0));
    assertThat(originalProcesses.get(0).getResourceName()).isEqualTo(originalResourceName);
    assertThat(repeatedProcesses.get(0).getResourceName()).isEqualTo(repeatedResourceName);
  }

  @Test
  public void shouldNotFilterWithDifferentResource() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE.deployment().withXmlResource("process.bpmn", process).deploy();

    // when
    final Record<DeploymentRecordValue> repeated =
        ENGINE.deployment().withXmlResource("process.bpmn", process_V2).deploy();

    // then
    final List<DeployedProcess> originalProcesses = original.getValue().getDeployedProcesses();
    final List<DeployedProcess> repeatedProcesses = repeated.getValue().getDeployedProcesses();
    assertThat(repeatedProcesses.size()).isEqualTo(originalProcesses.size()).isOne();

    assertDifferentResources(originalProcesses.get(0), repeatedProcesses.get(0));
  }

  @Test
  public void shouldFilterWithTwoEqualResources() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE
            .deployment()
            .withXmlResource("p1.bpmn", process)
            .withXmlResource("p2.bpmn", process2)
            .deploy();

    // when
    final Record<DeploymentRecordValue> repeated =
        ENGINE
            .deployment()
            .withXmlResource("p1.bpmn", process)
            .withXmlResource("p2.bpmn", process2)
            .deploy();

    // then
    final List<DeployedProcess> originalProcesses = original.getValue().getDeployedProcesses();
    final List<DeployedProcess> repeatedProcesses = repeated.getValue().getDeployedProcesses();
    assertThat(repeatedProcesses.size()).isEqualTo(originalProcesses.size()).isEqualTo(2);

    for (final DeployedProcess process : originalProcesses) {
      assertSameResource(process, findProcess(repeatedProcesses, process.getBpmnProcessId()));
    }
  }

  @Test
  public void shouldFilterWithOneDifferentAndOneEqual() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE
            .deployment()
            .withXmlResource("p1.bpmn", process)
            .withXmlResource("p2.bpmn", process2)
            .deploy();

    // when
    final Record<DeploymentRecordValue> repeated =
        ENGINE
            .deployment()
            .withXmlResource("p1.bpmn", process)
            .withXmlResource("p2.bpmn", process2_V2)
            .deploy();

    // then
    final List<DeployedProcess> originalProcesses = original.getValue().getDeployedProcesses();
    final List<DeployedProcess> repeatedProcesses = repeated.getValue().getDeployedProcesses();
    assertThat(repeatedProcesses.size()).isEqualTo(originalProcesses.size()).isEqualTo(2);

    assertSameResource(
        findProcess(originalProcesses, processId), findProcess(repeatedProcesses, processId));
    assertDifferentResources(
        findProcess(originalProcesses, processId2), findProcess(repeatedProcesses, processId2));
  }

  @Test
  public void shouldNotFilterWithRollbackToPreviousVersion() {
    // given
    final Record<DeploymentRecordValue> original =
        ENGINE.deployment().withXmlResource("p1.bpmn", process).deploy();
    ENGINE.deployment().withXmlResource("p1.bpmn", process_V2).deploy();

    // when
    final Record<DeploymentRecordValue> rollback =
        ENGINE.deployment().withXmlResource("p1.bpmn", process).deploy();

    // then
    final List<DeployedProcess> originalProcesses = original.getValue().getDeployedProcesses();
    final List<DeployedProcess> repeatedProcesses = rollback.getValue().getDeployedProcesses();
    assertThat(repeatedProcesses.size()).isEqualTo(originalProcesses.size()).isOne();

    assertDifferentResources(
        findProcess(originalProcesses, processId), findProcess(repeatedProcesses, processId));
  }

  @Test
  public void shouldRejectDeploymentWithDuplicateResources() {
    // given
    final BpmnModelInstance definition1 =
        Bpmn.createExecutableProcess("process1").startEvent().done();
    final BpmnModelInstance definition2 =
        Bpmn.createExecutableProcess("process2").startEvent().done();
    final BpmnModelInstance definition3 =
        Bpmn.createExecutableProcess("process2")
            .startEvent()
            .serviceTask("task", (t) -> t.zeebeJobType("j").zeebeTaskHeader("k", "v"))
            .done();

    // when
    final Record<DeploymentRecordValue> deploymentRejection =
        ENGINE
            .deployment()
            .withXmlResource("p1.bpmn", definition1)
            .withXmlResource("p2.bpmn", definition2)
            .withXmlResource("p3.bpmn", definition3)
            .expectRejection()
            .deploy();

    // then
    Assertions.assertThat(deploymentRejection)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT)
        .hasRejectionReason(
            "Expected to deploy new resources, but encountered the following errors:\n"
                + "Duplicated process id in resources 'p2.bpmn' and 'p3.bpmn'");
  }

  @Test
  public void shouldRejectDeploymentWithInvalidTimerStartEventExpression() {
    // given
    final BpmnModelInstance definition =
        Bpmn.createExecutableProcess("process1")
            .startEvent("start-event-1")
            .timerWithCycleExpression("INVALID_CYCLE_EXPRESSION")
            .done();

    // when
    final Record<DeploymentRecordValue> deploymentRejection =
        ENGINE.deployment().withXmlResource("p1.bpmn", definition).expectRejection().deploy();

    // then
    Assertions.assertThat(deploymentRejection)
        .hasRejectionType(RejectionType.INVALID_ARGUMENT)
        .hasRejectionReason(
            "Expected to deploy new resources, but encountered the following errors:\n"
                + "'p1.bpmn': - Element: start-event-1\n"
                + "    - ERROR: Invalid timer cycle expression ("
                + "failed to evaluate expression "
                + "'INVALID_CYCLE_EXPRESSION': no variable found for name "
                + "'INVALID_CYCLE_EXPRESSION')\n");
  }

  private DeployedProcess findProcess(
      final List<DeployedProcess> processes, final String processId) {
    return processes.stream()
        .filter(w -> w.getBpmnProcessId().equals(processId))
        .findFirst()
        .orElse(null);
  }

  private void assertSameResource(
      final DeployedProcess original, final DeployedProcess repeated) {
    io.zeebe.protocol.record.Assertions.assertThat(repeated)
        .hasVersion(original.getVersion())
        .hasProcessKey(original.getProcessKey())
        .hasResourceName(original.getResourceName())
        .hasBpmnProcessId(original.getBpmnProcessId());
  }

  private void assertDifferentResources(
      final DeployedProcess original, final DeployedProcess repeated) {
    assertThat(original.getProcessKey()).isLessThan(repeated.getProcessKey());
    assertThat(original.getVersion()).isLessThan(repeated.getVersion());
  }
}
