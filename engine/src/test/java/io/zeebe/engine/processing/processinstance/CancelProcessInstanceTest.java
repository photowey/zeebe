/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.processinstance;

import static io.zeebe.protocol.record.intent.ProcessInstanceIntent.CANCEL;
import static io.zeebe.protocol.record.intent.ProcessInstanceIntent.ELEMENT_ACTIVATED;
import static io.zeebe.protocol.record.intent.ProcessInstanceIntent.ELEMENT_TERMINATED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.builder.AbstractFlowNodeBuilder;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.protocol.record.value.JobRecordValue;
import io.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.List;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class CancelProcessInstanceTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();
  private static final BpmnModelInstance PROCESS =
      Bpmn.createExecutableProcess("PROCESS")
          .startEvent()
          .serviceTask("task", t -> t.zeebeJobType("test").zeebeJobRetries("5"))
          .endEvent()
          .done();
  private static final BpmnModelInstance SUB_PROCESS_PROCESS =
      Bpmn.createExecutableProcess("SUB_PROCESS_PROCESS")
          .startEvent()
          .subProcess("subProcess")
          .embeddedSubProcess()
          .startEvent()
          .serviceTask("task", t -> t.zeebeJobType("test").zeebeJobRetries("5"))
          .endEvent()
          .subProcessDone()
          .endEvent()
          .done();
  private static final BpmnModelInstance FORK_PROCESS;

  static {
    final AbstractFlowNodeBuilder<?, ?> builder =
        Bpmn.createExecutableProcess("FORK_PROCESS")
            .startEvent("start")
            .parallelGateway("fork")
            .serviceTask("task1", b -> b.zeebeJobType("type1"))
            .endEvent("end1")
            .moveToNode("fork");

    FORK_PROCESS =
        builder.serviceTask("task2", b -> b.zeebeJobType("type2")).endEvent("end2").done();
  }

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @BeforeClass
  public static void init() {
    ENGINE.deployment().withXmlResource(PROCESS).deploy();
    ENGINE.deployment().withXmlResource(SUB_PROCESS_PROCESS).deploy();
    ENGINE.deployment().withXmlResource(FORK_PROCESS).deploy();
  }

  @Test
  public void shouldCancelProcessInstance() {
    // given
    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId("PROCESS").create();
    RecordingExporter.processInstanceRecords()
        .withProcessInstanceKey(processInstanceKey)
        .withElementId("task")
        .withIntent(ELEMENT_ACTIVATED)
        .getFirst();

    // when
    ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // then
    final Record<ProcessInstanceRecordValue> processInstanceCanceledEvent =
        RecordingExporter.processInstanceRecords()
            .withRecordKey(processInstanceKey)
            .withProcessInstanceKey(processInstanceKey)
            .withIntent(ELEMENT_TERMINATED)
            .getFirst();

    Assertions.assertThat(processInstanceCanceledEvent.getValue())
        .hasBpmnProcessId("PROCESS")
        .hasVersion(1)
        .hasProcessInstanceKey(processInstanceKey)
        .hasElementId("PROCESS");

    final List<Record<ProcessInstanceRecordValue>> processEvents =
        RecordingExporter.processInstanceRecords()
            .withProcessInstanceKey(processInstanceKey)
            .skipUntil(r -> r.getIntent() == CANCEL)
            .limit(r -> r.getKey() == processInstanceKey && r.getIntent() == ELEMENT_TERMINATED)
            .asList();

    assertThat(processEvents)
        .hasSize(5)
        .extracting(e -> e.getValue().getElementId(), e -> e.getIntent())
        .containsSequence(
            tuple("", CANCEL),
            tuple("PROCESS", ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple("task", ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple("task", ELEMENT_TERMINATED),
            tuple("PROCESS", ELEMENT_TERMINATED));
  }

  @Test
  public void shouldNotCancelElementInstance() {
    // given
    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId("PROCESS").create();
    final Record<ProcessInstanceRecordValue> task =
        RecordingExporter.processInstanceRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withElementId("task")
            .withIntent(ELEMENT_ACTIVATED)
            .getFirst();

    // when
    final Record<ProcessInstanceRecordValue> rejectedCancel =
        ENGINE
            .processInstance()
            .withInstanceKey(task.getKey())
            .onPartition(1)
            .expectRejection()
            .cancel();

    // then
    assertThat(rejectedCancel.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejectedCancel.getRejectionReason())
        .isEqualTo(
            "Expected to cancel a process instance with key '"
                + task.getKey()
                + "', but no such process was found");
  }

  @Test
  public void shouldCancelProcessInstanceWithEmbeddedSubProcess() {
    // given
    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId("SUB_PROCESS_PROCESS").create();

    RecordingExporter.processInstanceRecords()
        .withProcessInstanceKey(processInstanceKey)
        .withElementId("task")
        .withIntent(ELEMENT_ACTIVATED)
        .getFirst();

    // when
    ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // then
    final List<Record<ProcessInstanceRecordValue>> processEvents =
        RecordingExporter.processInstanceRecords()
            .withProcessInstanceKey(processInstanceKey)
            .skipUntil(r -> r.getIntent() == ProcessInstanceIntent.CANCEL)
            .limitToProcessInstanceTerminated()
            .asList();

    assertThat(processEvents)
        .hasSize(7)
        .extracting(e -> e.getValue().getElementId(), e -> e.getIntent())
        .containsSequence(
            tuple("", ProcessInstanceIntent.CANCEL),
            tuple("SUB_PROCESS_PROCESS", ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple("subProcess", ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple("task", ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple("task", ELEMENT_TERMINATED),
            tuple("subProcess", ELEMENT_TERMINATED),
            tuple("SUB_PROCESS_PROCESS", ELEMENT_TERMINATED));
  }

  @Test
  public void shouldCancelActivityInstance() {
    // given
    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId("PROCESS").create();
    final Record<ProcessInstanceRecordValue> activityActivatedEvent =
        RecordingExporter.processInstanceRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withElementId("task")
            .withIntent(ELEMENT_ACTIVATED)
            .getFirst();

    // when
    ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // then
    final Record<ProcessInstanceRecordValue> activityTerminatedEvent =
        RecordingExporter.processInstanceRecords()
            .withElementId("task")
            .withIntent(ELEMENT_TERMINATED)
            .getFirst();

    assertThat(activityTerminatedEvent.getKey()).isEqualTo(activityActivatedEvent.getKey());

    Assertions.assertThat(activityActivatedEvent.getValue())
        .hasBpmnProcessId("PROCESS")
        .hasVersion(1)
        .hasProcessInstanceKey(processInstanceKey)
        .hasElementId("task");
  }

  @Test
  public void shouldCancelProcessInstanceWithParallelExecution() {
    // given
    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId("FORK_PROCESS").create();

    RecordingExporter.processInstanceRecords()
        .withProcessInstanceKey(processInstanceKey)
        .withElementType(BpmnElementType.SERVICE_TASK)
        .withIntent(ELEMENT_ACTIVATED)
        .limit(2)
        .asList();

    // when
    ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // then
    final List<Record<ProcessInstanceRecordValue>> terminatedElements =
        RecordingExporter.processInstanceRecords()
            .withProcessInstanceKey(processInstanceKey)
            .skipUntil(r -> r.getIntent() == ProcessInstanceIntent.CANCEL)
            .limitToProcessInstanceTerminated()
            .filter(r -> r.getIntent() == ELEMENT_TERMINATED)
            .asList();

    assertThat(terminatedElements).hasSize(3);
    assertThat(terminatedElements)
        .extracting(Record::getValue)
        .extracting(ProcessInstanceRecordValue::getElementId)
        .containsSubsequence("task1", "FORK_PROCESS")
        .containsSubsequence("task2", "FORK_PROCESS")
        .contains("task1", "task2", "FORK_PROCESS");
  }

  @Test
  public void shouldCancelIntermediateCatchEvent() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess("shouldCancelIntermediateCatchEvent")
                .startEvent()
                .intermediateCatchEvent("catch-event")
                .message(b -> b.name("msg").zeebeCorrelationKeyExpression("id"))
                .done())
        .deploy();

    final long processInstanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId("shouldCancelIntermediateCatchEvent")
            .withVariable("id", "123")
            .create();

    RecordingExporter.processInstanceRecords()
        .withProcessInstanceKey(processInstanceKey)
        .withElementId("catch-event")
        .withIntent(ELEMENT_ACTIVATED)
        .getFirst();

    // when
    ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // then
    final Record<ProcessInstanceRecordValue> terminatedEvent =
        RecordingExporter.processInstanceRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withElementId("shouldCancelIntermediateCatchEvent")
            .withIntent(ELEMENT_TERMINATED)
            .getFirst();

    Assertions.assertThat(terminatedEvent.getValue())
        .hasBpmnProcessId("shouldCancelIntermediateCatchEvent")
        .hasVersion(1)
        .hasProcessInstanceKey(processInstanceKey)
        .hasElementId("shouldCancelIntermediateCatchEvent");
  }

  @Test
  public void shouldCancelJobForActivity() {
    // given
    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId("PROCESS").create();
    final Record<JobRecordValue> jobCreatedEvent =
        RecordingExporter.jobRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withIntent(JobIntent.CREATED)
            .getFirst();

    // when
    ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // then
    final Record<ProcessInstanceRecordValue> terminateActivity =
        RecordingExporter.processInstanceRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withElementId("task")
            .withIntent(ProcessInstanceIntent.ELEMENT_TERMINATING)
            .getFirst();

    final Record<JobRecordValue> jobCancelCmd =
        RecordingExporter.jobRecords()
            .withProcessInstanceKey(processInstanceKey)
            .onlyCommands()
            .withIntent(JobIntent.CANCEL)
            .getFirst();
    final Record<JobRecordValue> jobCanceledEvent =
        RecordingExporter.jobRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withIntent(JobIntent.CANCELED)
            .getFirst();

    assertThat(jobCanceledEvent.getKey()).isEqualTo(jobCreatedEvent.getKey());
    assertThat(jobCancelCmd.getSourceRecordPosition()).isEqualTo(terminateActivity.getPosition());
    assertThat(jobCanceledEvent.getSourceRecordPosition()).isEqualTo(jobCancelCmd.getPosition());

    final JobRecordValue jobCanceledEventValue = jobCanceledEvent.getValue();
    assertThat(jobCanceledEventValue.getProcessInstanceKey()).isEqualTo(processInstanceKey);

    Assertions.assertThat(jobCanceledEventValue)
        .hasElementId("task")
        .hasProcessDefinitionVersion(1)
        .hasBpmnProcessId("PROCESS");
  }

  @Test
  public void shouldRejectCancelNonExistingProcessInstance() {
    // when
    final Record<ProcessInstanceRecordValue> rejectedCancel =
        ENGINE.processInstance().withInstanceKey(-1).onPartition(1).expectRejection().cancel();

    // then
    assertThat(rejectedCancel.getRecordType()).isEqualTo(RecordType.COMMAND_REJECTION);
    assertThat(rejectedCancel.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejectedCancel.getRejectionReason())
        .isEqualTo(
            "Expected to cancel a process instance with key '-1', but no such process was found");

    assertThat(
            RecordingExporter.processInstanceRecords()
                .withPosition(rejectedCancel.getSourceRecordPosition())
                .withIntent(CANCEL)
                .exists())
        .isTrue();
  }

  @Test
  public void shouldRejectCancelCompletedProcessInstance() {
    // given
    ENGINE
        .deployment()
        .withXmlResource(
            Bpmn.createExecutableProcess("shouldRejectCancelCompletedProcessInstance")
                .startEvent()
                .endEvent()
                .done())
        .deploy();

    final long processInstanceKey =
        ENGINE
            .processInstance()
            .ofBpmnProcessId("shouldRejectCancelCompletedProcessInstance")
            .create();

    RecordingExporter.processInstanceRecords()
        .withElementId("shouldRejectCancelCompletedProcessInstance")
        .withIntent(ProcessInstanceIntent.ELEMENT_COMPLETED)
        .getFirst();

    // when
    final Record<ProcessInstanceRecordValue> rejectedCancel =
        ENGINE.processInstance().withInstanceKey(processInstanceKey).expectRejection().cancel();

    // then
    assertThat(rejectedCancel.getRecordType()).isEqualTo(RecordType.COMMAND_REJECTION);
    assertThat(rejectedCancel.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejectedCancel.getRejectionReason())
        .isEqualTo(
            "Expected to cancel a process instance with key '"
                + processInstanceKey
                + "', but no such process was found");

    assertThat(
            RecordingExporter.processInstanceRecords()
                .withPosition(rejectedCancel.getSourceRecordPosition())
                .withIntent(CANCEL)
                .exists())
        .isTrue();
  }

  @Test
  public void shouldRejectCancelAlreadyCanceledProcessInstance() {
    // given
    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId("PROCESS").create();
    ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // when
    final Record<ProcessInstanceRecordValue> rejectedCancel =
        ENGINE.processInstance().withInstanceKey(processInstanceKey).expectRejection().cancel();

    // then
    assertThat(rejectedCancel.getRecordType()).isEqualTo(RecordType.COMMAND_REJECTION);
    assertThat(rejectedCancel.getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejectedCancel.getRejectionReason())
        .isEqualTo(
            "Expected to cancel a process instance with key '"
                + processInstanceKey
                + "', but no such process was found");
  }

  @Test
  public void shouldWriteEntireEventOnCancel() {
    // given
    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId("PROCESS").create();
    final Record<ProcessInstanceRecordValue> activatedEvent =
        RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .withElementId("PROCESS")
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    // when
    final Record<ProcessInstanceRecordValue> canceledRecord =
        ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // then
    assertThat(canceledRecord.getValue()).isEqualTo(activatedEvent.getValue());
  }
}
