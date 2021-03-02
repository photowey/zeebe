/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.bpmn.subprocess;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.model.bpmn.builder.EmbeddedSubProcessBuilder;
import io.zeebe.model.bpmn.builder.SubProcessBuilder;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.protocol.record.value.JobRecordValue;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.function.Consumer;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class EmbeddedSubProcessTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();

  private static final String PROCESS_ID = "process-with-sub-process";

  private static final BpmnModelInstance NO_TASK_SUB_PROCESS =
      processWithSubProcess(subProcess -> subProcess.startEvent().endEvent());

  private static final BpmnModelInstance ONE_TASK_SUB_PROCESS =
      processWithSubProcess(
          subProcess ->
              subProcess.startEvent().serviceTask("task", b -> b.zeebeJobType("task")).endEvent());

  private static final BpmnModelInstance PARALLEL_TASKS_SUB_PROCESS =
      processWithSubProcess(
          subProcess ->
              subProcess
                  .startEvent()
                  .parallelGateway("fork")
                  .serviceTask("task-1", b -> b.zeebeJobType("task-1"))
                  .sequenceFlowId("join-1")
                  .parallelGateway("join")
                  .moveToNode("fork")
                  .serviceTask("task-2", b -> b.zeebeJobType("task-2"))
                  .sequenceFlowId("join-2")
                  .connectTo("join")
                  .endEvent());

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  private static BpmnModelInstance processWithSubProcess(
      final Consumer<EmbeddedSubProcessBuilder> subProcessBuilder) {
    return Bpmn.createExecutableProcess(PROCESS_ID)
        .startEvent()
        .subProcess(
            "sub-process", subProcess -> subProcessBuilder.accept(subProcess.embeddedSubProcess()))
        .endEvent()
        .done();
  }

  @Test
  public void shouldActivateSubProcess() {
    // given
    ENGINE.deployment().withXmlResource(NO_TASK_SUB_PROCESS).deploy();

    // when
    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).create();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceCompleted())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSequence(
            tuple(BpmnElementType.SEQUENCE_FLOW, ProcessInstanceIntent.SEQUENCE_FLOW_TAKEN),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            tuple(BpmnElementType.START_EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            tuple(BpmnElementType.START_EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATED));

    final var subProcessActivating =
        RecordingExporter.processInstanceRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withElementType(BpmnElementType.SUB_PROCESS)
            .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATING)
            .getFirst();

    Assertions.assertThat(subProcessActivating.getValue())
        .hasFlowScopeKey(processInstanceKey)
        .hasElementId("sub-process");
  }

  @Test
  public void shouldCompleteSubProcess() {
    // given
    ENGINE.deployment().withXmlResource(NO_TASK_SUB_PROCESS).deploy();

    // when
    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).create();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceCompleted())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSequence(
            tuple(BpmnElementType.END_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETING),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.SEQUENCE_FLOW, ProcessInstanceIntent.SEQUENCE_FLOW_TAKEN),
            tuple(BpmnElementType.END_EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATING));

    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .withElementType(BpmnElementType.PROCESS)
                .limitToProcessInstanceCompleted())
        .extracting(Record::getIntent)
        .contains(ProcessInstanceIntent.ELEMENT_COMPLETED);
  }

  @Test
  public void shouldCreateJobForInnerTask() {
    // given
    ENGINE.deployment().withXmlResource(ONE_TASK_SUB_PROCESS).deploy();

    // when
    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).create();

    // then
    final var serviceTaskActivated =
        RecordingExporter.processInstanceRecords(ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .withProcessInstanceKey(processInstanceKey)
            .withElementType(BpmnElementType.SERVICE_TASK)
            .getFirst();

    final Record<JobRecordValue> jobCreated =
        RecordingExporter.jobRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withIntent(JobIntent.CREATED)
            .getFirst();

    Assertions.assertThat(jobCreated.getValue())
        .hasElementId("task")
        .hasElementInstanceKey(serviceTaskActivated.getKey())
        .hasBpmnProcessId(serviceTaskActivated.getValue().getBpmnProcessId())
        .hasProcessDefinitionVersion(serviceTaskActivated.getValue().getVersion())
        .hasProcessKey(serviceTaskActivated.getValue().getProcessKey());
  }

  @Test
  public void shouldTerminateSubProcess() {
    // given
    ENGINE.deployment().withXmlResource(ONE_TASK_SUB_PROCESS).deploy();

    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).create();

    RecordingExporter.processInstanceRecords()
        .withProcessInstanceKey(processInstanceKey)
        .withElementType(BpmnElementType.SERVICE_TASK)
        .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
        .await();

    // when
    ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceTerminated())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSequence(
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_TERMINATED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATED),
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATED));
  }

  @Test
  public void shouldInterruptSubProcess() {
    // given
    final var process =
        processWithSubProcess(
            subProcess ->
                subProcess
                    .startEvent()
                    .serviceTask("task", t -> t.zeebeJobType("task"))
                    .endEvent()
                    .subProcessDone()
                    .boundaryEvent(
                        "cancel",
                        b -> b.message(m -> m.name("cancel").zeebeCorrelationKeyExpression("key")))
                    .endEvent());

    ENGINE.deployment().withXmlResource(process).deploy();

    final long processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).withVariable("key", "key-1").create();

    RecordingExporter.processInstanceRecords()
        .withProcessInstanceKey(processInstanceKey)
        .withElementType(BpmnElementType.SERVICE_TASK)
        .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
        .await();

    // when
    ENGINE.message().withName("cancel").withCorrelationKey("key-1").publish();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceCompleted())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSequence(
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.EVENT_OCCURRED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_TERMINATED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATED),
            tuple(BpmnElementType.BOUNDARY_EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATING),
            tuple(BpmnElementType.BOUNDARY_EVENT, ProcessInstanceIntent.ELEMENT_ACTIVATED));

    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .withElementType(BpmnElementType.PROCESS)
                .limitToProcessInstanceCompleted())
        .extracting(Record::getIntent)
        .contains(ProcessInstanceIntent.ELEMENT_COMPLETED);
  }

  @Test
  public void shouldCompleteNestedSubProcess() {
    // given
    final Consumer<SubProcessBuilder> nestedSubProcess =
        subProcess -> subProcess.embeddedSubProcess().startEvent().endEvent();

    final BpmnModelInstance process =
        processWithSubProcess(
            subProcess ->
                subProcess
                    .startEvent()
                    .subProcess("nestedSubProcess", nestedSubProcess)
                    .endEvent());

    ENGINE.deployment().withXmlResource(process).deploy();

    // when
    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).create();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceCompleted())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSubsequence(
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_ACTIVATED),
            tuple(BpmnElementType.END_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.END_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.END_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED));
  }

  @Test
  public void shouldCompleteSubProcessWithParallelFlow() {
    // given
    final var process =
        processWithSubProcess(
            subProcess ->
                subProcess
                    .startEvent()
                    .parallelGateway("fork")
                    .serviceTask("task-1", b -> b.zeebeJobType("task-1"))
                    .endEvent()
                    .moveToLastGateway()
                    .serviceTask("task-2", b -> b.zeebeJobType("task-2"))
                    .endEvent());

    ENGINE.deployment().withXmlResource(process).deploy();

    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).create();

    // when
    ENGINE.job().ofInstance(processInstanceKey).withType("task-1").complete();

    RecordingExporter.processInstanceRecords()
        .withProcessInstanceKey(processInstanceKey)
        .withElementType(BpmnElementType.END_EVENT)
        .withIntent(ProcessInstanceIntent.ELEMENT_COMPLETED)
        .await();

    ENGINE.job().ofInstance(processInstanceKey).withType("task-2").complete();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceCompleted())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSubsequence(
            tuple(BpmnElementType.PARALLEL_GATEWAY, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.END_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.END_EVENT, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETING),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED),
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_COMPLETED));
  }

  @Test
  public void shouldTerminateSubProcessWithParallelFlow() {
    // given
    ENGINE.deployment().withXmlResource(PARALLEL_TASKS_SUB_PROCESS).deploy();

    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).create();

    RecordingExporter.jobRecords(JobIntent.CREATED)
        .withProcessInstanceKey(processInstanceKey)
        .limit(2)
        .await();

    // when
    ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceTerminated())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSequence(
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_TERMINATED),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_TERMINATED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATED),
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATED));
  }

  @Test
  public void shouldTerminateSubProcessWithPendingParallelGateway() {
    // given
    ENGINE.deployment().withXmlResource(PARALLEL_TASKS_SUB_PROCESS).deploy();

    final long processInstanceKey = ENGINE.processInstance().ofBpmnProcessId(PROCESS_ID).create();

    ENGINE.job().ofInstance(processInstanceKey).withType("task-1").complete();

    // await that one sequence flow on the joining parallel gateway is taken
    RecordingExporter.processInstanceRecords()
        .withProcessInstanceKey(processInstanceKey)
        .withElementId("join-1")
        .withIntent(ProcessInstanceIntent.SEQUENCE_FLOW_TAKEN)
        .await();

    // when
    ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // then
    assertThat(
            RecordingExporter.processInstanceRecords()
                .withProcessInstanceKey(processInstanceKey)
                .limitToProcessInstanceTerminated())
        .extracting(r -> tuple(r.getValue().getBpmnElementType(), r.getIntent()))
        .containsSequence(
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_TERMINATING),
            tuple(BpmnElementType.SERVICE_TASK, ProcessInstanceIntent.ELEMENT_TERMINATED),
            tuple(BpmnElementType.SUB_PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATED),
            tuple(BpmnElementType.PROCESS, ProcessInstanceIntent.ELEMENT_TERMINATED));
  }
}
