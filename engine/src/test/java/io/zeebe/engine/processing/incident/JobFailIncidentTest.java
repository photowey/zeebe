/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.incident;

import static io.zeebe.protocol.record.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.engine.util.client.JobClient;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.IncidentIntent;
import io.zeebe.protocol.record.intent.JobBatchIntent;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.zeebe.protocol.record.value.ErrorType;
import io.zeebe.protocol.record.value.IncidentRecordValue;
import io.zeebe.protocol.record.value.JobRecordValue;
import io.zeebe.protocol.record.value.ProcessInstanceRecordValue;
import io.zeebe.test.util.collection.Maps;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class JobFailIncidentTest {

  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();
  private static final String JOB_TYPE = "test";
  private static final BpmnModelInstance PROCESS_INPUT_MAPPING =
      Bpmn.createExecutableProcess("process")
          .startEvent()
          .serviceTask(
              "failingTask", t -> t.zeebeJobType(JOB_TYPE).zeebeInputExpression("foo", "foo"))
          .done();
  private static final Map<String, Object> VARIABLES = Maps.of(entry("foo", "bar"));
  private static long processKey;

  @Rule
  public RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  private long processInstanceKey;

  @BeforeClass
  public static void init() {
    processKey =
        ENGINE
            .deployment()
            .withXmlResource(PROCESS_INPUT_MAPPING)
            .deploy()
            .getValue()
            .getDeployedProcesses()
            .get(0)
            .getProcessKey();
  }

  @Before
  public void beforeTest() {
    processInstanceKey =
        ENGINE.processInstance().ofBpmnProcessId("process").withVariables(VARIABLES).create();

    RecordingExporter.jobRecords()
        .withProcessInstanceKey(processInstanceKey)
        .withIntent(JobIntent.CREATED)
        .getFirst();

    ENGINE.jobs().withType(JOB_TYPE).withMaxJobsToActivate(1).activate();
  }

  @Test
  public void shouldCreateIncidentIfJobHasNoRetriesLeft() {
    // given

    // when
    final Record<JobRecordValue> failedEvent =
        ENGINE.job().withType(JOB_TYPE).ofInstance(processInstanceKey).fail();

    // then
    final Record activityEvent =
        RecordingExporter.processInstanceRecords()
            .withElementId("failingTask")
            .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .getFirst();

    final Record incidentCommand =
        RecordingExporter.incidentRecords()
            .withIntent(IncidentIntent.CREATE)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    final Record<IncidentRecordValue> incidentEvent =
        RecordingExporter.incidentRecords()
            .withIntent(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    assertThat(incidentCommand.getSourceRecordPosition())
        .isEqualTo(failedEvent.getSourceRecordPosition());
    assertThat(incidentEvent.getKey()).isGreaterThan(0);

    assertThat(incidentEvent.getValue())
        .hasErrorType(ErrorType.JOB_NO_RETRIES)
        .hasErrorMessage("No more retries left.")
        .hasBpmnProcessId("process")
        .hasProcessKey(processKey)
        .hasProcessInstanceKey(processInstanceKey)
        .hasElementId("failingTask")
        .hasElementInstanceKey(activityEvent.getKey())
        .hasVariableScopeKey(activityEvent.getKey());
  }

  @Test
  public void shouldCreateIncidentWithJobErrorMessage() {
    // given

    // when
    ENGINE
        .job()
        .ofInstance(processInstanceKey)
        .withType(JOB_TYPE)
        .withErrorMessage("failed job")
        .fail();

    // then
    final Record activityEvent =
        RecordingExporter.processInstanceRecords()
            .withElementId("failingTask")
            .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();
    final Record failEvent =
        RecordingExporter.jobRecords()
            .withIntent(JobIntent.FAILED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    final Record incidentCommand =
        RecordingExporter.incidentRecords()
            .withIntent(IncidentIntent.CREATE)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();
    final Record<IncidentRecordValue> incidentEvent =
        RecordingExporter.incidentRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withIntent(IncidentIntent.CREATED)
            .getFirst();

    assertThat(incidentCommand.getSourceRecordPosition())
        .isEqualTo(failEvent.getSourceRecordPosition());
    assertThat(incidentEvent.getKey()).isGreaterThan(0);
    assertThat(incidentEvent.getValue())
        .hasErrorType(ErrorType.JOB_NO_RETRIES)
        .hasErrorMessage("failed job")
        .hasBpmnProcessId("process")
        .hasProcessKey(processKey)
        .hasProcessInstanceKey(processInstanceKey)
        .hasElementId("failingTask")
        .hasElementInstanceKey(activityEvent.getKey())
        .hasVariableScopeKey(activityEvent.getKey());
  }

  @Test
  public void shouldIncidentContainLastFailedJobErrorMessage() {
    // given

    // when
    final JobClient jobClient = ENGINE.job().ofInstance(processInstanceKey).withType(JOB_TYPE);

    jobClient.withRetries(1).withErrorMessage("first message").fail();

    ENGINE.jobs().withType(JOB_TYPE).activate();
    jobClient.withRetries(0).withErrorMessage("second message").fail();

    // then
    final Record activityEvent =
        RecordingExporter.processInstanceRecords()
            .withElementId("failingTask")
            .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    final Record<IncidentRecordValue> incidentEvent =
        RecordingExporter.incidentRecords()
            .withIntent(IncidentIntent.CREATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    assertThat(incidentEvent.getValue())
        .hasErrorType(ErrorType.JOB_NO_RETRIES)
        .hasErrorMessage("second message")
        .hasBpmnProcessId("process")
        .hasProcessKey(processKey)
        .hasProcessInstanceKey(processInstanceKey)
        .hasElementId("failingTask")
        .hasElementInstanceKey(activityEvent.getKey())
        .hasVariableScopeKey(activityEvent.getKey());
  }

  @Test
  public void shouldResolveIncidentIfJobRetriesIncreased() {
    // given
    ENGINE.job().withType(JOB_TYPE).ofInstance(processInstanceKey).fail();
    final Record<IncidentRecordValue> incidentCreatedEvent =
        RecordingExporter.incidentRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withIntent(IncidentIntent.CREATED)
            .getFirst();

    // when
    ENGINE.job().ofInstance(processInstanceKey).withType(JOB_TYPE).withRetries(1).updateRetries();
    final Record<IncidentRecordValue> resolvedIncident =
        ENGINE
            .incident()
            .ofInstance(processInstanceKey)
            .withKey(incidentCreatedEvent.getKey())
            .resolve();
    ENGINE.jobs().withType(JOB_TYPE).activate();

    // then
    final Record jobEvent =
        RecordingExporter.jobRecords()
            .withIntent(JobIntent.FAILED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    final Record activityEvent =
        RecordingExporter.processInstanceRecords()
            .withElementId("failingTask")
            .withIntent(ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .withProcessInstanceKey(processInstanceKey)
            .getFirst();

    final Record incidentEvent =
        RecordingExporter.incidentRecords()
            .withIntent(IncidentIntent.RESOLVE)
            .withRecordKey(incidentCreatedEvent.getKey())
            .getFirst();

    final long lastPos = incidentEvent.getPosition();

    assertThat(resolvedIncident.getKey()).isGreaterThan(0);
    assertThat(resolvedIncident.getSourceRecordPosition()).isEqualTo(lastPos);

    assertThat(resolvedIncident.getValue())
        .hasErrorType(ErrorType.JOB_NO_RETRIES)
        .hasErrorMessage("No more retries left.")
        .hasBpmnProcessId("process")
        .hasProcessKey(processKey)
        .hasProcessInstanceKey(processInstanceKey)
        .hasElementId("failingTask")
        .hasElementInstanceKey(activityEvent.getKey())
        .hasVariableScopeKey(activityEvent.getKey());

    // and the job is activated again
    final var batchActivations =
        RecordingExporter.jobBatchRecords(JobBatchIntent.ACTIVATED)
            .filter(
                jobBatchRecordValueRecord ->
                    jobBatchRecordValueRecord.getValue().getJobs().stream()
                        .anyMatch(
                            jobRecordValue ->
                                jobRecordValue.getProcessInstanceKey() == processInstanceKey))
            .limit(2)
            .collect(Collectors.toList());
    assertThat(batchActivations).hasSize(2);
    final var secondActivationJobValue = batchActivations.get(1).getValue().getJobs().get(0);
    final var secondActivationJobKey = batchActivations.get(1).getValue().getJobKeys().get(0);

    assertThat(secondActivationJobKey).isEqualTo(jobEvent.getKey());
    assertThat(secondActivationJobValue).hasRetries(1);

    // and the job lifecycle is correct
    final List<Record> jobEvents =
        RecordingExporter.jobRecords()
            .filter(
                r ->
                    r.getKey() == jobEvent.getKey()
                        || r.getValue().getProcessInstanceKey() == processInstanceKey)
            .limit(6)
            .collect(Collectors.toList());

    assertThat(jobEvents)
        .extracting(Record::getRecordType, Record::getValueType, Record::getIntent)
        .containsExactly(
            tuple(RecordType.COMMAND, ValueType.JOB, JobIntent.CREATE),
            tuple(RecordType.EVENT, ValueType.JOB, JobIntent.CREATED),
            tuple(RecordType.COMMAND, ValueType.JOB, JobIntent.FAIL),
            tuple(RecordType.EVENT, ValueType.JOB, JobIntent.FAILED),
            tuple(RecordType.COMMAND, ValueType.JOB, JobIntent.UPDATE_RETRIES),
            tuple(RecordType.EVENT, ValueType.JOB, JobIntent.RETRIES_UPDATED));
  }

  @Test
  public void shouldDeleteIncidentIfJobIsCanceled() {
    // given
    ENGINE.job().withType(JOB_TYPE).ofInstance(processInstanceKey).fail();

    final Record incidentCreatedEvent =
        RecordingExporter.incidentRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withIntent(IncidentIntent.CREATED)
            .getFirst();

    // when
    ENGINE.processInstance().withInstanceKey(processInstanceKey).cancel();

    // then
    final Record<ProcessInstanceRecordValue> terminatingTask =
        RecordingExporter.processInstanceRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withElementId("failingTask")
            .withIntent(ProcessInstanceIntent.ELEMENT_TERMINATING)
            .getFirst();

    final Record jobCancelCommand =
        RecordingExporter.jobRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withIntent(JobIntent.CANCEL)
            .getFirst();

    final Record<IncidentRecordValue> resolvedIncidentEvent =
        RecordingExporter.incidentRecords()
            .withProcessInstanceKey(processInstanceKey)
            .withIntent(IncidentIntent.RESOLVED)
            .getFirst();

    assertThat(resolvedIncidentEvent.getKey()).isEqualTo(incidentCreatedEvent.getKey());
    assertThat(resolvedIncidentEvent.getSourceRecordPosition())
        .isEqualTo(terminatingTask.getPosition());
    assertThat(jobCancelCommand.getSourceRecordPosition()).isEqualTo(terminatingTask.getPosition());

    assertThat(resolvedIncidentEvent.getValue())
        .hasErrorType(ErrorType.JOB_NO_RETRIES)
        .hasErrorMessage("No more retries left.")
        .hasBpmnProcessId("process")
        .hasProcessKey(processKey)
        .hasProcessInstanceKey(processInstanceKey)
        .hasElementId("failingTask")
        .hasVariableScopeKey(terminatingTask.getKey());
  }
}
