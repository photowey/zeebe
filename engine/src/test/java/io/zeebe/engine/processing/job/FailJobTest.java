/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.job;

import static io.zeebe.protocol.record.intent.JobIntent.FAIL;
import static io.zeebe.protocol.record.intent.JobIntent.FAILED;
import static io.zeebe.test.util.record.RecordingExporter.jobRecords;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import io.zeebe.engine.util.EngineRule;
import io.zeebe.protocol.record.Assertions;
import io.zeebe.protocol.record.Record;
import io.zeebe.protocol.record.RecordType;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.JobBatchIntent;
import io.zeebe.protocol.record.intent.JobIntent;
import io.zeebe.protocol.record.value.JobBatchRecordValue;
import io.zeebe.protocol.record.value.JobRecordValue;
import io.zeebe.test.util.Strings;
import io.zeebe.test.util.record.RecordingExporter;
import io.zeebe.test.util.record.RecordingExporterTestWatcher;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public final class FailJobTest {
  @ClassRule public static final EngineRule ENGINE = EngineRule.singlePartition();
  private static final String PROCESS_ID = "process";
  private static String jobType;

  @Rule
  public final RecordingExporterTestWatcher recordingExporterTestWatcher =
      new RecordingExporterTestWatcher();

  @Before
  public void setup() {
    jobType = Strings.newRandomValidBpmnId();
  }

  @Test
  public void shouldFail() {
    // given
    ENGINE.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = ENGINE.jobs().withType(jobType).activate();
    final JobRecordValue job = batchRecord.getValue().getJobs().get(0);
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);
    final int retries = 23;

    // when
    final Record<JobRecordValue> failRecord =
        ENGINE
            .job()
            .withKey(jobKey)
            .ofInstance(job.getProcessInstanceKey())
            .withRetries(retries)
            .fail();

    // then
    Assertions.assertThat(failRecord).hasRecordType(RecordType.EVENT).hasIntent(FAILED);
    Assertions.assertThat(failRecord.getValue())
        .hasWorker(job.getWorker())
        .hasType(job.getType())
        .hasRetries(retries)
        .hasDeadline(job.getDeadline());
  }

  @Test
  public void shouldFailWithMessage() {
    // given
    ENGINE.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = ENGINE.jobs().withType(jobType).activate();

    final long jobKey = batchRecord.getValue().getJobKeys().get(0);
    final JobRecordValue job = batchRecord.getValue().getJobs().get(0);
    final int retries = 23;

    // when
    final Record<JobRecordValue> failedRecord =
        ENGINE
            .job()
            .withKey(jobKey)
            .ofInstance(job.getProcessInstanceKey())
            .withRetries(retries)
            .withErrorMessage("failed job")
            .fail();

    // then
    Assertions.assertThat(failedRecord).hasRecordType(RecordType.EVENT).hasIntent(FAILED);
    Assertions.assertThat(failedRecord.getValue())
        .hasWorker(job.getWorker())
        .hasType(job.getType())
        .hasRetries(retries)
        .hasDeadline(job.getDeadline())
        .hasErrorMessage(failedRecord.getValue().getErrorMessage());
  }

  @Test
  public void shouldFailJobAndRetry() {
    // given
    final Record<JobRecordValue> job = ENGINE.createJob(jobType, PROCESS_ID);

    final Record<JobBatchRecordValue> batchRecord = ENGINE.jobs().withType(jobType).activate();
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);

    // when
    final Record<JobRecordValue> failRecord =
        ENGINE
            .job()
            .withKey(jobKey)
            .ofInstance(job.getValue().getProcessInstanceKey())
            .withRetries(3)
            .fail();
    ENGINE.jobs().withType(jobType).activate();

    // then
    Assertions.assertThat(failRecord).hasRecordType(RecordType.EVENT).hasIntent(FAILED);

    // and the job is published again
    final var jobBatchActivations =
        RecordingExporter.jobBatchRecords(JobBatchIntent.ACTIVATED)
            .withType(jobType)
            .limit(2)
            .collect(Collectors.toList());

    assertThat(jobBatchActivations).hasSize(2);
    assertThat(jobBatchActivations.get(0)).isEqualTo(batchRecord);
    assertThat(jobBatchActivations.get(1).getPosition())
        .isGreaterThan(jobBatchActivations.get(0).getPosition());
    assertThat(jobBatchActivations.get(1).getValue().getJobKeys().get(0)).isEqualTo(jobKey);

    // and the job lifecycle is correct
    final List<Record> jobEvents = jobRecords().limit(4).collect(Collectors.toList());
    assertThat(jobEvents)
        .extracting(Record::getRecordType, Record::getValueType, Record::getIntent)
        .containsExactly(
            tuple(RecordType.COMMAND, ValueType.JOB, JobIntent.CREATE),
            tuple(RecordType.EVENT, ValueType.JOB, JobIntent.CREATED),
            tuple(RecordType.COMMAND, ValueType.JOB, FAIL),
            tuple(RecordType.EVENT, ValueType.JOB, FAILED));

    final List<Record<JobBatchRecordValue>> jobActivateCommands =
        RecordingExporter.jobBatchRecords().limit(4).collect(Collectors.toList());

    assertThat(jobActivateCommands)
        .extracting(Record::getRecordType, Record::getValueType, Record::getIntent)
        .containsExactly(
            tuple(RecordType.COMMAND, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATE),
            tuple(RecordType.EVENT, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATED),
            tuple(RecordType.COMMAND, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATE),
            tuple(RecordType.EVENT, ValueType.JOB_BATCH, JobBatchIntent.ACTIVATED));
  }

  @Test
  public void shouldFailIfJobCreated() {
    // given
    final Record<JobRecordValue> job = ENGINE.createJob(jobType, PROCESS_ID);

    // when
    final Record<JobRecordValue> jobRecord = ENGINE.job().withKey(job.getKey()).fail();

    // then
    Assertions.assertThat(jobRecord).hasRecordType(RecordType.EVENT).hasIntent(FAILED);
  }

  @Test
  public void shouldRejectFailIfJobNotFound() {
    // given
    final int key = 123;

    // when
    final Record<JobRecordValue> jobRecord =
        ENGINE.job().withKey(key).withRetries(3).expectRejection().fail();

    // then
    Assertions.assertThat(jobRecord).hasRejectionType(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldRejectFailIfJobAlreadyFailed() {
    // given
    ENGINE.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = ENGINE.jobs().withType(jobType).activate();
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);
    ENGINE.job().withKey(jobKey).withRetries(0).fail();

    // when
    final Record<JobRecordValue> jobRecord =
        ENGINE.job().withKey(jobKey).withRetries(3).expectRejection().fail();

    // then
    Assertions.assertThat(jobRecord).hasRejectionType(RejectionType.INVALID_STATE);
    assertThat(jobRecord.getRejectionReason()).contains("it is in state 'FAILED'");
  }

  @Test
  public void shouldRejectFailIfJobCompleted() {
    // given
    ENGINE.createJob(jobType, PROCESS_ID);
    final Record<JobBatchRecordValue> batchRecord = ENGINE.jobs().withType(jobType).activate();
    final JobRecordValue job = batchRecord.getValue().getJobs().get(0);
    final long jobKey = batchRecord.getValue().getJobKeys().get(0);

    ENGINE.job().withKey(jobKey).complete();

    // when
    final Record<JobRecordValue> jobRecord =
        ENGINE.job().withKey(jobKey).withRetries(3).expectRejection().fail();

    // then
    Assertions.assertThat(jobRecord).hasRejectionType(RejectionType.NOT_FOUND);
  }

  @Test
  public void shouldRejectFailIfErrorThrown() {
    // given
    final var job = ENGINE.createJob(jobType, PROCESS_ID);

    ENGINE.job().withKey(job.getKey()).withErrorCode("error").throwError();

    // when
    final Record<JobRecordValue> jobRecord =
        ENGINE.job().withKey(job.getKey()).withRetries(3).expectRejection().fail();

    // then
    Assertions.assertThat(jobRecord).hasRejectionType(RejectionType.INVALID_STATE);
    assertThat(jobRecord.getRejectionReason()).contains("is in state 'ERROR_THROWN'");
  }
}
