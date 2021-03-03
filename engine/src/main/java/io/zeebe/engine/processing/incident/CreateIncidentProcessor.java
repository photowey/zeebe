/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.incident;

import io.zeebe.engine.processing.streamprocessor.CommandProcessor;
import io.zeebe.engine.processing.streamprocessor.TypedRecord;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.immutable.ElementInstanceState;
import io.zeebe.engine.state.immutable.IncidentState;
import io.zeebe.engine.state.immutable.JobState;
import io.zeebe.engine.state.immutable.JobState.State;
import io.zeebe.engine.state.instance.IndexedRecord;
import io.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.zeebe.protocol.record.RejectionType;
import io.zeebe.protocol.record.intent.IncidentIntent;
import io.zeebe.protocol.record.value.ErrorType;

public final class CreateIncidentProcessor implements CommandProcessor<IncidentRecord> {

  private static final String NO_FAILED_RECORD_MESSAGE =
      "Expected to create incident for failed record with key '%d', but no such record was found";

  private static final String INVALID_JOB_STATE_MESSAGE =
      "Expected to create incident for failed job with key '%d', but it is in state '%s'";
  private static final String NO_FAILED_JOB_MESSAGE =
      "Expected to create incident for failed job with key '%d', but no such job was found";

  private final ZeebeState zeebeState;

  public CreateIncidentProcessor(final ZeebeState zeebeState) {
    this.zeebeState = zeebeState;
  }

  @Override
  public boolean onCommand(
      final TypedRecord<IncidentRecord> command,
      final CommandControl<IncidentRecord> commandControl) {
    final IncidentRecord incidentEvent = command.getValue();

    final boolean incidentIsNotRejected =
        !rejectIncidentCreation(incidentEvent, commandControl, command);

    if (incidentIsNotRejected) {
      final long incidentKey = commandControl.accept(IncidentIntent.CREATED, incidentEvent);

      // TODO (#6174) move to event applier
      if (ErrorType.MESSAGE_SIZE_EXCEEDED == incidentEvent.getErrorType()) {
        final var jobState = zeebeState.getJobState();
        final var jobkey = incidentEvent.getJobKey();
        final var jobRecord = jobState.getJob(jobkey);
        jobState.disable(jobkey, jobRecord);
      }
      // TODO (#6174) move to event applier

      zeebeState.getIncidentState().createIncident(incidentKey, incidentEvent);
    }

    return true;
  }

  public boolean rejectIncidentCreation(
      final IncidentRecord incidentEvent,
      final CommandControl<IncidentRecord> commandControl,
      final TypedRecord<IncidentRecord> command) {
    final IncidentState incidentState = zeebeState.getIncidentState();

    final boolean isJobIncident = incidentState.isJobIncident(incidentEvent);

    if (isJobIncident) {
      return rejectJobIncident(incidentEvent.getJobKey(), commandControl, command);
    } else {
      return rejectWorkflowInstanceIncident(incidentEvent.getElementInstanceKey(), commandControl);
    }
  }

  private boolean rejectJobIncident(
      final long jobKey,
      final CommandControl<IncidentRecord> commandControl,
      final TypedRecord<IncidentRecord> command) {
    final JobState state = zeebeState.getJobState();
    final JobState.State jobState = state.getState(jobKey);

    if (jobState == State.NOT_FOUND) {
      commandControl.reject(RejectionType.NOT_FOUND, String.format(NO_FAILED_JOB_MESSAGE, jobKey));
      return true;
    } else if (jobState == State.ACTIVATABLE
        && ErrorType.MESSAGE_SIZE_EXCEEDED == command.getValue().getErrorType()) {
      /**
       * if the message size is exceeded the job is still in activatable stage. An incident needs to
       * be created and after the incident is created, the job needs to be disabled
       */
      return false;
    } else if (jobState != State.FAILED && jobState != State.ERROR_THROWN) {
      commandControl.reject(
          RejectionType.INVALID_STATE, String.format(INVALID_JOB_STATE_MESSAGE, jobKey, jobState));
      return true;
    }

    return false;
  }

  private boolean rejectWorkflowInstanceIncident(
      final long elementInstanceKey, final CommandControl<IncidentRecord> commandControl) {
    final ElementInstanceState elementInstanceState = zeebeState.getElementInstanceState();

    final IndexedRecord failedRecord = elementInstanceState.getFailedRecord(elementInstanceKey);
    final boolean noFailedRecord = failedRecord == null;
    if (noFailedRecord) {
      commandControl.reject(
          RejectionType.NOT_FOUND, String.format(NO_FAILED_RECORD_MESSAGE, elementInstanceKey));
    }
    return noFailedRecord;
  }
}
