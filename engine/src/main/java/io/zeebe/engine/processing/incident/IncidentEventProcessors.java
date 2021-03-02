/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.incident;

import io.zeebe.engine.processing.job.JobErrorThrownProcessor;
import io.zeebe.engine.processing.streamprocessor.TypedRecordProcessor;
import io.zeebe.engine.processing.streamprocessor.TypedRecordProcessors;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.IncidentIntent;

public final class IncidentEventProcessors {

  public static void addProcessors(
      final TypedRecordProcessors typedRecordProcessors,
      final ZeebeState zeebeState,
      final TypedRecordProcessor<ProcessInstanceRecord> bpmnStreamProcessor,
      final JobErrorThrownProcessor jobErrorThrownProcessor) {
    typedRecordProcessors
        .onCommand(
            ValueType.INCIDENT, IncidentIntent.CREATE, new CreateIncidentProcessor(zeebeState))
        .onCommand(
            ValueType.INCIDENT,
            IncidentIntent.RESOLVE,
            new ResolveIncidentProcessor(zeebeState, bpmnStreamProcessor, jobErrorThrownProcessor));
  }
}
