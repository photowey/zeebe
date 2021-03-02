/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.appliers;

import io.zeebe.engine.state.TypedEventApplier;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.mutable.MutableProcessState;
import io.zeebe.protocol.impl.record.value.deployment.ProcessRecord;
import io.zeebe.protocol.record.intent.ProcessIntent;

public class ProcessCreatedApplier implements TypedEventApplier<ProcessIntent, ProcessRecord> {

  private final MutableProcessState processState;

  public ProcessCreatedApplier(final ZeebeState state) {
    processState = state.getProcessState();
  }

  @Override
  public void applyState(final long processKey, final ProcessRecord value) {
    processState.putProcess(processKey, value);
  }
}
