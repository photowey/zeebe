/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.api.workflow;

import static io.zeebe.util.buffer.BufferUtil.wrapString;

import io.zeebe.gateway.api.util.StubbedBrokerClient;
import io.zeebe.gateway.api.util.StubbedBrokerClient.RequestStub;
import io.zeebe.gateway.impl.broker.request.BrokerResolveIncidentRequest;
import io.zeebe.gateway.impl.broker.response.BrokerResponse;
import io.zeebe.protocol.impl.record.value.incident.IncidentRecord;
import io.zeebe.protocol.record.value.ErrorType;
import org.agrona.DirectBuffer;

public final class ResolveIncidentStub
    implements RequestStub<BrokerResolveIncidentRequest, BrokerResponse<IncidentRecord>> {

  public static final long WORKFLOW_INSTANCE_KEY = 123;
  public static final long INCIDENT_KEY = 11;
  public static final DirectBuffer PROCESS_ID = wrapString("process");

  @Override
  public void registerWith(final StubbedBrokerClient gateway) {
    gateway.registerHandler(BrokerResolveIncidentRequest.class, this);
  }

  public long getIncidentKey() {
    return INCIDENT_KEY;
  }

  @Override
  public BrokerResponse<IncidentRecord> handle(final BrokerResolveIncidentRequest request) {

    final IncidentRecord response = new IncidentRecord();
    response.setElementInstanceKey(WORKFLOW_INSTANCE_KEY);
    response.setWorkflowInstanceKey(WORKFLOW_INSTANCE_KEY);
    response.setElementId(PROCESS_ID);
    response.setBpmnProcessId(PROCESS_ID);
    response.setErrorMessage("Error in IO mapping");
    response.setErrorType(ErrorType.IO_MAPPING_ERROR);

    return new BrokerResponse<>(response, 0, WORKFLOW_INSTANCE_KEY);
  }
}
