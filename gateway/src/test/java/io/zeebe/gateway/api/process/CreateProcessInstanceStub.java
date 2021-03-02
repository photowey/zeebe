/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.api.process;

import io.zeebe.gateway.api.util.StubbedBrokerClient;
import io.zeebe.gateway.api.util.StubbedBrokerClient.RequestStub;
import io.zeebe.gateway.impl.broker.request.BrokerCreateProcessInstanceRequest;
import io.zeebe.gateway.impl.broker.response.BrokerResponse;
import io.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceCreationRecord;

public final class CreateProcessInstanceStub
    implements RequestStub<
        BrokerCreateProcessInstanceRequest, BrokerResponse<ProcessInstanceCreationRecord>> {

  public static final long PROCESS_INSTANCE_KEY = 123;
  public static final String PROCESS_ID = "process";
  public static final int PROCESS_VERSION = 1;
  public static final long PROCESS_KEY = 456;
  private BrokerResponse<ProcessInstanceCreationRecord> response;

  @Override
  public void registerWith(final StubbedBrokerClient gateway) {
    gateway.registerHandler(BrokerCreateProcessInstanceRequest.class, this);
  }

  public CreateProcessInstanceStub respondWith(
      final BrokerResponse<ProcessInstanceCreationRecord> response) {
    this.response = response;
    return this;
  }

  public long getProcessInstanceKey() {
    return PROCESS_INSTANCE_KEY;
  }

  public String getProcessId() {
    return PROCESS_ID;
  }

  public int getProcessVersion() {
    return PROCESS_VERSION;
  }

  public long getProcessKey() {
    return PROCESS_KEY;
  }

  @Override
  public BrokerResponse<ProcessInstanceCreationRecord> handle(
      final BrokerCreateProcessInstanceRequest request) {

    if (response != null) {
      return response;
    }

    return getDefaultResponse(request);
  }

  private BrokerResponse<ProcessInstanceCreationRecord> getDefaultResponse(
      final BrokerCreateProcessInstanceRequest request) {
    final var record = new ProcessInstanceCreationRecord();
    record.setBpmnProcessId(PROCESS_ID);
    record.setVariables(request.getRequestWriter().getVariablesBuffer());
    record.setVersion(PROCESS_VERSION);
    record.setProcessKey(PROCESS_KEY);
    record.setProcessInstanceKey(PROCESS_INSTANCE_KEY);
    return new BrokerResponse<>(record, 0, PROCESS_INSTANCE_KEY);
  }
}
