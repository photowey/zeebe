/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.api.process;

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.gateway.api.util.GatewayTest;
import io.zeebe.gateway.impl.broker.request.BrokerCreateProcessInstanceWithResultRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateProcessInstanceRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateProcessInstanceWithResultRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.CreateProcessInstanceWithResultResponse;
import io.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceCreationRecord;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.ProcessInstanceCreationIntent;
import java.util.List;
import org.junit.Test;

public final class CreateProcessInstanceWithResultTest extends GatewayTest {

  @Test
  public void shouldMapToBrokerRequest() {
    // given
    final CreateProcessInstanceWithResultStub stub = new CreateProcessInstanceWithResultStub();
    stub.registerWith(brokerClient);

    final CreateProcessInstanceWithResultRequest request =
        CreateProcessInstanceWithResultRequest.newBuilder()
            .setRequest(
                CreateProcessInstanceRequest.newBuilder().setProcessKey(stub.getProcessKey()))
            .addAllFetchVariables(List.of("x"))
            .build();

    // when
    client.createProcessInstanceWithResult(request);

    // then
    final BrokerCreateProcessInstanceWithResultRequest brokerRequest =
        brokerClient.getSingleBrokerRequest();
    assertThat(brokerRequest.getIntent())
        .isEqualTo(ProcessInstanceCreationIntent.CREATE_WITH_AWAITING_RESULT);
    assertThat(brokerRequest.getValueType()).isEqualTo(ValueType.PROCESS_INSTANCE_CREATION);

    final ProcessInstanceCreationRecord brokerRequestValue = brokerRequest.getRequestWriter();
    assertThat(brokerRequestValue.getProcessKey()).isEqualTo(stub.getProcessKey());
    assertThat(brokerRequestValue.fetchVariables().iterator().next().getValue())
        .isEqualTo(wrapString("x"));
  }

  @Test
  public void shouldMapRequestAndResponse() {
    // given
    final CreateProcessInstanceWithResultStub stub = new CreateProcessInstanceWithResultStub();
    stub.registerWith(brokerClient);

    final CreateProcessInstanceWithResultRequest request =
        CreateProcessInstanceWithResultRequest.newBuilder()
            .setRequest(
                CreateProcessInstanceRequest.newBuilder().setProcessKey(stub.getProcessKey()))
            .build();

    // when
    final CreateProcessInstanceWithResultResponse response =
        client.createProcessInstanceWithResult(request);

    // then
    assertThat(response.getBpmnProcessId()).isEqualTo(stub.getProcessId());
    assertThat(response.getVersion()).isEqualTo(stub.getProcessVersion());
    assertThat(response.getProcessKey()).isEqualTo(stub.getProcessKey());
    assertThat(response.getProcessInstanceKey()).isEqualTo(stub.getProcessInstanceKey());
  }
}
