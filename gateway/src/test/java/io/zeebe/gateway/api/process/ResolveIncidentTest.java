/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.gateway.api.workflow;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.gateway.api.util.GatewayTest;
import io.zeebe.gateway.impl.broker.request.BrokerResolveIncidentRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.ResolveIncidentRequest;
import io.zeebe.gateway.protocol.GatewayOuterClass.ResolveIncidentResponse;
import io.zeebe.protocol.record.ValueType;
import io.zeebe.protocol.record.intent.IncidentIntent;
import org.junit.Test;

public final class ResolveIncidentTest extends GatewayTest {

  @Test
  public void shouldMapRequestAndResponse() {
    // given
    final ResolveIncidentStub stub = new ResolveIncidentStub();
    stub.registerWith(brokerClient);

    final ResolveIncidentRequest request =
        ResolveIncidentRequest.newBuilder().setIncidentKey(stub.getIncidentKey()).build();

    // when
    final ResolveIncidentResponse response = client.resolveIncident(request);

    // then
    assertThat(response).isNotNull();

    final BrokerResolveIncidentRequest brokerRequest = brokerClient.getSingleBrokerRequest();
    assertThat(brokerRequest.getIntent()).isEqualTo(IncidentIntent.RESOLVE);
    assertThat(brokerRequest.getValueType()).isEqualTo(ValueType.INCIDENT);
    assertThat(brokerRequest.getKey()).isEqualTo(stub.getIncidentKey());
  }
}
