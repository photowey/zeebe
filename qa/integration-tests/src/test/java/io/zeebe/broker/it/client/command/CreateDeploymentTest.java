/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.broker.it.client.command;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.zeebe.broker.it.util.GrpcClientRule;
import io.zeebe.broker.test.EmbeddedBrokerRule;
import io.zeebe.client.api.command.ClientException;
import io.zeebe.client.api.response.DeploymentEvent;
import io.zeebe.client.api.response.Process;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.test.util.BrokerClassRuleHelper;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public final class CreateDeploymentTest {

  private static final EmbeddedBrokerRule BROKER_RULE = new EmbeddedBrokerRule();
  private static final GrpcClientRule CLIENT_RULE = new GrpcClientRule(BROKER_RULE);

  @ClassRule
  public static RuleChain ruleChain = RuleChain.outerRule(BROKER_RULE).around(CLIENT_RULE);

  @Rule public final BrokerClassRuleHelper helper = new BrokerClassRuleHelper();

  @Test
  public void shouldDeployProcessModel() {
    // given
    final String processId = helper.getBpmnProcessId();
    final String resourceName = processId + ".bpmn";

    final BpmnModelInstance process =
        Bpmn.createExecutableProcess(processId)
            .startEvent()
            .serviceTask("task", t -> t.zeebeJobType("test"))
            .endEvent()
            .done();

    // when
    final DeploymentEvent result =
        CLIENT_RULE
            .getClient()
            .newDeployCommand()
            .addProcessModel(process, resourceName)
            .send()
            .join();

    // then
    assertThat(result.getKey()).isGreaterThan(0);
    assertThat(result.getProcesses()).hasSize(1);

    final Process deployedProcess = result.getProcesses().get(0);
    assertThat(deployedProcess.getBpmnProcessId()).isEqualTo(processId);
    assertThat(deployedProcess.getVersion()).isEqualTo(1);
    assertThat(deployedProcess.getProcessKey()).isGreaterThan(0);
    assertThat(deployedProcess.getResourceName()).isEqualTo(resourceName);
  }

  @Test
  public void shouldRejectDeployIfProcessIsInvalid() {
    // given
    final BpmnModelInstance process =
        Bpmn.createExecutableProcess("process").startEvent().serviceTask("task").done();

    // when
    final var command =
        CLIENT_RULE
            .getClient()
            .newDeployCommand()
            .addProcessModel(process, "process.bpmn")
            .send();

    // when
    assertThatThrownBy(() -> command.join())
        .isInstanceOf(ClientException.class)
        .hasMessageContaining("Must have exactly one 'zeebe:taskDefinition' extension element");
  }
}
