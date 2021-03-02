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
import io.zeebe.client.api.response.ProcessInstanceEvent;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.protocol.record.intent.ProcessInstanceCreationIntent;
import io.zeebe.test.util.BrokerClassRuleHelper;
import io.zeebe.test.util.record.RecordingExporter;
import java.util.Map;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

public final class CreateProcessInstanceTest {

  private static final EmbeddedBrokerRule BROKER_RULE = new EmbeddedBrokerRule();
  private static final GrpcClientRule CLIENT_RULE = new GrpcClientRule(BROKER_RULE);

  @ClassRule
  public static RuleChain ruleChain = RuleChain.outerRule(BROKER_RULE).around(CLIENT_RULE);

  @Rule public final BrokerClassRuleHelper helper = new BrokerClassRuleHelper();

  private String processId;
  private long firstProcessKey;
  private Long secondProcessKey;

  @Before
  public void deployProcess() {
    processId = helper.getBpmnProcessId();

    firstProcessKey =
        CLIENT_RULE.deployProcess(Bpmn.createExecutableProcess(processId).startEvent("v1").done());
    secondProcessKey =
        CLIENT_RULE.deployProcess(Bpmn.createExecutableProcess(processId).startEvent("v2").done());
  }

  @Test
  public void shouldCreateBpmnProcessById() {
    // when
    final ProcessInstanceEvent processInstance =
        CLIENT_RULE
            .getClient()
            .newCreateInstanceCommand()
            .bpmnProcessId(processId)
            .latestVersion()
            .send()
            .join();

    // then
    assertThat(processInstance.getBpmnProcessId()).isEqualTo(processId);
    assertThat(processInstance.getVersion()).isEqualTo(2);
    assertThat(processInstance.getProcessKey()).isEqualTo(secondProcessKey);
  }

  @Test
  public void shouldCreateBpmnProcessByIdAndVersion() {
    // when
    final ProcessInstanceEvent processInstance =
        CLIENT_RULE
            .getClient()
            .newCreateInstanceCommand()
            .bpmnProcessId(processId)
            .version(1)
            .send()
            .join();

    // then instance is created of first process version
    assertThat(processInstance.getBpmnProcessId()).isEqualTo(processId);
    assertThat(processInstance.getVersion()).isEqualTo(1);
    assertThat(processInstance.getProcessKey()).isEqualTo(firstProcessKey);
  }

  @Test
  public void shouldCreateBpmnProcessByKey() {
    // when
    final ProcessInstanceEvent processInstance =
        CLIENT_RULE
            .getClient()
            .newCreateInstanceCommand()
            .processKey(firstProcessKey)
            .send()
            .join();

    // then
    assertThat(processInstance.getBpmnProcessId()).isEqualTo(processId);
    assertThat(processInstance.getVersion()).isEqualTo(1);
    assertThat(processInstance.getProcessKey()).isEqualTo(firstProcessKey);
  }

  @Test
  public void shouldCreateWithVariables() {
    // given
    final Map<String, Object> variables = Map.of("foo", 123);

    // when
    final ProcessInstanceEvent event =
        CLIENT_RULE
            .getClient()
            .newCreateInstanceCommand()
            .bpmnProcessId(processId)
            .latestVersion()
            .variables(variables)
            .send()
            .join();

    // then
    final var createdEvent =
        RecordingExporter.processInstanceCreationRecords()
            .withIntent(ProcessInstanceCreationIntent.CREATED)
            .withInstanceKey(event.getProcessInstanceKey())
            .getFirst();

    assertThat(createdEvent.getValue().getVariables()).containsExactlyEntriesOf(variables);
  }

  @Test
  public void shouldCreateWithoutVariables() {
    // when
    final ProcessInstanceEvent event =
        CLIENT_RULE
            .getClient()
            .newCreateInstanceCommand()
            .bpmnProcessId(processId)
            .latestVersion()
            .send()
            .join();

    // then
    final var createdEvent =
        RecordingExporter.processInstanceCreationRecords()
            .withIntent(ProcessInstanceCreationIntent.CREATED)
            .withInstanceKey(event.getProcessInstanceKey())
            .getFirst();

    assertThat(createdEvent.getValue().getVariables()).isEmpty();
  }

  @Test
  public void shouldCreateWithNullVariables() {
    // when
    final ProcessInstanceEvent event =
        CLIENT_RULE
            .getClient()
            .newCreateInstanceCommand()
            .bpmnProcessId(processId)
            .latestVersion()
            .variables("null")
            .send()
            .join();

    // then
    final var createdEvent =
        RecordingExporter.processInstanceCreationRecords()
            .withIntent(ProcessInstanceCreationIntent.CREATED)
            .withInstanceKey(event.getProcessInstanceKey())
            .getFirst();

    assertThat(createdEvent.getValue().getVariables()).isEmpty();
  }

  @Test
  public void shouldRejectCompleteJobIfVariablesAreInvalid() {
    // when
    final var command =
        CLIENT_RULE
            .getClient()
            .newCreateInstanceCommand()
            .bpmnProcessId(processId)
            .latestVersion()
            .variables("[]")
            .send();

    assertThatThrownBy(() -> command.join())
        .isInstanceOf(ClientException.class)
        .hasMessageContaining(
            "Property 'variables' is invalid: Expected document to be a root level object, but was 'ARRAY'");
  }

  @Test
  public void shouldRejectCreateBpmnProcessByNonExistingId() {
    // when
    final var command =
        CLIENT_RULE
            .getClient()
            .newCreateInstanceCommand()
            .bpmnProcessId("non-existing")
            .latestVersion()
            .send();

    assertThatThrownBy(() -> command.join())
        .isInstanceOf(ClientException.class)
        .hasMessageContaining(
            "Expected to find process definition with process ID 'non-existing', but none found");
  }

  @Test
  public void shouldRejectCreateBpmnProcessByNonExistingKey() {
    // when
    final var command = CLIENT_RULE.getClient().newCreateInstanceCommand().processKey(123L).send();

    assertThatThrownBy(() -> command.join())
        .isInstanceOf(ClientException.class)
        .hasMessageContaining("Expected to find process definition with key '123', but none found");
  }
}
