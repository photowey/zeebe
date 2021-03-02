/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.instance;

import static io.zeebe.util.buffer.BufferUtil.wrapString;
import static java.util.function.Predicate.not;
import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.engine.state.ZbColumnFamilies;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.engine.state.instance.StoredRecord.Purpose;
import io.zeebe.engine.state.mutable.MutableElementInstanceState;
import io.zeebe.engine.util.ZeebeStateRule;
import io.zeebe.protocol.impl.record.value.processinstance.ProcessInstanceRecord;
import io.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.zeebe.protocol.record.value.BpmnElementType;
import io.zeebe.test.util.MsgPackUtil;
import io.zeebe.util.buffer.BufferUtil;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.agrona.DirectBuffer;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public final class ElementInstanceStateTest {

  private static final long PROCESS_KEY = 123;

  @Rule public final ZeebeStateRule stateRule = new ZeebeStateRule();

  private MutableElementInstanceState elementInstanceState;
  private ZeebeState zeebeState;

  @Before
  public void setUp() {
    zeebeState = stateRule.getZeebeState();
    elementInstanceState = zeebeState.getElementInstanceState();
  }

  @Test
  public void shouldCreateNewInstance() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();

    // when
    final ElementInstance elementInstance =
        elementInstanceState.newInstance(
            100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);

    // then
    assertElementInstance(elementInstance, 0);
  }

  @Test
  public void shouldCreateNewInstanceWithParent() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    final ElementInstance parentInstance =
        elementInstanceState.newInstance(
            100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);

    // when
    final ProcessInstanceRecord otherRecord = createProcessInstanceRecord();
    otherRecord.setElementId("subProcess");
    final ElementInstance childInstance =
        elementInstanceState.newInstance(
            parentInstance, 101, otherRecord, ProcessInstanceIntent.ELEMENT_ACTIVATING);

    // then
    assertElementInstance(parentInstance, 1);
    assertChildInstance(childInstance, 101, "subProcess");
  }

  @Test
  public void shouldSpawnToken() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    elementInstanceState.newInstance(
        100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);

    // when
    elementInstanceState.spawnToken(100);

    // then
    final ElementInstance elementInstance = elementInstanceState.getInstance(100);
    Assertions.assertThat(elementInstance.getNumberOfActiveElementInstances()).isEqualTo(0);
    Assertions.assertThat(elementInstance.getNumberOfActiveExecutionPaths()).isEqualTo(1);
    Assertions.assertThat(elementInstance.getNumberOfActiveTokens()).isEqualTo(1);
  }

  @Test
  public void shouldConsumeToken() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    elementInstanceState.newInstance(
        100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);
    elementInstanceState.spawnToken(100);

    // when
    elementInstanceState.consumeToken(100);

    // then
    final ElementInstance elementInstance = elementInstanceState.getInstance(100);
    Assertions.assertThat(elementInstance.getNumberOfActiveElementInstances()).isEqualTo(0);
    Assertions.assertThat(elementInstance.getNumberOfActiveExecutionPaths()).isEqualTo(0);
    Assertions.assertThat(elementInstance.getNumberOfActiveTokens()).isEqualTo(0);
  }

  @Test
  public void shouldFindElementInstance() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    elementInstanceState.newInstance(
        100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);

    // when
    final ElementInstance instance = elementInstanceState.getInstance(100);

    // then
    assertElementInstance(instance, 0);
  }

  @Test
  public void shouldFindChildInstance() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    final ElementInstance parentInstance =
        elementInstanceState.newInstance(
            100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);
    processInstanceRecord.setElementId("subProcess");
    elementInstanceState.newInstance(
        parentInstance, 101, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATING);

    // when
    final ElementInstance childInstance = elementInstanceState.getInstance(101L);

    // then
    assertChildInstance(childInstance, 101, "subProcess");
  }

  @Test
  public void shouldFindParentInstance() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    final ElementInstance parentInstance =
        elementInstanceState.newInstance(
            100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);
    processInstanceRecord.setElementId("subProcess");
    elementInstanceState.newInstance(
        parentInstance, 101, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATING);

    // when
    final ElementInstance updatedParentInstance = elementInstanceState.getInstance(100L);

    // then
    assertElementInstance(updatedParentInstance, 1);
  }

  @Test
  public void shouldRemoveParentInstanceAfterRemovingChild() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    final ElementInstance parentInstance =
        elementInstanceState.newInstance(
            100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);
    processInstanceRecord.setElementId("subProcess");
    elementInstanceState.newInstance(
        parentInstance, 101, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATING);
    elementInstanceState.removeInstance(101L);

    // when
    elementInstanceState.removeInstance(100L);

    // then
    final ElementInstance childInstance = elementInstanceState.getInstance(101L);
    Assertions.assertThat(childInstance).isNull();

    final ElementInstance parent = elementInstanceState.getInstance(100L);
    Assertions.assertThat(parent).isNull();

    final List<ElementInstance> children = elementInstanceState.getChildren(100L);
    Assertions.assertThat(children).hasSize(0);
  }

  @Test
  public void shouldRemoveChildInstance() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    final ElementInstance parentInstance =
        elementInstanceState.newInstance(
            100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);
    processInstanceRecord.setElementId("subProcess");
    elementInstanceState.newInstance(
        parentInstance, 101, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATING);
    processInstanceRecord.setElementId("subProcess2");
    elementInstanceState.newInstance(
        parentInstance, 102, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATING);

    // when
    elementInstanceState.removeInstance(101L);

    // then
    final ElementInstance childInstance = elementInstanceState.getInstance(101L);
    Assertions.assertThat(childInstance).isNull();

    final List<ElementInstance> children = elementInstanceState.getChildren(100L);
    Assertions.assertThat(children).hasSize(1);

    final ElementInstance childInstance2 = elementInstanceState.getInstance(102L);
    assertChildInstance(childInstance2, 102L, "subProcess2");

    final ElementInstance updatedParent = elementInstanceState.getInstance(100L);
    assertElementInstance(updatedParent, 1);
  }

  @Test
  public void shouldUpdateElementInstance() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    final ElementInstance instance =
        elementInstanceState.newInstance(
            100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);

    // when
    instance.spawnToken();
    instance.setState(ProcessInstanceIntent.ELEMENT_ACTIVATING);
    instance.setJobKey(5);
    elementInstanceState.updateInstance(instance);

    // then
    final ElementInstance updatedInstance = elementInstanceState.getInstance(100);

    Assertions.assertThat(updatedInstance.getKey()).isEqualTo(100);
    Assertions.assertThat(updatedInstance.getState())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATING);
    Assertions.assertThat(updatedInstance.getJobKey()).isEqualTo(5);
    Assertions.assertThat(updatedInstance.canTerminate()).isTrue();

    Assertions.assertThat(updatedInstance.getNumberOfActiveElementInstances()).isEqualTo(0);
    Assertions.assertThat(updatedInstance.getNumberOfActiveExecutionPaths()).isEqualTo(1);
    Assertions.assertThat(updatedInstance.getNumberOfActiveTokens()).isEqualTo(1);

    final ProcessInstanceRecord record = updatedInstance.getValue();
    assertProcessInstanceRecord(record);
  }

  @Test
  public void shouldNotUpdateElementInstance() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    final ElementInstance instance =
        elementInstanceState.newInstance(
            100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);

    // when
    instance.spawnToken();
    instance.setState(ProcessInstanceIntent.ELEMENT_ACTIVATING);
    instance.setJobKey(5);

    // then
    final ElementInstance updatedInstance = elementInstanceState.getInstance(100);

    Assertions.assertThat(updatedInstance.getKey()).isEqualTo(100);
    Assertions.assertThat(updatedInstance.getState())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED);
    Assertions.assertThat(updatedInstance.getJobKey()).isEqualTo(0L);
    Assertions.assertThat(updatedInstance.canTerminate()).isTrue();

    Assertions.assertThat(updatedInstance.getNumberOfActiveElementInstances()).isEqualTo(0);
    Assertions.assertThat(updatedInstance.getNumberOfActiveExecutionPaths()).isEqualTo(0);
    Assertions.assertThat(updatedInstance.getNumberOfActiveTokens()).isEqualTo(0);

    final ProcessInstanceRecord record = updatedInstance.getValue();
    assertProcessInstanceRecord(record);
  }

  @Test
  public void shouldNotUpdateElementInstanceWithoutFlush() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    final ElementInstance instance =
        elementInstanceState.newInstance(
            100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);

    // when
    instance.spawnToken();
    instance.setState(ProcessInstanceIntent.ELEMENT_ACTIVATING);
    instance.setJobKey(5);

    // then
    final ElementInstance oldInstance = elementInstanceState.getInstance(100);

    assertElementInstance(oldInstance, 0);
  }

  @Test
  public void shouldCollectChildInstances() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    final ElementInstance parentInstance =
        elementInstanceState.newInstance(
            100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);
    processInstanceRecord.setElementId("subProcess");
    elementInstanceState.newInstance(
        parentInstance, 101, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATING);
    processInstanceRecord.setElementId("subProcess2");
    elementInstanceState.newInstance(
        parentInstance, 102, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATING);

    // when
    final List<ElementInstance> children = elementInstanceState.getChildren(100L);

    // then
    Assertions.assertThat(children).hasSize(2);
    assertChildInstance(children.get(0), 101, "subProcess");
    assertChildInstance(children.get(1), 102, "subProcess2");
  }

  @Test
  public void shouldStoreAndCollectRecord() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    elementInstanceState.newInstance(
        100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);

    // when
    elementInstanceState.storeRecord(
        123L,
        100,
        createProcessInstanceRecord(),
        ProcessInstanceIntent.ELEMENT_ACTIVATED,
        Purpose.DEFERRED);

    // then
    final List<IndexedRecord> storedRecords = elementInstanceState.getDeferredRecords(100);

    Assertions.assertThat(storedRecords).hasSize(1);
    final IndexedRecord indexedRecord = storedRecords.get(0);
    Assertions.assertThat(indexedRecord.getKey()).isEqualTo(123L);
    Assertions.assertThat(indexedRecord.getState())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED);
    assertProcessInstanceRecord(indexedRecord.getValue());
  }

  @Test
  public void shouldRemoveSingleRecord() {
    // given
    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    elementInstanceState.newInstance(
        100, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);
    elementInstanceState.storeRecord(
        123L,
        100,
        createProcessInstanceRecord(),
        ProcessInstanceIntent.ELEMENT_ACTIVATED,
        Purpose.DEFERRED);
    elementInstanceState.storeRecord(
        124L,
        100,
        createProcessInstanceRecord(),
        ProcessInstanceIntent.ELEMENT_ACTIVATED,
        Purpose.DEFERRED);

    // when
    elementInstanceState.removeStoredRecord(100, 123, Purpose.DEFERRED);

    // then
    final List<IndexedRecord> storedRecords = elementInstanceState.getDeferredRecords(100);

    Assertions.assertThat(storedRecords).hasSize(1);
    final IndexedRecord indexedRecord = storedRecords.get(0);
    Assertions.assertThat(indexedRecord.getKey()).isEqualTo(124L);
    Assertions.assertThat(indexedRecord.getState())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED);
    assertProcessInstanceRecord(indexedRecord.getValue());
  }

  @Test
  public void shouldRemoveStoredRecordsOnInstanceRemoval() {
    // given
    final int key = 100;

    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    elementInstanceState.newInstance(
        key, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);

    elementInstanceState.storeRecord(
        123L,
        100,
        createProcessInstanceRecord(),
        ProcessInstanceIntent.ELEMENT_ACTIVATED,
        Purpose.DEFERRED);

    // when
    elementInstanceState.removeInstance(key);

    // then
    Assertions.assertThat(elementInstanceState.getDeferredRecords(key)).isEmpty();
  }

  @Test
  public void shouldNotLeakMemoryOnRemoval() {
    // given
    final int parent = 100;
    final int child = 101;

    final ProcessInstanceRecord processInstanceRecord = createProcessInstanceRecord();
    final ElementInstance parentInstance =
        elementInstanceState.newInstance(
            parent, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATED);
    setVariableLocal(parent, BufferUtil.wrapString("a"), MsgPackUtil.asMsgPack("1"));

    processInstanceRecord.setElementId("subProcess");
    elementInstanceState.newInstance(
        parentInstance, child, processInstanceRecord, ProcessInstanceIntent.ELEMENT_ACTIVATING);
    setVariableLocal(child, BufferUtil.wrapString("b"), MsgPackUtil.asMsgPack("2"));

    // when
    elementInstanceState.removeInstance(101);
    elementInstanceState.removeInstance(100);

    // then
    final var nonEmptyColumns =
        Arrays.stream(ZbColumnFamilies.values())
            .filter(not(ZbColumnFamilies.KEY::equals))
            .filter(not(zeebeState::isEmpty))
            .collect(Collectors.toList());

    assertThat(nonEmptyColumns).describedAs("Expected all columns to be empty").isEmpty();
  }

  @Test
  public void shouldUpdateAwaitResultMetadata() {
    final long key = 10L;
    final int streamId = 2;
    final long requestId = 10000L;

    // when
    elementInstanceState.setAwaitResultRequestMetadata(
        key,
        new AwaitProcessInstanceResultMetadata()
            .setRequestStreamId(streamId)
            .setRequestId(requestId));
    final AwaitProcessInstanceResultMetadata metadata =
        elementInstanceState.getAwaitResultRequestMetadata(key);

    // then
    assertThat(metadata.getRequestId()).isEqualTo(requestId);
    assertThat(metadata.getRequestStreamId()).isEqualTo(streamId);
  }

  private void assertElementInstance(final ElementInstance elementInstance, final int childCount) {
    Assertions.assertThat(elementInstance.getKey()).isEqualTo(100);
    Assertions.assertThat(elementInstance.getState())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED);
    Assertions.assertThat(elementInstance.getJobKey()).isEqualTo(0);
    Assertions.assertThat(elementInstance.canTerminate()).isTrue();

    Assertions.assertThat(elementInstance.getNumberOfActiveElementInstances())
        .isEqualTo(childCount);
    Assertions.assertThat(elementInstance.getNumberOfActiveExecutionPaths()).isEqualTo(childCount);
    Assertions.assertThat(elementInstance.getNumberOfActiveTokens()).isEqualTo(0);

    final ProcessInstanceRecord record = elementInstance.getValue();

    assertProcessInstanceRecord(record);
  }

  private void assertChildInstance(
      final ElementInstance childInstance, final long key, final String elementId) {
    Assertions.assertThat(childInstance.getKey()).isEqualTo(key);
    Assertions.assertThat(childInstance.getState())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATING);
    Assertions.assertThat(childInstance.getJobKey()).isEqualTo(0);
    Assertions.assertThat(childInstance.canTerminate()).isTrue();

    Assertions.assertThat(childInstance.getNumberOfActiveElementInstances()).isEqualTo(0);
    Assertions.assertThat(childInstance.getNumberOfActiveExecutionPaths()).isEqualTo(0);
    Assertions.assertThat(childInstance.getNumberOfActiveTokens()).isEqualTo(0);

    assertProcessInstanceRecord(childInstance.getValue(), wrapString(elementId));
  }

  private ProcessInstanceRecord createProcessInstanceRecord() {
    final ProcessInstanceRecord processInstanceRecord = new ProcessInstanceRecord();
    processInstanceRecord.setElementId("startEvent");
    processInstanceRecord.setBpmnProcessId(wrapString("process1"));
    processInstanceRecord.setProcessInstanceKey(1000L);
    processInstanceRecord.setFlowScopeKey(1001L);
    processInstanceRecord.setVersion(1);
    processInstanceRecord.setProcessKey(2);
    processInstanceRecord.setBpmnElementType(BpmnElementType.START_EVENT);

    return processInstanceRecord;
  }

  private void assertProcessInstanceRecord(final ProcessInstanceRecord record) {
    assertProcessInstanceRecord(record, wrapString("startEvent"));
  }

  private void assertProcessInstanceRecord(
      final ProcessInstanceRecord record, final DirectBuffer elementId) {
    assertThat(record.getElementIdBuffer()).isEqualTo(elementId);
    assertThat(record.getBpmnProcessIdBuffer()).isEqualTo(wrapString("process1"));
    assertThat(record.getProcessInstanceKey()).isEqualTo(1000L);
    assertThat(record.getFlowScopeKey()).isEqualTo(1001L);
    assertThat(record.getVersion()).isEqualTo(1);
    assertThat(record.getProcessKey()).isEqualTo(2);
    assertThat(record.getBpmnElementType()).isEqualTo(BpmnElementType.START_EVENT);
  }

  public void setVariableLocal(
      final long scopeKey, final DirectBuffer name, final DirectBuffer value) {
    zeebeState
        .getVariableState()
        .setVariableLocal(
            scopeKey, PROCESS_KEY, name, 0, name.capacity(), value, 0, value.capacity());
  }
}
