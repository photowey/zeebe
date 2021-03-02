/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.protocol.impl.record.value.message;

import static io.zeebe.util.buffer.BufferUtil.bufferAsString;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.zeebe.msgpack.property.BooleanProperty;
import io.zeebe.msgpack.property.DocumentProperty;
import io.zeebe.msgpack.property.LongProperty;
import io.zeebe.msgpack.property.StringProperty;
import io.zeebe.protocol.impl.encoding.MsgPackConverter;
import io.zeebe.protocol.impl.record.UnifiedRecordValue;
import io.zeebe.protocol.record.value.MessageSubscriptionRecordValue;
import java.util.Map;
import org.agrona.DirectBuffer;

public final class MessageSubscriptionRecord extends UnifiedRecordValue
    implements MessageSubscriptionRecordValue {

  private final LongProperty processInstanceKeyProp = new LongProperty("processInstanceKey");
  private final LongProperty elementInstanceKeyProp = new LongProperty("elementInstanceKey");
  private final StringProperty bpmnProcessIdProp = new StringProperty("bpmnProcessId", "");
  private final LongProperty messageKeyProp = new LongProperty("messageKey", -1L);
  private final StringProperty messageNameProp = new StringProperty("messageName", "");
  private final StringProperty correlationKeyProp = new StringProperty("correlationKey", "");
  private final BooleanProperty closeOnCorrelateProp =
      new BooleanProperty("closeOnCorrelate", true);

  private final DocumentProperty variablesProp = new DocumentProperty("variables");

  public MessageSubscriptionRecord() {
    declareProperty(processInstanceKeyProp)
        .declareProperty(elementInstanceKeyProp)
        .declareProperty(messageKeyProp)
        .declareProperty(messageNameProp)
        .declareProperty(correlationKeyProp)
        .declareProperty(closeOnCorrelateProp)
        .declareProperty(bpmnProcessIdProp)
        .declareProperty(variablesProp);
  }

  public boolean shouldCloseOnCorrelate() {
    return closeOnCorrelateProp.getValue();
  }

  @JsonIgnore
  public DirectBuffer getCorrelationKeyBuffer() {
    return correlationKeyProp.getValue();
  }

  @Override
  public long getElementInstanceKey() {
    return elementInstanceKeyProp.getValue();
  }

  public MessageSubscriptionRecord setElementInstanceKey(final long key) {
    elementInstanceKeyProp.setValue(key);
    return this;
  }

  @Override
  public String getBpmnProcessId() {
    return bufferAsString(bpmnProcessIdProp.getValue());
  }

  @Override
  public String getMessageName() {
    return bufferAsString(messageNameProp.getValue());
  }

  @Override
  public String getCorrelationKey() {
    return bufferAsString(correlationKeyProp.getValue());
  }

  public MessageSubscriptionRecord setCorrelationKey(final DirectBuffer correlationKey) {
    correlationKeyProp.setValue(correlationKey);
    return this;
  }

  @Override
  public long getMessageKey() {
    return messageKeyProp.getValue();
  }

  public MessageSubscriptionRecord setMessageKey(final long messageKey) {
    messageKeyProp.setValue(messageKey);
    return this;
  }

  public MessageSubscriptionRecord setMessageName(final DirectBuffer messageName) {
    messageNameProp.setValue(messageName);
    return this;
  }

  public MessageSubscriptionRecord setBpmnProcessId(final DirectBuffer bpmnProcessId) {
    bpmnProcessIdProp.setValue(bpmnProcessId);
    return this;
  }

  @JsonIgnore
  public DirectBuffer getMessageNameBuffer() {
    return messageNameProp.getValue();
  }

  @Override
  public long getProcessInstanceKey() {
    return processInstanceKeyProp.getValue();
  }

  public MessageSubscriptionRecord setProcessInstanceKey(final long key) {
    processInstanceKeyProp.setValue(key);
    return this;
  }

  public MessageSubscriptionRecord setCloseOnCorrelate(final boolean closeOnCorrelate) {
    closeOnCorrelateProp.setValue(closeOnCorrelate);
    return this;
  }

  @JsonIgnore
  public DirectBuffer getBpmnProcessIdBuffer() {
    return bpmnProcessIdProp.getValue();
  }

  @Override
  public Map<String, Object> getVariables() {
    return MsgPackConverter.convertToMap(variablesProp.getValue());
  }

  public MessageSubscriptionRecord setVariables(final DirectBuffer variables) {
    variablesProp.setValue(variables);
    return this;
  }

  @JsonIgnore
  public DirectBuffer getVariablesBuffer() {
    return variablesProp.getValue();
  }
}
