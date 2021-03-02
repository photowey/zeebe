/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.processing.bpmn.behavior;

import io.zeebe.engine.metrics.ProcessEngineMetrics;
import io.zeebe.engine.processing.bpmn.BpmnElementContainerProcessor;
import io.zeebe.engine.processing.bpmn.ProcessInstanceStateTransitionGuard;
import io.zeebe.engine.processing.common.CatchEventBehavior;
import io.zeebe.engine.processing.common.ExpressionProcessor;
import io.zeebe.engine.processing.deployment.model.element.ExecutableFlowElement;
import io.zeebe.engine.processing.streamprocessor.sideeffect.SideEffects;
import io.zeebe.engine.processing.streamprocessor.writers.TypedCommandWriter;
import io.zeebe.engine.processing.streamprocessor.writers.TypedResponseWriter;
import io.zeebe.engine.processing.streamprocessor.writers.TypedStreamWriter;
import io.zeebe.engine.processing.streamprocessor.writers.Writers;
import io.zeebe.engine.state.ZeebeState;
import io.zeebe.protocol.record.value.BpmnElementType;
import java.util.function.Function;

public final class BpmnBehaviorsImpl implements BpmnBehaviors {

  private final ExpressionProcessor expressionBehavior;
  private final BpmnVariableMappingBehavior variableMappingBehavior;
  private final BpmnEventPublicationBehavior eventPublicationBehavior;
  private final BpmnEventSubscriptionBehavior eventSubscriptionBehavior;
  private final BpmnIncidentBehavior incidentBehavior;
  private final BpmnStateBehavior stateBehavior;
  private final BpmnStateTransitionBehavior stateTransitionBehavior;
  private final BpmnDeferredRecordsBehavior deferredRecordsBehavior;
  private final ProcessInstanceStateTransitionGuard stateTransitionGuard;
  private final TypedStreamWriter streamWriter;
  private final BpmnProcessResultSenderBehavior processResultSenderBehavior;
  private final BpmnBufferedMessageStartEventBehavior bufferedMessageStartEventBehavior;

  public BpmnBehaviorsImpl(
      final ExpressionProcessor expressionBehavior,
      final TypedStreamWriter streamWriter,
      final TypedResponseWriter responseWriter,
      final SideEffects sideEffects,
      final ZeebeState zeebeState,
      final CatchEventBehavior catchEventBehavior,
      final Function<BpmnElementType, BpmnElementContainerProcessor<ExecutableFlowElement>>
          processorLookup,
      final Writers writers) {

    this.streamWriter = streamWriter;
    this.expressionBehavior = expressionBehavior;

    stateBehavior = new BpmnStateBehavior(zeebeState);
    stateTransitionGuard = new ProcessInstanceStateTransitionGuard(stateBehavior);
    variableMappingBehavior = new BpmnVariableMappingBehavior(expressionBehavior, zeebeState);
    stateTransitionBehavior =
        new BpmnStateTransitionBehavior(
            streamWriter,
            zeebeState.getKeyGenerator(),
            stateBehavior,
            new ProcessEngineMetrics(zeebeState.getPartitionId()),
            stateTransitionGuard,
            processorLookup,
            writers);
    eventSubscriptionBehavior =
        new BpmnEventSubscriptionBehavior(
            stateBehavior,
            stateTransitionBehavior,
            catchEventBehavior,
            streamWriter,
            sideEffects,
            zeebeState);
    incidentBehavior = new BpmnIncidentBehavior(zeebeState, streamWriter);
    deferredRecordsBehavior = new BpmnDeferredRecordsBehavior(zeebeState);
    eventPublicationBehavior = new BpmnEventPublicationBehavior(zeebeState, streamWriter);
    processResultSenderBehavior = new BpmnProcessResultSenderBehavior(zeebeState, responseWriter);
    bufferedMessageStartEventBehavior =
        new BpmnBufferedMessageStartEventBehavior(zeebeState, streamWriter);
  }

  @Override
  public ExpressionProcessor expressionBehavior() {
    return expressionBehavior;
  }

  @Override
  public BpmnVariableMappingBehavior variableMappingBehavior() {
    return variableMappingBehavior;
  }

  @Override
  public BpmnEventPublicationBehavior eventPublicationBehavior() {
    return eventPublicationBehavior;
  }

  @Override
  public BpmnEventSubscriptionBehavior eventSubscriptionBehavior() {
    return eventSubscriptionBehavior;
  }

  @Override
  public BpmnIncidentBehavior incidentBehavior() {
    return incidentBehavior;
  }

  @Override
  public BpmnStateBehavior stateBehavior() {
    return stateBehavior;
  }

  @Override
  public TypedCommandWriter commandWriter() {
    return streamWriter;
  }

  @Override
  public BpmnStateTransitionBehavior stateTransitionBehavior() {
    return stateTransitionBehavior;
  }

  @Override
  public BpmnDeferredRecordsBehavior deferredRecordsBehavior() {
    return deferredRecordsBehavior;
  }

  @Override
  public ProcessInstanceStateTransitionGuard stateTransitionGuard() {
    return stateTransitionGuard;
  }

  @Override
  public BpmnProcessResultSenderBehavior processResultSenderBehavior() {
    return processResultSenderBehavior;
  }

  @Override
  public BpmnBufferedMessageStartEventBehavior bufferedMessageStartEventBehavior() {
    return bufferedMessageStartEventBehavior;
  }
}
