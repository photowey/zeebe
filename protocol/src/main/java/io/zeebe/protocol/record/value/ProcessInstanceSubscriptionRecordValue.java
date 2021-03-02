/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.protocol.record.value;

import io.zeebe.protocol.record.RecordValueWithVariables;
import io.zeebe.protocol.record.intent.WorkflowInstanceSubscriptionIntent;

/**
 * Represents a workflow instance subscription command or event.
 *
 * <p>See {@link WorkflowInstanceSubscriptionIntent} for intents.
 */
public interface WorkflowInstanceSubscriptionRecordValue
    extends RecordValueWithVariables, WorkflowInstanceRelated {
  /** @return the workflow instance key */
  long getWorkflowInstanceKey();

  /** @return the element instance key */
  long getElementInstanceKey();

  /** @return the BPMN process id */
  String getBpmnProcessId();

  /** @return the key of the correlated message */
  long getMessageKey();

  /** @return the message name */
  String getMessageName();

  /** @return the correlation key */
  String getCorrelationKey();
}
