/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.engine.state.deployment;

import static io.zeebe.util.buffer.BufferUtil.bufferAsString;

import io.zeebe.db.ColumnFamily;
import io.zeebe.db.TransactionContext;
import io.zeebe.db.ZeebeDb;
import io.zeebe.db.impl.DbCompositeKey;
import io.zeebe.db.impl.DbLong;
import io.zeebe.db.impl.DbString;
import io.zeebe.engine.processing.deployment.model.BpmnFactory;
import io.zeebe.engine.processing.deployment.model.element.ExecutableFlowElement;
import io.zeebe.engine.processing.deployment.model.element.ExecutableWorkflow;
import io.zeebe.engine.processing.deployment.model.transformation.BpmnTransformer;
import io.zeebe.engine.state.NextValueManager;
import io.zeebe.engine.state.ZbColumnFamilies;
import io.zeebe.engine.state.mutable.MutableWorkflowState;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentResource;
import io.zeebe.protocol.impl.record.value.deployment.WorkflowRecord;
import io.zeebe.util.buffer.BufferUtil;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.io.DirectBufferInputStream;

public final class DbWorkflowState implements MutableWorkflowState {

  private final BpmnTransformer transformer = BpmnFactory.createTransformer();

  private final Map<DirectBuffer, Long2ObjectHashMap<DeployedWorkflow>>
      workflowsByProcessIdAndVersion = new HashMap<>();
  private final Long2ObjectHashMap<DeployedWorkflow> workflowsByKey;

  // workflow
  private final ColumnFamily<DbLong, PersistedWorkflow> workflowColumnFamily;
  private final DbLong workflowKey;
  private final PersistedWorkflow persistedWorkflow;

  private final ColumnFamily<DbCompositeKey<DbString, DbLong>, PersistedWorkflow>
      workflowByIdAndVersionColumnFamily;
  private final DbLong workflowVersion;
  private final DbCompositeKey<DbString, DbLong> idAndVersionKey;

  private final ColumnFamily<DbString, LatestWorkflowVersion> latestWorkflowColumnFamily;
  private final DbString workflowId;
  private final LatestWorkflowVersion latestVersion = new LatestWorkflowVersion();

  private final ColumnFamily<DbString, Digest> digestByIdColumnFamily;
  private final Digest digest = new Digest();

  private final NextValueManager versionManager;

  public DbWorkflowState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final TransactionContext transactionContext) {
    workflowKey = new DbLong();
    persistedWorkflow = new PersistedWorkflow();
    workflowColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.WORKFLOW_CACHE, transactionContext, workflowKey, persistedWorkflow);

    workflowId = new DbString();
    workflowVersion = new DbLong();
    idAndVersionKey = new DbCompositeKey<>(workflowId, workflowVersion);
    workflowByIdAndVersionColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.WORKFLOW_CACHE_BY_ID_AND_VERSION,
            transactionContext,
            idAndVersionKey,
            persistedWorkflow);

    latestWorkflowColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.WORKFLOW_CACHE_LATEST_KEY,
            transactionContext,
            workflowId,
            latestVersion);

    digestByIdColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.WORKFLOW_CACHE_DIGEST_BY_ID, transactionContext, workflowId, digest);

    workflowsByKey = new Long2ObjectHashMap<>();

    versionManager =
        new NextValueManager(zeebeDb, transactionContext, ZbColumnFamilies.WORKFLOW_VERSION);
  }

  @Override
  public void putDeployment(final DeploymentRecord deploymentRecord) {
    for (final WorkflowRecord workflowRecord : deploymentRecord.workflows()) {
      final long workflowKey = workflowRecord.getKey();
      final DirectBuffer resourceName = workflowRecord.getResourceNameBuffer();
      for (final DeploymentResource resource : deploymentRecord.resources()) {
        if (resource.getResourceNameBuffer().equals(resourceName)) {
          persistWorkflow(workflowKey, workflowRecord, resource);
          updateLatestVersion(workflowRecord);
        }
      }
    }
  }

  @Override
  public void putWorkflow(final long key, final WorkflowRecord workflowRecord) {
    persistWorkflow(key, workflowRecord);
    updateLatestVersion(workflowRecord);
    putLatestVersionDigest(
        workflowRecord.getBpmnProcessIdBuffer(), workflowRecord.getChecksumBuffer());
  }

  private void persistWorkflow(final long workflowKey, final WorkflowRecord workflowRecord) {
    persistedWorkflow.wrap(workflowRecord, workflowKey);
    this.workflowKey.wrapLong(workflowKey);
    workflowColumnFamily.put(this.workflowKey, persistedWorkflow);

    workflowId.wrapBuffer(workflowRecord.getBpmnProcessIdBuffer());
    workflowVersion.wrapLong(workflowRecord.getVersion());

    workflowByIdAndVersionColumnFamily.put(idAndVersionKey, persistedWorkflow);
  }

  @Deprecated
  private void persistWorkflow(
      final long workflowKey,
      final WorkflowRecord workflowRecord,
      final DeploymentResource resource) {
    persistedWorkflow.wrap(resource, workflowRecord, workflowKey);
    this.workflowKey.wrapLong(workflowKey);
    workflowColumnFamily.put(this.workflowKey, persistedWorkflow);

    workflowId.wrapBuffer(workflowRecord.getBpmnProcessIdBuffer());
    workflowVersion.wrapLong(workflowRecord.getVersion());

    workflowByIdAndVersionColumnFamily.put(idAndVersionKey, persistedWorkflow);
  }

  private void updateLatestVersion(final WorkflowRecord workflowRecord) {
    workflowId.wrapBuffer(workflowRecord.getBpmnProcessIdBuffer());
    final LatestWorkflowVersion storedVersion = latestWorkflowColumnFamily.get(workflowId);
    final long latestVersion = storedVersion == null ? -1 : storedVersion.get();

    if (workflowRecord.getVersion() > latestVersion) {
      this.latestVersion.set(workflowRecord.getVersion());
      latestWorkflowColumnFamily.put(workflowId, this.latestVersion);
    }
  }

  // is called on getters, if workflow is not in memory
  private DeployedWorkflow updateInMemoryState(final PersistedWorkflow persistedWorkflow) {

    // we have to copy to store this in cache
    final byte[] bytes = new byte[persistedWorkflow.getLength()];
    final MutableDirectBuffer buffer = new UnsafeBuffer(bytes);
    persistedWorkflow.write(buffer, 0);

    final PersistedWorkflow copiedWorkflow = new PersistedWorkflow();
    copiedWorkflow.wrap(buffer, 0, persistedWorkflow.getLength());

    final BpmnModelInstance modelInstance =
        readModelInstanceFromBuffer(copiedWorkflow.getResource());
    final List<ExecutableWorkflow> definitions = transformer.transformDefinitions(modelInstance);

    final ExecutableWorkflow executableWorkflow =
        definitions.stream()
            .filter((w) -> BufferUtil.equals(persistedWorkflow.getBpmnProcessId(), w.getId()))
            .findFirst()
            .get();

    final DeployedWorkflow deployedWorkflow =
        new DeployedWorkflow(executableWorkflow, copiedWorkflow);

    addWorkflowToInMemoryState(deployedWorkflow);

    return deployedWorkflow;
  }

  private BpmnModelInstance readModelInstanceFromBuffer(final DirectBuffer buffer) {
    try (final DirectBufferInputStream stream = new DirectBufferInputStream(buffer)) {
      return Bpmn.readModelFromStream(stream);
    }
  }

  private void addWorkflowToInMemoryState(final DeployedWorkflow deployedWorkflow) {
    final DirectBuffer bpmnProcessId = deployedWorkflow.getBpmnProcessId();
    workflowsByKey.put(deployedWorkflow.getKey(), deployedWorkflow);

    Long2ObjectHashMap<DeployedWorkflow> versionMap =
        workflowsByProcessIdAndVersion.get(bpmnProcessId);

    if (versionMap == null) {
      versionMap = new Long2ObjectHashMap<>();
      workflowsByProcessIdAndVersion.put(bpmnProcessId, versionMap);
    }

    final int version = deployedWorkflow.getVersion();
    versionMap.put(version, deployedWorkflow);
  }

  @Override
  public DeployedWorkflow getLatestWorkflowVersionByProcessId(final DirectBuffer processId) {
    final Long2ObjectHashMap<DeployedWorkflow> versionMap =
        workflowsByProcessIdAndVersion.get(processId);

    workflowId.wrapBuffer(processId);
    final LatestWorkflowVersion latestVersion = latestWorkflowColumnFamily.get(workflowId);

    DeployedWorkflow deployedWorkflow;
    if (versionMap == null) {
      deployedWorkflow = lookupWorkflowByIdAndPersistedVersion(latestVersion);
    } else {
      deployedWorkflow = versionMap.get(latestVersion.get());
      if (deployedWorkflow == null) {
        deployedWorkflow = lookupWorkflowByIdAndPersistedVersion(latestVersion);
      }
    }
    return deployedWorkflow;
  }

  private DeployedWorkflow lookupWorkflowByIdAndPersistedVersion(
      final LatestWorkflowVersion version) {
    final long latestVersion = version != null ? version.get() : -1;
    workflowVersion.wrapLong(latestVersion);

    final PersistedWorkflow persistedWorkflow =
        workflowByIdAndVersionColumnFamily.get(idAndVersionKey);

    if (persistedWorkflow != null) {
      final DeployedWorkflow deployedWorkflow = updateInMemoryState(persistedWorkflow);
      return deployedWorkflow;
    }
    return null;
  }

  @Override
  public DeployedWorkflow getWorkflowByProcessIdAndVersion(
      final DirectBuffer processId, final int version) {
    final Long2ObjectHashMap<DeployedWorkflow> versionMap =
        workflowsByProcessIdAndVersion.get(processId);

    if (versionMap != null) {
      final DeployedWorkflow deployedWorkflow = versionMap.get(version);
      return deployedWorkflow != null
          ? deployedWorkflow
          : lookupPersistenceState(processId, version);
    } else {
      return lookupPersistenceState(processId, version);
    }
  }

  private DeployedWorkflow lookupPersistenceState(final DirectBuffer processId, final int version) {
    workflowId.wrapBuffer(processId);
    workflowVersion.wrapLong(version);

    final PersistedWorkflow persistedWorkflow =
        workflowByIdAndVersionColumnFamily.get(idAndVersionKey);

    if (persistedWorkflow != null) {
      updateInMemoryState(persistedWorkflow);

      final Long2ObjectHashMap<DeployedWorkflow> newVersionMap =
          workflowsByProcessIdAndVersion.get(processId);

      if (newVersionMap != null) {
        return newVersionMap.get(version);
      }
    }
    // does not exist in persistence and in memory state
    return null;
  }

  @Override
  public DeployedWorkflow getWorkflowByKey(final long key) {
    final DeployedWorkflow deployedWorkflow = workflowsByKey.get(key);

    if (deployedWorkflow != null) {
      return deployedWorkflow;
    } else {
      return lookupPersistenceStateForWorkflowByKey(key);
    }
  }

  private DeployedWorkflow lookupPersistenceStateForWorkflowByKey(final long workflowKey) {
    this.workflowKey.wrapLong(workflowKey);

    final PersistedWorkflow persistedWorkflow = workflowColumnFamily.get(this.workflowKey);
    if (persistedWorkflow != null) {
      updateInMemoryState(persistedWorkflow);

      final DeployedWorkflow deployedWorkflow = workflowsByKey.get(workflowKey);
      return deployedWorkflow;
    }
    // does not exist in persistence and in memory state
    return null;
  }

  @Override
  public Collection<DeployedWorkflow> getWorkflows() {
    updateCompleteInMemoryState();
    return workflowsByKey.values();
  }

  @Override
  public Collection<DeployedWorkflow> getWorkflowsByBpmnProcessId(
      final DirectBuffer bpmnProcessId) {
    updateCompleteInMemoryState();

    final Long2ObjectHashMap<DeployedWorkflow> workflowsByVersions =
        workflowsByProcessIdAndVersion.get(bpmnProcessId);

    if (workflowsByVersions != null) {
      return workflowsByVersions.values();
    }
    return Collections.emptyList();
  }

  private void updateCompleteInMemoryState() {
    workflowColumnFamily.forEach((workflow) -> updateInMemoryState(persistedWorkflow));
  }

  @Override
  public void putLatestVersionDigest(final DirectBuffer processId, final DirectBuffer digest) {
    workflowId.wrapBuffer(processId);
    this.digest.set(digest);

    digestByIdColumnFamily.put(workflowId, this.digest);
  }

  @Override
  public DirectBuffer getLatestVersionDigest(final DirectBuffer processId) {
    workflowId.wrapBuffer(processId);
    final Digest latestDigest = digestByIdColumnFamily.get(workflowId);
    return latestDigest == null || digest.get().byteArray() == null ? null : latestDigest.get();
  }

  @Override
  public int incrementAndGetWorkflowVersion(final String bpmnProcessId) {
    return (int) versionManager.getNextValue(bpmnProcessId);
  }

  @Override
  public <T extends ExecutableFlowElement> T getFlowElement(
      final long workflowKey, final DirectBuffer elementId, final Class<T> elementType) {

    final var deployedWorkflow = getWorkflowByKey(workflowKey);
    if (deployedWorkflow == null) {
      throw new IllegalStateException(
          String.format(
              "Expected to find a workflow deployed with key '%d' but not found.", workflowKey));
    }

    final var workflow = deployedWorkflow.getWorkflow();
    final var element = workflow.getElementById(elementId, elementType);
    if (element == null) {
      throw new IllegalStateException(
          String.format(
              "Expected to find a flow element with id '%s' in workflow with key '%d' but not found.",
              bufferAsString(elementId), workflowKey));
    }

    return element;
  }
}
