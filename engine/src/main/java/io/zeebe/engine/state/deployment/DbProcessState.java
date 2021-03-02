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
import io.zeebe.engine.processing.deployment.model.element.ExecutableProcess;
import io.zeebe.engine.processing.deployment.model.transformation.BpmnTransformer;
import io.zeebe.engine.state.NextValueManager;
import io.zeebe.engine.state.ZbColumnFamilies;
import io.zeebe.engine.state.mutable.MutableProcessState;
import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentRecord;
import io.zeebe.protocol.impl.record.value.deployment.DeploymentResource;
import io.zeebe.protocol.impl.record.value.deployment.ProcessRecord;
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

public final class DbProcessState implements MutableProcessState {

  private final BpmnTransformer transformer = BpmnFactory.createTransformer();

  private final Map<DirectBuffer, Long2ObjectHashMap<DeployedProcess>>
      processesByProcessIdAndVersion = new HashMap<>();
  private final Long2ObjectHashMap<DeployedProcess> processesByKey;

  // process
  private final ColumnFamily<DbLong, PersistedProcess> processColumnFamily;
  private final DbLong processKey;
  private final PersistedProcess persistedProcess;

  private final ColumnFamily<DbCompositeKey<DbString, DbLong>, PersistedProcess>
      processByIdAndVersionColumnFamily;
  private final DbLong processVersion;
  private final DbCompositeKey<DbString, DbLong> idAndVersionKey;

  private final ColumnFamily<DbString, LatestProcessVersion> latestProcessColumnFamily;
  private final DbString processId;
  private final LatestProcessVersion latestVersion = new LatestProcessVersion();

  private final ColumnFamily<DbString, Digest> digestByIdColumnFamily;
  private final Digest digest = new Digest();

  private final NextValueManager versionManager;

  public DbProcessState(
      final ZeebeDb<ZbColumnFamilies> zeebeDb, final TransactionContext transactionContext) {
    processKey = new DbLong();
    persistedProcess = new PersistedProcess();
    processColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.PROCESS_CACHE, transactionContext, processKey, persistedProcess);

    processId = new DbString();
    processVersion = new DbLong();
    idAndVersionKey = new DbCompositeKey<>(processId, processVersion);
    processByIdAndVersionColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.PROCESS_CACHE_BY_ID_AND_VERSION,
            transactionContext,
            idAndVersionKey,
            persistedProcess);

    latestProcessColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.PROCESS_CACHE_LATEST_KEY,
            transactionContext,
            processId,
            latestVersion);

    digestByIdColumnFamily =
        zeebeDb.createColumnFamily(
            ZbColumnFamilies.PROCESS_CACHE_DIGEST_BY_ID, transactionContext, processId, digest);

    processesByKey = new Long2ObjectHashMap<>();

    versionManager =
        new NextValueManager(zeebeDb, transactionContext, ZbColumnFamilies.PROCESS_VERSION);
  }

  @Override
  public void putDeployment(final DeploymentRecord deploymentRecord) {
    for (final ProcessRecord processRecord : deploymentRecord.processes()) {
      final long processKey = processRecord.getKey();
      final DirectBuffer resourceName = processRecord.getResourceNameBuffer();
      for (final DeploymentResource resource : deploymentRecord.resources()) {
        if (resource.getResourceNameBuffer().equals(resourceName)) {
          persistProcess(processKey, processRecord, resource);
          updateLatestVersion(processRecord);
        }
      }
    }
  }

  @Override
  public void putProcess(final long key, final ProcessRecord processRecord) {
    persistProcess(key, processRecord);
    updateLatestVersion(processRecord);
    putLatestVersionDigest(
        processRecord.getBpmnProcessIdBuffer(), processRecord.getChecksumBuffer());
  }

  private void persistProcess(final long processKey, final ProcessRecord processRecord) {
    persistedProcess.wrap(processRecord, processKey);
    this.processKey.wrapLong(processKey);
    processColumnFamily.put(this.processKey, persistedProcess);

    processId.wrapBuffer(processRecord.getBpmnProcessIdBuffer());
    processVersion.wrapLong(processRecord.getVersion());

    processByIdAndVersionColumnFamily.put(idAndVersionKey, persistedProcess);
  }

  @Deprecated
  private void persistProcess(
      final long processKey,
      final ProcessRecord processRecord,
      final DeploymentResource resource) {
    persistedProcess.wrap(resource, processRecord, processKey);
    this.processKey.wrapLong(processKey);
    processColumnFamily.put(this.processKey, persistedProcess);

    processId.wrapBuffer(processRecord.getBpmnProcessIdBuffer());
    processVersion.wrapLong(processRecord.getVersion());

    processByIdAndVersionColumnFamily.put(idAndVersionKey, persistedProcess);
  }

  private void updateLatestVersion(final ProcessRecord processRecord) {
    processId.wrapBuffer(processRecord.getBpmnProcessIdBuffer());
    final LatestProcessVersion storedVersion = latestProcessColumnFamily.get(processId);
    final long latestVersion = storedVersion == null ? -1 : storedVersion.get();

    if (processRecord.getVersion() > latestVersion) {
      this.latestVersion.set(processRecord.getVersion());
      latestProcessColumnFamily.put(processId, this.latestVersion);
    }
  }

  // is called on getters, if process is not in memory
  private DeployedProcess updateInMemoryState(final PersistedProcess persistedProcess) {

    // we have to copy to store this in cache
    final byte[] bytes = new byte[persistedProcess.getLength()];
    final MutableDirectBuffer buffer = new UnsafeBuffer(bytes);
    persistedProcess.write(buffer, 0);

    final PersistedProcess copiedProcess = new PersistedProcess();
    copiedProcess.wrap(buffer, 0, persistedProcess.getLength());

    final BpmnModelInstance modelInstance =
        readModelInstanceFromBuffer(copiedProcess.getResource());
    final List<ExecutableProcess> definitions = transformer.transformDefinitions(modelInstance);

    final ExecutableProcess executableProcess =
        definitions.stream()
            .filter((w) -> BufferUtil.equals(persistedProcess.getBpmnProcessId(), w.getId()))
            .findFirst()
            .get();

    final DeployedProcess deployedProcess =
        new DeployedProcess(executableProcess, copiedProcess);

    addProcessToInMemoryState(deployedProcess);

    return deployedProcess;
  }

  private BpmnModelInstance readModelInstanceFromBuffer(final DirectBuffer buffer) {
    try (final DirectBufferInputStream stream = new DirectBufferInputStream(buffer)) {
      return Bpmn.readModelFromStream(stream);
    }
  }

  private void addProcessToInMemoryState(final DeployedProcess deployedProcess) {
    final DirectBuffer bpmnProcessId = deployedProcess.getBpmnProcessId();
    processesByKey.put(deployedProcess.getKey(), deployedProcess);

    Long2ObjectHashMap<DeployedProcess> versionMap =
        processesByProcessIdAndVersion.get(bpmnProcessId);

    if (versionMap == null) {
      versionMap = new Long2ObjectHashMap<>();
      processesByProcessIdAndVersion.put(bpmnProcessId, versionMap);
    }

    final int version = deployedProcess.getVersion();
    versionMap.put(version, deployedProcess);
  }

  @Override
  public DeployedProcess getLatestProcessVersionByProcessId(final DirectBuffer processId) {
    final Long2ObjectHashMap<DeployedProcess> versionMap =
        processesByProcessIdAndVersion.get(processId);

    processId.wrapBuffer(processId);
    final LatestProcessVersion latestVersion = latestProcessColumnFamily.get(processId);

    DeployedProcess deployedProcess;
    if (versionMap == null) {
      deployedProcess = lookupProcessByIdAndPersistedVersion(latestVersion);
    } else {
      deployedProcess = versionMap.get(latestVersion.get());
      if (deployedProcess == null) {
        deployedProcess = lookupProcessByIdAndPersistedVersion(latestVersion);
      }
    }
    return deployedProcess;
  }

  private DeployedProcess lookupProcessByIdAndPersistedVersion(
      final LatestProcessVersion version) {
    final long latestVersion = version != null ? version.get() : -1;
    processVersion.wrapLong(latestVersion);

    final PersistedProcess persistedProcess =
        processByIdAndVersionColumnFamily.get(idAndVersionKey);

    if (persistedProcess != null) {
      final DeployedProcess deployedProcess = updateInMemoryState(persistedProcess);
      return deployedProcess;
    }
    return null;
  }

  @Override
  public DeployedProcess getProcessByProcessIdAndVersion(
      final DirectBuffer processId, final int version) {
    final Long2ObjectHashMap<DeployedProcess> versionMap =
        processesByProcessIdAndVersion.get(processId);

    if (versionMap != null) {
      final DeployedProcess deployedProcess = versionMap.get(version);
      return deployedProcess != null
          ? deployedProcess
          : lookupPersistenceState(processId, version);
    } else {
      return lookupPersistenceState(processId, version);
    }
  }

  private DeployedProcess lookupPersistenceState(final DirectBuffer processId, final int version) {
    processId.wrapBuffer(processId);
    processVersion.wrapLong(version);

    final PersistedProcess persistedProcess =
        processByIdAndVersionColumnFamily.get(idAndVersionKey);

    if (persistedProcess != null) {
      updateInMemoryState(persistedProcess);

      final Long2ObjectHashMap<DeployedProcess> newVersionMap =
          processesByProcessIdAndVersion.get(processId);

      if (newVersionMap != null) {
        return newVersionMap.get(version);
      }
    }
    // does not exist in persistence and in memory state
    return null;
  }

  @Override
  public DeployedProcess getProcessByKey(final long key) {
    final DeployedProcess deployedProcess = processesByKey.get(key);

    if (deployedProcess != null) {
      return deployedProcess;
    } else {
      return lookupPersistenceStateForProcessByKey(key);
    }
  }

  private DeployedProcess lookupPersistenceStateForProcessByKey(final long processKey) {
    this.processKey.wrapLong(processKey);

    final PersistedProcess persistedProcess = processColumnFamily.get(this.processKey);
    if (persistedProcess != null) {
      updateInMemoryState(persistedProcess);

      final DeployedProcess deployedProcess = processesByKey.get(processKey);
      return deployedProcess;
    }
    // does not exist in persistence and in memory state
    return null;
  }

  @Override
  public Collection<DeployedProcess> getProcesses() {
    updateCompleteInMemoryState();
    return processesByKey.values();
  }

  @Override
  public Collection<DeployedProcess> getProcessesByBpmnProcessId(
      final DirectBuffer bpmnProcessId) {
    updateCompleteInMemoryState();

    final Long2ObjectHashMap<DeployedProcess> processesByVersions =
        processesByProcessIdAndVersion.get(bpmnProcessId);

    if (processesByVersions != null) {
      return processesByVersions.values();
    }
    return Collections.emptyList();
  }

  private void updateCompleteInMemoryState() {
    processColumnFamily.forEach((process) -> updateInMemoryState(persistedProcess));
  }

  @Override
  public void putLatestVersionDigest(final DirectBuffer processId, final DirectBuffer digest) {
    processId.wrapBuffer(processId);
    this.digest.set(digest);

    digestByIdColumnFamily.put(processId, this.digest);
  }

  @Override
  public DirectBuffer getLatestVersionDigest(final DirectBuffer processId) {
    processId.wrapBuffer(processId);
    final Digest latestDigest = digestByIdColumnFamily.get(processId);
    return latestDigest == null || digest.get().byteArray() == null ? null : latestDigest.get();
  }

  @Override
  public int incrementAndGetProcessVersion(final String bpmnProcessId) {
    return (int) versionManager.getNextValue(bpmnProcessId);
  }

  @Override
  public <T extends ExecutableFlowElement> T getFlowElement(
      final long processKey, final DirectBuffer elementId, final Class<T> elementType) {

    final var deployedProcess = getProcessByKey(processKey);
    if (deployedProcess == null) {
      throw new IllegalStateException(
          String.format(
              "Expected to find a process deployed with key '%d' but not found.", processKey));
    }

    final var process = deployedProcess.getProcess();
    final var element = process.getElementById(elementId, elementType);
    if (element == null) {
      throw new IllegalStateException(
          String.format(
              "Expected to find a flow element with id '%s' in process with key '%d' but not found.",
              bufferAsString(elementId), processKey));
    }

    return element;
  }
}
