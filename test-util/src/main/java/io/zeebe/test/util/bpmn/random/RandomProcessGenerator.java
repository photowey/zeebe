/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.0. You may not use this file
 * except in compliance with the Zeebe Community License 1.0.
 */
package io.zeebe.test.util.bpmn.random;

import io.zeebe.model.bpmn.Bpmn;
import io.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.test.util.bpmn.random.blocks.BlockSequenceBuilder.BlockSequenceBuilderFactory;
import io.zeebe.test.util.bpmn.random.blocks.ProcessBuilder;
import java.io.File;
import java.util.Optional;
import java.util.Random;

/** Class to generate random processes and execution paths for those processes */
public final class RandomProcessGenerator {

  private static final BlockSequenceBuilderFactory FACTORY = new BlockSequenceBuilderFactory();

  private final ProcessBuilder processBuilder;

  /**
   * Creates the random process generator
   *
   * @param seed seed for random noise generator
   * @param maxBlocks maximum number of blocks in a sequence (defaults to {@code 5})
   * @param maxDepth maximum level of depth for nested elements (defaults to {@code 3})
   * @param maxBranches maximum number of outgoing branches for a forking node (defaults to {@code
   *     3})
   */
  public RandomProcessGenerator(
      final long seed, final Integer maxBlocks, final Integer maxDepth, final Integer maxBranches) {
    final Random random = new Random(seed);

    final IDGenerator idGenerator = new IDGenerator(0);

    final ConstructionContext context =
        new ConstructionContext(
            random,
            idGenerator,
            FACTORY,
            Optional.ofNullable(maxBlocks).orElse(5),
            Optional.ofNullable(maxDepth).orElse(3),
            Optional.ofNullable(maxBranches).orElse(3),
            0);

    processBuilder = new ProcessBuilder(context);
  }

  public BpmnModelInstance buildProcess() {
    return processBuilder.buildProcess();
  }

  public ExecutionPath findRandomExecutionPath(final long seed) {
    return processBuilder.findRandomExecutionPath(new Random(seed));
  }

  // main method to test and debug this class
  public static void main(final String[] args) {
    final Random random = new Random();

    for (int i = 0; i < 10; i++) {
      System.out.println("Generating process " + i);

      final String id = "process" + i;

      final RandomProcessGenerator builder = new RandomProcessGenerator(random.nextLong(), 5, 3, 3);

      Bpmn.writeModelToFile(new File(id + ".bpmn"), builder.buildProcess());

      for (int p = 0; p < 5; p++) {
        final ExecutionPath path = builder.findRandomExecutionPath(random.nextLong());

        System.out.println("Execution path " + p + " :" + path);
      }
    }
  }
}
