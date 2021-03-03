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
package io.zeebe.journal.file;

import static org.assertj.core.api.Assertions.assertThat;

import io.zeebe.journal.JournalReader;
import io.zeebe.journal.file.record.KryoSerializer;
import io.zeebe.journal.file.record.RecordData;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SegmentedJournalReaderTest {

  private static final int ENTRIES_PER_SEGMENT = 2;

  @TempDir Path directory;

  private final DirectBuffer data = new UnsafeBuffer("test".getBytes(StandardCharsets.UTF_8));

  private JournalReader reader;
  private SegmentedJournal journal;

  @BeforeEach
  void setup() {
    final int entrySize = getSerializedSize(data);

    journal =
        SegmentedJournal.builder()
            .withDirectory(directory.resolve("data").toFile())
            .withMaxSegmentSize(entrySize * ENTRIES_PER_SEGMENT + JournalSegmentDescriptor.BYTES)
            .withMaxEntrySize(entrySize)
            .withJournalIndexDensity(5)
            .build();
    reader = journal.openReader();
  }

  @Test
  void shouldReadAfterCompact() {
    // given
    for (int i = 1; i <= ENTRIES_PER_SEGMENT * 5; i++) {
      assertThat(journal.append(i, data).index()).isEqualTo(i);
    }
    assertThat(reader.hasNext()).isTrue();

    // when - compact up to the first index of segment 3
    final int indexToCompact = ENTRIES_PER_SEGMENT * 2 + 1;
    journal.deleteUntil(indexToCompact);

    // then
    assertThat(reader.hasNext()).isTrue();
    assertThat(reader.next().index()).isEqualTo(indexToCompact);
  }

  @Test
  void shouldSeekToAnyIndexInMultipleSegments() {
    // given
    long asqn = 1;

    for (int i = 1; i <= ENTRIES_PER_SEGMENT * 2; i++) {
      journal.append(asqn++, data).index();
    }

    for (int i = 1; i < ENTRIES_PER_SEGMENT * 2; i++) {
      // when
      reader.seek(i);

      // then
      assertThat(reader.hasNext()).isTrue();
      assertThat(reader.next().index()).isEqualTo(i);
    }
  }

  @Test
  void shouldSeekToAnyAsqnInMultipleSegments() {
    // given

    for (int i = 1; i <= ENTRIES_PER_SEGMENT * 2; i++) {
      journal.append(i, data).index();
    }

    for (int i = 1; i <= ENTRIES_PER_SEGMENT * 2; i++) {
      // when
      reader.seekToAsqn(i);

      // then
      assertThat(reader.hasNext()).isTrue();
      assertThat(reader.next().asqn()).isEqualTo(i);
    }
  }

  private int getSerializedSize(final DirectBuffer data) {
    final var record = new RecordData(Long.MAX_VALUE, Long.MAX_VALUE, data);
    final var serializer = new KryoSerializer();
    final ByteBuffer buffer = ByteBuffer.allocate(128);
    return serializer.writeData(record, new UnsafeBuffer(buffer), 0)
        + serializer.getMetadataLength();
  }
}
