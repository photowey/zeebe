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

import io.zeebe.journal.JournalRecord;
import java.util.Objects;
import org.agrona.DirectBuffer;

public class TestJournalRecord implements JournalRecord {

  private final long index;
  private final long asqn;
  private final long checksum;
  private final DirectBuffer data;

  public TestJournalRecord(
      final long index, final long asqn, final long checksum, final DirectBuffer data) {
    this.index = index;
    this.asqn = asqn;
    this.checksum = checksum;
    this.data = data;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public long asqn() {
    return asqn;
  }

  @Override
  public long checksum() {
    return checksum;
  }

  @Override
  public DirectBuffer data() {
    return data;
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, asqn, checksum, data);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final JournalRecord that = (JournalRecord) o;
    return index == that.index()
        && asqn == that.asqn()
        && checksum == that.checksum()
        && Objects.equals(data, that.data());
  }
}
