/*
 * Copyright © 2021 Christopher Kujawa (zelldon91@gmail.com)
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
package io.zell.zdb.journal.record;

import io.camunda.zeebe.journal.CorruptedJournalException;
import io.camunda.zeebe.journal.file.*;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/** The serializer that writes and reads a journal record according to the SBE schema defined. */
public final class SBESerializer implements JournalRecordSerializer {
  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final RecordMetadataEncoder metadataEncoder = new RecordMetadataEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final RecordMetadataDecoder metadataDecoder = new RecordMetadataDecoder();
  private final RecordDataDecoder recordDecoder = new RecordDataDecoder();

  @Override
  public int getMetadataLength() {
    return headerEncoder.encodedLength() + metadataEncoder.sbeBlockLength();
  }

  @Override
  public RecordMetadata readMetadata(final DirectBuffer buffer, final int offset) {
    if (!hasMetadata(buffer, offset)) {
      throw new CorruptedJournalException("Cannot read metadata. Header does not match.");
    }
    metadataDecoder.wrap(
        buffer,
        offset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    return new RecordMetadata(metadataDecoder.checksum(), metadataDecoder.length());
  }

  @Override
  public RecordData readData(final DirectBuffer buffer, final int offset) {
    headerDecoder.wrap(buffer, offset);
    if (headerDecoder.schemaId() != recordDecoder.sbeSchemaId()
        || headerDecoder.templateId() != recordDecoder.sbeTemplateId()) {
      throw new CorruptedJournalException("Cannot read record. Header does not match.");
    }
    recordDecoder.wrap(
        buffer,
        offset + headerDecoder.encodedLength(),
        headerDecoder.blockLength(),
        headerDecoder.version());

    final DirectBuffer data = new UnsafeBuffer();
    recordDecoder.wrapData(data);
    return new RecordData(recordDecoder.index(), recordDecoder.asqn(), data);
  }

  @Override
  public int getMetadataLength(final DirectBuffer buffer, final int offset) {
    headerDecoder.wrap(buffer, offset);
    return headerDecoder.encodedLength() + headerDecoder.blockLength();
  }

  private boolean hasMetadata(final DirectBuffer buffer, final int offset) {
    headerDecoder.wrap(buffer, offset);
    return (headerDecoder.schemaId() == metadataDecoder.sbeSchemaId()
        && headerDecoder.templateId() == metadataDecoder.sbeTemplateId());
  }
}
