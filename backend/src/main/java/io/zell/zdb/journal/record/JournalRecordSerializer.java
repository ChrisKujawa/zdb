/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zell.zdb.journal.record;

import org.agrona.DirectBuffer;

public interface JournalRecordSerializer {

  /**
   * Returns the number of bytes required to write a {@link RecordMetadata} to a buffer.
   * @return the expected length of a serialized metadata
   */
  int getMetadataLength();

  /**
   * Reads the {@link RecordMetadata} from the buffer at offset 0. A valid record must exist in the
   * buffer at this position.
   *
   * @param buffer to read
   * @param offset the offset in the buffer at which the metadata will be read from
   * @return a journal record metadata that is read.
   */
  RecordMetadata readMetadata(DirectBuffer buffer, int offset);

  /**
   * Reads the {@link RecordData} from the buffer at offset 0. A valid record must exist in the
   * buffer at this position.
   *
   * @param buffer to read
   * @param offset the offset in the buffer at which the data will be read from
   * @return a journal indexed record that is read.
   */
  RecordData readData(DirectBuffer buffer, int offset);

  /**
   * Returns the length of the serialized {@link RecordMetadata} in the buffer.
   *
   * @param buffer to read
   * @param offset the offset in the buffer at which the metadata will be read from
   * @return the length of the metadata
   */
  int getMetadataLength(DirectBuffer buffer, int offset);
}
