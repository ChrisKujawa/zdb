/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
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
package io.zell.zdb.journal;

import org.agrona.DirectBuffer;

public interface ReadOnlyJournalRecord {

  /**
   * Index of the record
   *
   * @return index
   */
  long index();

  /**
   * Application sequence number for the record
   *
   * @return asqn
   */
  long asqn();

  /**
   * Checksum of the serializedRecord
   *
   * @return checksum
   */
  long checksum();

  /**
   * Application provided data of the record
   *
   * @return data
   */
  DirectBuffer data();

  /**
   * Serialized journal record that includes index, asqn and data.
   *
   * @return serialized record
   */
  DirectBuffer serializedRecord();
}
