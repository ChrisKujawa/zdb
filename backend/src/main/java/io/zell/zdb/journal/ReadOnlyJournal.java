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


public interface ReadOnlyJournal extends AutoCloseable {
  /**
   * Returns the index of last record in the journal
   *
   * @return the last index
   */
  long getLastIndex();

  /**
   * Returns the index of the first record.
   *
   * @return the first index
   */
  long getFirstIndex();

  /**
   * Check if the journal is empty.
   *
   * @return true if empty, false otherwise.
   */
  boolean isEmpty();

  /**
   * Opens a new {@link JournalReader}
   *
   * @return a journal reader
   */
  JournalReader openReader();

  /**
   * Check if the journal is open
   *
   * @return true if open, false otherwise
   */
  boolean isOpen();
}
