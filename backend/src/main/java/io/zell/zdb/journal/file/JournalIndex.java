/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
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
package io.zell.zdb.journal.file;


import io.zell.zdb.journal.ReadOnlyJournalRecord;

/**
 * JournalIndex that indexes record's index, position and asqn. JournalReader may use this to
 * optimize seek.
 */
interface JournalIndex {

  /**
   * Indexes the record and its position with in a segment
   *
   * @param record the record that should be indexed
   * @param position the position of the given index
   */
  void index(ReadOnlyJournalRecord record, int position);

  /**
   * Looks up the position of the given index.
   *
   * @param index the index to lookup
   * @return the position of the given index or a lesser index
   */
  IndexInfo lookup(long index);

  /**
   * Look up the index for the given application sequence number. Same as {code lookupAsqn(asqn)},
   * but the returned index will be less than or equal to the given indexUpperBound.
   *
   * @param asqn asqn to lookup
   * @param indexUpperBound the upper bound of the index that will be returned
   * @return the index (<= indexUpperBound) of a record with asqn less than or equal to the given
   *     asqn.
   */
  Long lookupAsqn(long asqn, long indexUpperBound);

  /**
   * Checks if the entry at this index might have been already indexed. Note that the result is
   * probabilistic. If it returns true, it does not mean the lookup return exact index. If it
   * returns false, it is likely that calling {@link JournalIndex#index(ReadOnlyJournalRecord, int)} with
   * the entry at this index is useful.
   *
   * @param index
   * @return true if this index likely have been already indexed. false if otherwise.
   */
  boolean hasIndexed(long index);
}
