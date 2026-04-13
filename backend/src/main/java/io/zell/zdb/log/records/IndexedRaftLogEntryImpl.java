/*
 * Copyright 2017-present Open Networking Foundation
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
package io.zell.zdb.log.records;

import io.atomix.raft.protocol.PersistedRaftRecord;
import io.atomix.raft.storage.log.entry.ApplicationEntry;
import io.atomix.raft.storage.log.entry.RaftEntry;
import io.zell.zdb.journal.ReadOnlyJournalRecord;

/** Indexed journal entry. */
public record IndexedRaftLogEntryImpl(long index, long term, RaftEntry entry, ReadOnlyJournalRecord record)
         {
    public IndexedRaftLogEntryImpl(final long term, final RaftEntry entry, final ReadOnlyJournalRecord record) {
        this(record.index(), term, entry, record);
    }

    public IndexedRaftLogEntryImpl(long index, long term, RaftEntry entry, ReadOnlyJournalRecord record) {
        this.index = index;
        this.term = term;
        this.entry = entry;
        this.record = record;
    }

    public boolean isApplicationEntry() {
        return this.entry instanceof ApplicationEntry;
    }

    public ApplicationEntry getApplicationEntry() {
        return (ApplicationEntry)this.entry;
    }

    public PersistedRaftRecord getPersistedRaftRecord() {
        byte[] serializedRaftLogEntry = new byte[this.record.data().capacity()];
        this.record.data().getBytes(0, serializedRaftLogEntry);
        return new PersistedRaftRecord(this.term, this.index, this.record.asqn(), this.record.checksum(), serializedRaftLogEntry);
    }

    public long index() {
        return this.index;
    }

    public long term() {
        return this.term;
    }
}