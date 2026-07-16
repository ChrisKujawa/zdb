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
package io.zell.zdb.output;

import io.zell.zdb.raft.RaftStatus;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.stream.Collectors;

public final class RaftOutput {

  private RaftOutput() {}

  public static String json(final Path partitionPath) {
    return new RaftStatus(partitionPath).detailsAsJson();
  }

  public static void writeJson(final PrintStream out, final Path partitionPath) {
    out.println(json(partitionPath));
  }

  public static String table(final Path partitionPath) {
    return OutputSupport.capture(out -> writeTable(out, partitionPath));
  }

  public static void writeTable(final PrintStream out, final Path partitionPath) {
    final var status = new RaftStatus(partitionPath).details();
    out.printf(
        """
            --------------------------------------------------------------
            Raft Status for partition '%s':
            --------------------------------------------------------------
            Meta Store:
                Term:                    %d
                Last Flushed Index:      %d
                Commit Index:            %d
                Voted For:               %s%n""",
        partitionPath.getFileName(),
        status.meta().term(),
        status.meta().lastFlushedIndex(),
        status.meta().commitIndex(),
        status.meta().votedFor());
    out.printf(
        """
            --------------------------------------------------------------
            Configuration:
                Index:                   %d
                Term:                    %d
                Time:                    %d
                Force:                   %b
                Requires Join Consensus: %b
                New Members:             %s
                Old Members:             %s
            --------------------------------------------------------------""",
        status.config().index(),
        status.config().term(),
        status.config().time(),
        status.config().force(),
        status.config().requiresJointConsensus(),
        formatMembers(status.config().newMembers()),
        formatMembers(status.config().oldMembers()));
  }

  private static String formatMembers(final Collection<RaftStatus.RaftMemberDetails> members) {
    if (members.isEmpty()) {
      return "[]";
    }

    return "["
        + System.lineSeparator()
        + members.stream()
            .map(
                m ->
                    """
            \t\tId: %s, Type: %s, Hash: %d, Updated: %s"""
                        .formatted(m.id(), m.type(), m.hash(), m.lastUpdated()))
            .collect(Collectors.joining(System.lineSeparator()))
        + System.lineSeparator()
        + "    ]";
  }
}
