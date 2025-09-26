package io.zell.zdb.journal;

import io.atomix.raft.cluster.RaftMember;
import io.zell.zdb.raft.RaftStatus;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@SuppressWarnings("unused")
@CommandLine.Command(
    name = "raft",
    mixinStandardHelpOptions = true,
    description = "Allows to inspect the log via sub commands")
public class RaftCommand implements Callable<Integer> {
  @CommandLine.Spec private CommandLine.Model.CommandSpec spec;

  public enum Format {
    JSON,
    TABLE,
  }

  @CommandLine.Option(
      names = {"-p", "--path"},
      paramLabel = "LOG_PATH",
      description = "The path to the partition log data, should end with the partition id.",
      required = true,
      scope = CommandLine.ScopeType.INHERIT)
  private Path partitionPath;

  @CommandLine.Option(
      names = {"-f", "--format"},
      description =
          "Print's the complete log in the specified format, defaults to json. Possible values: [ ${COMPLETION-CANDIDATES} ]",
      defaultValue = "JSON",
      scope = CommandLine.ScopeType.INHERIT)
  private Format format;

  @CommandLine.Command(name = "status", description = "Print's the status of the Raft server")
  public int status() {
    if (format == Format.JSON) {
      System.out.println(new RaftStatus(partitionPath).detailsAsJson());
    } else {
      printDetailsAsTable();
    }
    return 0;
  }

  private void printDetailsAsTable() {
    final var status = new RaftStatus(partitionPath).details();
    System.out.printf(
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
    System.out.printf(
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

  @NotNull
  private static String formatMembers(Collection<RaftStatus.RaftMemberDetails> members) {
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

  @Override
  public Integer call() {
    spec.commandLine().usage(System.out);
    return 0;
  }
}
