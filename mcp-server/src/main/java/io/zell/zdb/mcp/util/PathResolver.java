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
package io.zell.zdb.mcp.util;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

/** Utility for resolving the RocksDB runtime path inside a Zeebe partition directory. */
public final class PathResolver {

  private PathResolver() {}

  /**
   * Resolve the RocksDB runtime path for the given Zeebe partition directory. Falls back to the
   * latest snapshot directory if no live runtime exists.
   *
   * @param partitionPath the partition directory (e.g. {@code <data>/raft-partition/partitions/1})
   * @return the path to the runtime (or latest snapshot) directory
   * @throws IllegalArgumentException if neither a runtime nor any snapshot is present
   */
  public static Path runtimePath(final String partitionPath) {
    final Path partDir = Paths.get(partitionPath);
    final Path runtime = partDir.resolve("runtime");
    if (Files.isDirectory(runtime)) {
      return runtime;
    }

    final Path snapshots = partDir.resolve("snapshots");
    if (Files.isDirectory(snapshots)) {
      try (Stream<Path> stream = Files.list(snapshots)) {
        return stream
            .filter(Files::isDirectory)
            .max(Comparator.naturalOrder())
            .orElseThrow(() -> new IllegalArgumentException("No snapshots found in " + snapshots));
      } catch (final java.io.IOException e) {
        throw new IllegalArgumentException("Failed to list snapshots directory: " + snapshots, e);
      }
    }

    throw new IllegalArgumentException(
        "No runtime or snapshots directory found in " + partitionPath);
  }
}
