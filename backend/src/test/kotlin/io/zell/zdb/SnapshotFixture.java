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
package io.zell.zdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Unzips a pre-committed {@code zeebe-states/<version>.zip} snapshot (as produced by a
 * `SnapshotGenerator*Test`) into a temp directory and exposes its {@link SnapshotMetadata}. Shared
 * across all version-specific test classes so each one only wires up {@code @BeforeAll}/{@code
 * @AfterAll} against this fixture instead of repeating the unzip logic.
 */
public record SnapshotFixture(Path snapshotDir, SnapshotMetadata metadata) {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static SnapshotFixture unzip(final Path zipPath, final String version) throws IOException {
    final Path tempRoot = Files.createTempDirectory("zdb-" + version + "-snapshot");
    try (var zis = new ZipInputStream(Files.newInputStream(zipPath))) {
      ZipEntry entry;
      while ((entry = zis.getNextEntry()) != null) {
        final var dest = tempRoot.resolve(entry.getName());
        if (entry.isDirectory()) {
          Files.createDirectories(dest);
        } else {
          Files.createDirectories(dest.getParent());
          Files.copy(zis, dest);
        }
        zis.closeEntry();
      }
    }

    final Path snapshotDir = tempRoot.resolve("zeebe-states/" + version);
    final SnapshotMetadata metadata =
        OBJECT_MAPPER.readValue(snapshotDir.resolve("metadata.json").toFile(), SnapshotMetadata.class);
    return new SnapshotFixture(snapshotDir, metadata);
  }

  public void cleanup() throws IOException {
    final Path tempRoot = snapshotDir.getParent().getParent();
    Files.walk(tempRoot).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
  }
}
