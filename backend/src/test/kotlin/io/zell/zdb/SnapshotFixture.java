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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 * Reads and writes the pre-committed {@code zeebe-states/<version>.zip} snapshots used across all
 * version-specific test classes: {@link #unzip} for golden/behavior tests that consume a
 * snapshot, {@link #pack}/{@link #copyDirectory}/{@link #deleteRecursively} for
 * `SnapshotGenerator*Test` classes that produce one. Keeping both directions in one place avoids
 * every version repeating its own zip/copy/cleanup plumbing.
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

  /**
   * Zips {@code versionDir} (e.g. {@code .../zeebe-states/v8.8}) into {@code zipTarget}, with
   * entries relative to its grandparent so they carry the {@code zeebe-states/<version>/} prefix
   * that {@link #unzip} expects.
   */
  public static void pack(final Path versionDir, final Path zipTarget) throws IOException {
    final Path baseDir = versionDir.getParent().getParent();
    Files.deleteIfExists(zipTarget);
    try (var fos = new FileOutputStream(zipTarget.toFile());
        var zos = new ZipOutputStream(fos);
        var paths = Files.walk(versionDir)) {
      paths
          .filter(p -> !Files.isDirectory(p))
          .forEach(
              p -> {
                try {
                  zos.putNextEntry(new ZipEntry(baseDir.relativize(p).toString()));
                  Files.copy(p, zos);
                  zos.closeEntry();
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
    }
  }

  public static void copyDirectory(final Path source, final Path target) throws IOException {
    try (var paths = Files.walk(source)) {
      paths.forEach(
          src -> {
            try {
              final Path dest = target.resolve(source.relativize(src));
              if (Files.isDirectory(src)) {
                Files.createDirectories(dest);
              } else {
                Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
              }
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          });
    }
  }

  public static void deleteRecursively(final Path dir) throws IOException {
    if (!Files.exists(dir)) {
      return;
    }
    try (var paths = Files.walk(dir)) {
      paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  public void cleanup() throws IOException {
    deleteRecursively(snapshotDir.getParent().getParent());
  }
}
