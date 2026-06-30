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
package io.zell.zdb.v88;

import static io.zell.zdb.TestUtils.createZeebeContainerGreaterOrEquals88;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.containers.ZeebeContainer;
import io.zell.zdb.SnapshotMetadata;
import io.zell.zdb.TestUtils;
import io.zell.zdb.ZeebeContentCreator;
import io.zell.zdb.ZeebePaths;
import io.zell.zdb.state.incident.IncidentState;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;

@Tag("snapshot-generator")
class SnapshotGeneratorV88Test {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotGeneratorV88Test.class);
  private static final DockerImageName DOCKER_IMAGE =
      DockerImageName.parse("camunda/camunda:8.8.0");
  private static final File TEMP_DIR = TestUtils.newTmpFolder(SnapshotGeneratorV88Test.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Relative to backend/ module root — Maven sets CWD to the module during tests
  private static final Path SNAPSHOT_TARGET = Path.of("src/test/resources/zeebe-states/v8.8");
  private static final Path SNAPSHOT_ZIP =
      Path.of("src/test/resources/zeebe-states/v8.8.zip");

  private static final BpmnModelInstance PROCESS =
      Bpmn.createExecutableProcess("process")
          .startEvent()
          .parallelGateway("gw")
          .serviceTask("task")
          .zeebeJobType("type")
          .endEvent()
          .moveToLastGateway()
          .serviceTask("incidentTask")
          .zeebeInputExpression("=foo", "bar")
          .zeebeJobType("incidentTask")
          .zeebeJobRetriesExpression("=foo")
          .endEvent()
          .done();

  private static final ZeebeContentCreator zeebeContentCreator =
      new ZeebeContentCreator(PROCESS);
  private static ZeebeContainer zeebeContainer;

  static {
    TEMP_DIR.mkdirs();
  }

  @BeforeAll
  static void startContainerAndCreateContent() {
    zeebeContainer =
        createZeebeContainerGreaterOrEquals88(DOCKER_IMAGE, TEMP_DIR.getPath(), LOGGER);
    zeebeContainer.start();
    zeebeContentCreator.createContent(zeebeContainer.getExternalGatewayAddress());
  }

  @Test
  void generateSnapshot() throws Exception {
    // given: content created in @BeforeAll, stop container to flush all data to disk
    zeebeContainer.stop();

    // when: copy Zeebe data directory to committed test resources
    deleteRecursively(SNAPSHOT_TARGET);
    copyDirectory(TEMP_DIR.toPath(), SNAPSHOT_TARGET);

    // Query the incident key from the committed snapshot (not available directly from
    // ZeebeContentCreator — we derive it by reading ZDB state on the copied snapshot)
    final var runtimePath =
        ZeebePaths.Companion.getRuntimePath(SNAPSHOT_TARGET.toFile(), "1");
    final var incidentKeys = new ArrayList<Long>();
    new IncidentState(runtimePath)
        .listIncidents(
            json -> {
              try {
                incidentKeys.add(OBJECT_MAPPER.readTree(json).get("key").asLong());
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    // then: write metadata.json with all keys needed by Version88GoldenTest
    final var metadata =
        new SnapshotMetadata(
            zeebeContentCreator.firstProcessKey,
            zeebeContentCreator.secondProcessKey,
            zeebeContentCreator.processInstanceEvent.getProcessInstanceKey(),
            zeebeContentCreator.elementInstanceKey,
            zeebeContentCreator.responseJobKey,
            incidentKeys.get(0));

    OBJECT_MAPPER
        .writerWithDefaultPrettyPrinter()
        .writeValue(SNAPSHOT_TARGET.resolve("metadata.json").toFile(), metadata);

    // Zip the snapshot directory and remove the unzipped tree so only the zip is committed
    zipDirectory(SNAPSHOT_TARGET, SNAPSHOT_ZIP);
    deleteRecursively(SNAPSHOT_TARGET);
  }

  private static void deleteRecursively(Path dir) throws Exception {
    if (!Files.exists(dir)) {
      return;
    }
    Files.walk(dir)
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
  }

  private static void zipDirectory(Path source, Path zipTarget) throws Exception {
    Files.deleteIfExists(zipTarget);
    try (var fos = new FileOutputStream(zipTarget.toFile());
        var zos = new ZipOutputStream(fos)) {
      Files.walk(source)
          .filter(p -> !Files.isDirectory(p))
          .forEach(
              p -> {
                try {
                  zos.putNextEntry(new ZipEntry(source.relativize(p).toString()));
                  Files.copy(p, zos);
                  zos.closeEntry();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });
    }
  }

  private static void copyDirectory(Path source, Path target) throws Exception {
    Files.walk(source)
        .forEach(
            src -> {
              try {
                final Path dest = target.resolve(source.relativize(src));
                if (Files.isDirectory(src)) {
                  Files.createDirectories(dest);
                } else {
                  Files.copy(src, dest, StandardCopyOption.REPLACE_EXISTING);
                }
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
  }
}
