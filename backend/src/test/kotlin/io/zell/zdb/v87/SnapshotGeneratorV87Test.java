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
package io.zell.zdb.v87;

import static io.zell.zdb.TestUtils.createZeebeContainerBetween85And88;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.containers.ZeebeContainer;
import io.zell.zdb.SnapshotFixture;
import io.zell.zdb.SnapshotMetadata;
import io.zell.zdb.TestUtils;
import io.zell.zdb.ZeebeContentCreator;
import io.zell.zdb.ZeebePaths;
import io.zell.zdb.state.incident.IncidentState;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;

@Tag("snapshot-generator")
class SnapshotGeneratorV87Test {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotGeneratorV87Test.class);
  private static final DockerImageName DOCKER_IMAGE =
      DockerImageName.parse("camunda/zeebe:8.7.0");
  private static final File TEMP_DIR = TestUtils.newTmpFolder(SnapshotGeneratorV87Test.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Relative to backend/ module root — Maven sets CWD to the module during tests
  private static final Path SNAPSHOT_TARGET = Path.of("src/test/resources/zeebe-states/v8.7");
  private static final Path SNAPSHOT_ZIP =
      Path.of("src/test/resources/zeebe-states/v8.7.zip");

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
        createZeebeContainerBetween85And88(DOCKER_IMAGE, TEMP_DIR.getPath(), LOGGER);
    zeebeContainer.start();
    zeebeContentCreator.createContent(zeebeContainer.getExternalGatewayAddress());
  }

  @AfterAll
  static void stopContainerAndCleanup() throws Exception {
    if (zeebeContainer != null && zeebeContainer.isRunning()) {
      zeebeContainer.stop();
    }
    SnapshotFixture.deleteRecursively(TEMP_DIR.toPath());
  }

  @Test
  void generateSnapshot() throws Exception {
    // given: content created in @BeforeAll, stop container to flush all data to disk
    zeebeContainer.stop();

    // when: copy Zeebe data directory to committed test resources
    SnapshotFixture.deleteRecursively(SNAPSHOT_TARGET);
    SnapshotFixture.copyDirectory(TEMP_DIR.toPath(), SNAPSHOT_TARGET);

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
    Assertions.assertThat(incidentKeys)
        .as("expected exactly one incident in the snapshot")
        .hasSize(1);

    // then: write metadata.json with all keys needed by Version87GoldenTest
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

    // Zip relative to src/test/resources so entries carry the zeebe-states/v8.7/ prefix,
    // matching the path the golden test resolves after unzipping (snapshotDir = tempRoot.resolve("zeebe-states/v8.7"))
    SnapshotFixture.pack(SNAPSHOT_TARGET, SNAPSHOT_ZIP);
    SnapshotFixture.deleteRecursively(SNAPSHOT_TARGET);
  }
}
