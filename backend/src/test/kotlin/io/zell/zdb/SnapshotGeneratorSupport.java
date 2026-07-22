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
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.zeebe.containers.ZeebeContainer;
import io.zell.zdb.state.incident.IncidentState;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import org.assertj.core.api.Assertions;
import org.slf4j.Logger;
import org.testcontainers.utility.DockerImageName;

public final class SnapshotGeneratorSupport {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Path SNAPSHOT_ROOT = Path.of("src/test/resources/zeebe-states");

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

  private SnapshotGeneratorSupport() {}

  @FunctionalInterface
  public interface ContainerFactory {
    ZeebeContainer create(DockerImageName dockerImage, String tempDir, Logger logger);
  }

  public record GeneratorContext(
      Path tempDir, ZeebeContainer zeebeContainer, ZeebeContentCreator zeebeContentCreator) {}

  public static GeneratorContext startContainerAndCreateContent(
      final Class<?> testClass,
      final String dockerImage,
      final ContainerFactory containerFactory,
      final Logger logger) {
    final Path tempDir = TestUtils.newTmpFolder(testClass).toPath();
    try {
      Files.createDirectories(tempDir);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final var zeebeContentCreator = new ZeebeContentCreator(PROCESS);
    final var zeebeContainer =
        containerFactory.create(DockerImageName.parse(dockerImage), tempDir.toString(), logger);
    zeebeContainer.start();
    zeebeContentCreator.createContent(zeebeContainer.getExternalGatewayAddress());

    return new GeneratorContext(tempDir, zeebeContainer, zeebeContentCreator);
  }

  public static void stopContainerAndCleanup(final GeneratorContext context) throws IOException {
    if (context != null) {
      final var zeebeContainer = context.zeebeContainer();
      if (zeebeContainer != null && zeebeContainer.isRunning()) {
        zeebeContainer.stop();
      }
      SnapshotFixture.deleteRecursively(context.tempDir());
    }
  }

  public static void generateSnapshot(final GeneratorContext context, final String version)
      throws IOException {
    context.zeebeContainer().stop();

    final Path snapshotTarget = SNAPSHOT_ROOT.resolve(version);
    final Path snapshotZip = SNAPSHOT_ROOT.resolve(version + ".zip");

    SnapshotFixture.deleteRecursively(snapshotTarget);
    SnapshotFixture.copyDirectory(context.tempDir(), snapshotTarget);

    final var runtimePath = ZeebePaths.Companion.getRuntimePath(snapshotTarget.toFile(), "1");
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

    final var zeebeContentCreator = context.zeebeContentCreator();
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
        .writeValue(snapshotTarget.resolve("metadata.json").toFile(), metadata);

    SnapshotFixture.pack(snapshotTarget, snapshotZip);
    SnapshotFixture.deleteRecursively(snapshotTarget);
  }
}
