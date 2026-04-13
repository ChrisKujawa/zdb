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

import static io.zell.zdb.TestUtils.createZeebeContainerGreaterOrEquals88;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.camunda.zeebe.client.api.response.DeploymentEvent;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import io.camunda.zeebe.db.impl.ZeebeDbConstants;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.test.util.MsgPackUtil;
import io.camunda.zeebe.util.FileUtil;
import io.zeebe.containers.ZeebeContainer;
import io.zell.zdb.state.ZeebeDbReader;
import io.zell.zdb.state.process.ProcessState;
import io.zell.zdb.v88.Version88Test;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ZeebeStateTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ZeebeStateTest.class);
  private static final File tempDir =
      new File("/tmp/", "data-" + ThreadLocalRandom.current().nextLong());

  static {
    // for the Zeebe container the folder need to exist
    tempDir.mkdirs();
  }

  private static final BpmnModelInstance process =
      Bpmn.createExecutableProcess("process")
          .startEvent()
          .parallelGateway("gw")
          .serviceTask("task")
          .zeebeJobType("type")
          .endEvent()
          .moveToLastGateway()
          .serviceTask("incidentTask")
          .zeebeInputExpression("=foo", "bar")
          .zeebeJobType("type")
          .endEvent()
          .done();
  private static final String CONTAINER_PATH = "/usr/local/zeebe/data/";

  @Container
  public static ZeebeContainer zeebeContainer =
      createZeebeContainerGreaterOrEquals88(Version88Test.DOCKER_IMAGE, tempDir.getPath(), LOGGER);

  private static DeploymentEvent deploymentEvent;
  private static ProcessInstanceEvent returnedProcessInstance;
  private static CountDownLatch jobLatch;
  private static final AtomicLong jobKey = new AtomicLong();

  @BeforeAll
  public static void setup() throws Exception {
    try (final var rocksDB = RocksDB.open(tempDir.getPath())) {

      final byte[] keyBytes = new byte[16];
      final var keyBuffer = new UnsafeBuffer(keyBytes);

      // write some process with non-typical value
      // to ensure that we are not fail if values doesn't correspond to normal value
      keyBuffer.putLong(
          0,
          ZbColumnFamilies.DEPRECATED_PROCESS_CACHE.ordinal(),
          ZeebeDbConstants.ZB_DB_BYTE_ORDER);
      keyBuffer.putLong(Long.BYTES, 0xCAFE, ZeebeDbConstants.ZB_DB_BYTE_ORDER);

      rocksDB.put(keyBytes, MsgPackUtil.asMsgPackReturnArray("{\"foo\":123}"));

      // write some key with prefix column family which doesn't exist yet
      // to mimic newer version and handle such gracefully

      keyBuffer.putLong(0, 255, ZeebeDbConstants.ZB_DB_BYTE_ORDER);
      keyBuffer.putLong(Long.BYTES, 0xCAFE, ZeebeDbConstants.ZB_DB_BYTE_ORDER);

      rocksDB.put(keyBytes, MsgPackUtil.asMsgPackReturnArray("{\"foo\":456}"));

      keyBuffer.putLong(0, 1023, ZeebeDbConstants.ZB_DB_BYTE_ORDER);
      keyBuffer.putLong(Long.BYTES, 11, ZeebeDbConstants.ZB_DB_BYTE_ORDER);

      rocksDB.put(keyBytes, MsgPackUtil.asMsgPackReturnArray(""));
      rocksDB.flushWal(true);
      rocksDB.syncWal();
      final FlushOptions flushOptions = new FlushOptions();
      flushOptions.waitForFlush();
      rocksDB.flush(flushOptions);
    }
  }

  @AfterAll
  public static void cleanup() throws Exception {
    FileUtil.deleteFolderIfExists(tempDir.toPath());
  }

  @Test
  public void shouldCalculateStatisticsCorrectly() {
    // given
    final var zeebeDbReader = new ZeebeDbReader(tempDir.toPath());

    // when
    final var cfMap = zeebeDbReader.stateStatistics();

    // then
    assertThat(cfMap)
        .containsEntry(ZbColumnFamilies.DEPRECATED_PROCESS_CACHE.name(), 1)
        .containsEntry("255 (UNKNOWN)", 1);
  }

  @Test
  public void shouldListAllValuesAsJson() {
    // given
    final var zeebeDbReader = new ZeebeDbReader(tempDir.toPath());
    final var state = new HashMap<String, Map<Long, String>>();
    final ZeebeDbReader.JsonValueVisitor jsonVisitor =
        (cf, k, v) -> {
          final var keyValues = state.computeIfAbsent(cf, (columnFamily) -> new HashMap<>());

          keyValues.put(
              new UnsafeBuffer(k).getLong(Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER), v);
        };

    // when
    zeebeDbReader.visitDBWithJsonValues(jsonVisitor);

    // then
    assertThat(state)
        .containsEntry("255 (UNKNOWN)", Map.of(0xCAFEL, "{\"foo\":456}"))
        .containsEntry("1023 (UNKNOWN)", Map.of(11L, "null"))
        .containsEntry(
            ZbColumnFamilies.DEPRECATED_PROCESS_CACHE.name(), Map.of(0xCAFEL, "{\"foo\":123}"));
  }

  @Test
  public void shouldThrowOnNonExistingDb() {
    assertThatThrownBy(
            () -> new ZeebeDbReader(Path.of("/tmp", "shouldNotCalculateStatisticsOnNonExistingDb")))
        .hasMessageContaining("Expected to find RocksDB instance")
        .isInstanceOf(FileNotFoundException.class);
  }

  @Test
  public void shouldGetProcessDetails() {
    // given
    final var processState = new ProcessState(tempDir.toPath());
    final var processes = new ArrayList<String>();

    // when
    processState.processDetails(0xCAFEL, (key, valueJson) -> processes.add(valueJson));

    // then
    assertThat(processes).containsExactly("{\"foo\":123}");
  }
}
