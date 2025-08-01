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
package io.zell.zdb.v86;

import static io.zell.zdb.TestUtils.TIMESTAMP_REGEX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.db.impl.ZeebeDbConstants;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.protocol.record.value.ErrorType;
import io.camunda.zeebe.util.FileUtil;
import io.zeebe.containers.ZeebeContainer;
import io.zell.zdb.TestUtils;
import io.zell.zdb.ZeebeContentCreator;
import io.zell.zdb.ZeebePaths;
import io.zell.zdb.log.LogContentReader;
import io.zell.zdb.log.LogSearch;
import io.zell.zdb.log.LogStatus;
import io.zell.zdb.log.LogWriter;
import io.zell.zdb.log.records.ApplicationRecord;
import io.zell.zdb.log.records.PersistedRecord;
import io.zell.zdb.log.records.RaftRecord;
import io.zell.zdb.log.records.Record;
import io.zell.zdb.raft.RaftStatus;
import io.zell.zdb.state.ZeebeDbReader;
import io.zell.zdb.state.incident.IncidentState;
import io.zell.zdb.state.instance.InstanceState;
import io.zell.zdb.state.process.ProcessState;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.stream.StreamSupport;
import org.agrona.concurrent.UnsafeBuffer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class Version86Test {

  private static final DockerImageName DOCKER_IMAGE = DockerImageName.parse("camunda/zeebe:8.6.0");
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final BpmnModelInstance PROCESS =
      Bpmn.createExecutableProcess("process")
          .startEvent()
          .parallelGateway("gw")
          .serviceTask("task")
          .zeebeJobType("type")
          .endEvent()
          .moveToLastGateway()
          .serviceTask("incidentTask")
          .zeebeInputExpression("=foo", "bar") // before 8.3 caused an incident
          .zeebeJobType("type")
          .zeebeJobRetriesExpression("=foo") // should cause to create an incident
          .endEvent()
          .done();

  @Nested
  public class LargeLogTest {
    // earlier ZDB versions failed on large logs, because of segments were async created and
    // incomplete
    private static final Logger LOGGER = LoggerFactory.getLogger(LargeLogTest.class);
    private static final File TEMP_DIR = TestUtils.newTmpFolder(LargeLogTest.class);
    private static final ZeebeContentCreator zeebeContentCreator = new ZeebeContentCreator(PROCESS);

    @Container
    public static ZeebeContainer zeebeContainer =
        new ZeebeContainer(DOCKER_IMAGE)
            /* run the container with the current user, in order to access the data and delete it later */
            .withCreateContainerCmdModifier(cmd -> cmd.withUser(TestUtils.getRunAsUser()))
            // with 8.2 we disabled WAL per default
            // we have to enabled it inorder to access the data from RocksDB
            .withEnv("ZEEBE_BROKER_EXPERIMENTAL_ROCKSDB_DISABLEWAL", "false")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withFileSystemBind(TEMP_DIR.getPath(), TestUtils.CONTAINER_PATH, BindMode.READ_WRITE);

    static {
      TEMP_DIR.mkdirs();
    }

    @BeforeAll
    public static void setup() {
      zeebeContentCreator.createLargeContent(zeebeContainer.getExternalGatewayAddress());
    }

    @AfterAll
    public static void cleanup() throws Exception {
      FileUtil.deleteFolderIfExists(TEMP_DIR.toPath());
    }

    @Test
    public void shouldReadStatusFromLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logStatus = new LogStatus(logPath);

      // when
      final var status = logStatus.status();

      // then
      assertThat(status.getHighestIndex()).isEqualTo(213);
      assertThat(status.getHighestTerm()).isEqualTo(1);
      assertThat(status.getHighestRecordPosition()).isEqualTo(261);
      assertThat(status.getLowestIndex()).isEqualTo(1);
      assertThat(status.getLowestRecordPosition()).isEqualTo(1);

      assertThat(status.toString())
          .contains("lowestRecordPosition")
          .contains("highestRecordPosition")
          .contains("highestTerm")
          .contains("highestIndex")
          .contains("lowestIndex");
    }
  }

  @Nested
  public class ZeebeLogTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeebeLogTest.class);
    private static final File TEMP_DIR = TestUtils.newTmpFolder(ZeebeLogTest.class);
    private static final ZeebeContentCreator zeebeContentCreator = new ZeebeContentCreator(PROCESS);

    @Container
    public static ZeebeContainer zeebeContainer =
        new ZeebeContainer(DOCKER_IMAGE)
            /* run the container with the current user, in order to access the data and delete it later */
            .withCreateContainerCmdModifier(cmd -> cmd.withUser(TestUtils.getRunAsUser()))
            // with 8.2 we disabled WAL per default
            // we have to enabled it inorder to access the data from RocksDB
            .withEnv("ZEEBE_BROKER_EXPERIMENTAL_ROCKSDB_DISABLEWAL", "false")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withFileSystemBind(TEMP_DIR.getPath(), TestUtils.CONTAINER_PATH, BindMode.READ_WRITE);

    static {
      TEMP_DIR.mkdirs();
    }

    @BeforeAll
    public static void setup() {
      zeebeContentCreator.createContent(zeebeContainer.getExternalGatewayAddress());
    }

    @AfterAll
    public static void cleanup() throws Exception {
      FileUtil.deleteFolderIfExists(TEMP_DIR.toPath());
    }

    @Test
    public void shouldReadMetaStore() {
      // given
      final var dataPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var raftStatus = new RaftStatus(dataPath);

      // when
      final var status = raftStatus.details();

      // then
      assertThat(status.meta().commitIndex())
          .as("no commit index in the meta store for 8.6")
          .isZero();
      assertThat(status.meta().term()).isEqualTo(1);
      assertThat(status.meta().lastFlushedIndex()).isEqualTo(15);
      // we can't easily compare the time, but at least we can make sure it's between certain bounds
      // indicating a valid
      // value
      assertThat(status.config().term()).isZero();
      assertThat(status.config().force()).isFalse();
      assertThat(status.config().requiresJointConsensus()).isFalse();
      assertThat(status.config().time())
          .isBetween(Instant.now().minusSeconds(60).toEpochMilli(), Instant.now().toEpochMilli());
      // we don't compare the member's last updated, again because we can't exactly pinpoint the
      // instant in time it was
      // last updated
      assertThat(status.config().newMembers())
          .hasSize(1)
          .first()
          .returns("0", RaftStatus.RaftMemberDetails::id)
          .returns("ACTIVE", RaftStatus.RaftMemberDetails::type)
          .returns(384918240, RaftStatus.RaftMemberDetails::hash);
      assertThat(status.config().oldMembers()).isEmpty();
    }

    @Test
    public void shouldReadStatusFromLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logStatus = new LogStatus(logPath);

      // when
      final var status = logStatus.status();

      // then
      assertThat(status.getHighestIndex()).isEqualTo(15);
      assertThat(status.getHighestTerm()).isEqualTo(1);
      assertThat(status.getHighestRecordPosition()).isEqualTo(63);
      assertThat(status.getLowestIndex()).isEqualTo(1);
      assertThat(status.getLowestRecordPosition()).isEqualTo(1);

      assertThat(status.toString())
          .contains("lowestRecordPosition")
          .contains("highestRecordPosition")
          .contains("highestTerm")
          .contains("highestIndex")
          .contains("lowestIndex");
    }

    @Test
    public void shouldThrowWhenReadStatusFromNonExistingLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(new File("/tmp/doesntExist"), "1");

      // when - throw
      assertThatThrownBy(() -> new LogStatus(logPath))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Expected to read segments, but there was nothing to read");
    }

    @Test
    public void shouldBuildLogContent() throws JsonProcessingException {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);

      // when
      final var content = logContentReader.readAll();

      // then
      verifyCompleteLog(content.getRecords());

      final var objectMapper = new ObjectMapper();
      final var jsonNode = objectMapper.readTree(content.toString());
      assertThat(jsonNode).isNotNull(); // is valid json
    }

    @Test
    public void shouldReadLogContentWithIterator() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      verifyCompleteLog(records);
    }

    @Test
    public void shouldReadRejection() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);

      // when
      logContentReader.filterForRejections();

      // then
      final var rejection =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
              .filter(persistedRecord -> persistedRecord instanceof ApplicationRecord)
              .map(persistedRecord -> (ApplicationRecord) persistedRecord)
              .flatMap(applicationRecord -> applicationRecord.getEntries().stream())
              .filter(record -> record.component8() != RejectionType.NULL_VAL)
              .findFirst();

      assertThat(rejection).isPresent();
      assertThat(rejection.get().component8()).isEqualTo(RejectionType.NOT_FOUND);
      assertThat(rejection.get().component9())
          .isEqualTo(
              "Expected to find process definition with process ID 'nonExisting', but none found");
    }

    @Test
    public void shouldSerializeRejectionToJson() throws JsonProcessingException {
      // given
      final var expectedJson =
          OBJECT_MAPPER.readTree(
"""
                    {"position":63,"sourceRecordPosition":62,"key":-1,"recordType":"COMMAND_REJECTION",
                    "valueType":"PROCESS_INSTANCE_CREATION","intent":"CREATE","rejectionType":"NOT_FOUND",
                    "rejectionReason":"Expected to find process definition with process ID 'nonExisting', but none found",
                    "requestId":-1,"requestStreamId":-2147483648,"protocolVersion":5,"brokerVersion":"8.6.0",
                    "recordVersion":1,
                    "recordValue":{"bpmnProcessId":"nonExisting","processDefinitionKey":0,"processInstanceKey":-1,
                    "version":-1,"variables":"gA==","fetchVariables":[],
                    "startInstructions":[],"tenantId":"<default>"}}
                    }
""");
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);

      // when
      logContentReader.filterForRejections();

      // then
      final var rejection =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
              .filter(persistedRecord -> persistedRecord instanceof ApplicationRecord)
              .map(persistedRecord -> (ApplicationRecord) persistedRecord)
              .flatMap(applicationRecord -> applicationRecord.getEntries().stream())
              .filter(record -> !record.component8().equals(RejectionType.NULL_VAL.name()))
              .findFirst();

      assertThat(rejection).isPresent();
      assertThat(rejection.get().component8()).isEqualTo(RejectionType.NOT_FOUND);
      assertThat(rejection.get().component9())
          .isEqualTo(
              "Expected to find process definition with process ID 'nonExisting', but none found");

      final var recordJson = rejection.get().toString().replaceFirst(TIMESTAMP_REGEX, "");
      final var actualJson = OBJECT_MAPPER.readTree(recordJson);
      assertThat(actualJson).isNotNull(); // is valid json
      assertThat(actualJson).isEqualTo(expectedJson);
    }

    @Test
    public void shouldSerializeRecordToJson() throws JsonProcessingException {
      // given
      final var expectedJson =
          OBJECT_MAPPER.readTree(
"""
                    {"position":12,"sourceRecordPosition":5,"key":2251799813685252,"recordType":"EVENT",
                    "valueType":"PROCESS_INSTANCE","intent":"ELEMENT_ACTIVATED","requestId":-1,
                    "requestStreamId":-2147483648,"protocolVersion":5,"brokerVersion":"8.6.0","recordVersion":1,
                    "recordValue":{"bpmnElementType":"PROCESS","elementId":"process","bpmnProcessId":"process",
                    "version":1,"processDefinitionKey":2251799813685250,"processInstanceKey":2251799813685252,
                    "flowScopeKey":-1,"bpmnEventType":"UNSPECIFIED","parentProcessInstanceKey":-1,
                    "parentElementInstanceKey":-1,"tenantId":"<default>"}}
""");
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      logContentReader.filterForProcessInstance(
          zeebeContentCreator.processInstanceEvent.getProcessInstanceKey());

      // when
      final var piActivated =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
              .filter(persistedRecord -> persistedRecord instanceof ApplicationRecord)
              .map(persistedRecord -> (ApplicationRecord) persistedRecord)
              .flatMap(applicationRecord -> applicationRecord.getEntries().stream())
              .filter(record -> record.component6() == ValueType.PROCESS_INSTANCE)
              .filter(record -> record.component7() == ProcessInstanceIntent.ELEMENT_ACTIVATED)
              .filter(record -> record.getPiRelatedValue() != null)
              .filter(
                  record ->
                      record.getPiRelatedValue().getBpmnElementType() == BpmnElementType.PROCESS)
              .findFirst();

      // then
      assertThat(piActivated).isPresent();
      final var recordJson = piActivated.get().toString().replaceFirst(TIMESTAMP_REGEX, "");
      final var actualJson = OBJECT_MAPPER.readTree(recordJson);
      assertThat(actualJson).isNotNull(); // is valid json
      assertThat(actualJson).isEqualTo(expectedJson);
    }

    @Test
    public void shouldSkipFirstPartOfLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(10);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(11);
      // we skip the first raft record
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(0);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count())
          .isEqualTo(11);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(15);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(5);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(63);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(6);
    }

    @Test
    public void shouldNotSkipIfNegativeSeek() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(-1);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      verifyCompleteLog(records);
    }

    @Test
    public void shouldNotSkipIfZeroSeek() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(0);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      verifyCompleteLog(records);
    }

    @Test
    public void shouldSeekToEndOfLogIfNoExistingSeek() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(Long.MAX_VALUE);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(0);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(15);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(15);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(63);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(63);
    }

    @Test
    public void shouldLimitLogToPosition() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.limitToPosition(30);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(5);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(4);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(5);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(1);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(34);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(1);
    }

    @Test
    public void shouldLimitViaPositionExclusive() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.limitToPosition(1);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(1);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(1);
    }

    @Test
    public void shouldConvertRecordToColumn() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.limitToPosition(2);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(2);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);

      final var record = (ApplicationRecord) records.get(1);
      // Index Term RecordType ValueType Intent Position SourceRecordPosition

      final String columnString = record.asColumnString();
      final String[] elements = columnString.trim().split(" ");
      assertThat(elements).hasSize(9); // deployment record skips the last two columns
      assertThat(elements).containsSubsequence("2", "1", "1", "-1");
      // we skip timestamp since it is not reproducible
      assertThat(elements).containsSubsequence("-1", "COMMAND", "DEPLOYMENT", "CREATE");
    }

    @Test
    public void shouldWriteTableHeaderToStreamWhenNoDataFound() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var outputStream = new ByteArrayOutputStream();
      logContentReader.limitToPosition(30);
      logContentReader.seekToPosition(3);
      logContentReader.filterForProcessInstance(2251799813685254L);
      final var logWriter = new LogWriter(outputStream, logContentReader);

      // when
      logWriter.writeAsTable();

      // then
      assertThat(outputStream.toString().trim())
          .isEqualTo(
              "Index Term Position SourceRecordPosition Timestamp Key RecordType ValueType Intent ProcessInstanceKey BPMNElementType");
    }

    @Test
    public void shouldWriteTableToStream() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      final var outputStream = new ByteArrayOutputStream();
      logContentReader.limitToPosition(600);
      logContentReader.seekToPosition(6);
      logContentReader.filterForProcessInstance(2251799813685252L);
      final var logWriter = new LogWriter(outputStream, logContentReader);

      // when
      logWriter.writeAsTable();

      // then
      assertThat(outputStream.toString())
          .startsWith(
              "Index Term Position SourceRecordPosition Timestamp Key RecordType ValueType Intent ProcessInstanceKey BPMNElementType")
          // EQUALs check is hard due to the timestamp
          .contains("2251799813685253 EVENT VARIABLE CREATED 2251799813685252")
          .contains("EVENT PROCESS_INSTANCE ELEMENT_ACTIVATING 2251799813685252 START_EVENT");
    }

    @Test
    public void shouldSeekAndLimitLogWithPosition() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(5);
      logContentReader.limitToPosition(30);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(2);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(0);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(2);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(5);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(4);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(34);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(5);
    }

    @Test
    public void shouldFilterWithProcessInstanceKey() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.filterForProcessInstance(2251799813685252L);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(0);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(5);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(5);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(34);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(6);
    }

    @Test
    public void shouldFilterWithNoExistingProcessInstanceKey() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.filterForProcessInstance(0xCAFE);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(0);
    }

    @Test
    public void shouldFilterWithProcessInstanceKeyAndSetBeginAndEndOfLogPosition() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.filterForProcessInstance(2251799813685252L);
      logContentReader.seekToPosition(5);
      logContentReader.limitToPosition(30);

      // when
      logContentReader.forEachRemaining(records::add);

      // then
      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(0);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(5);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(5);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(34);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(6);
    }

    private static void verifyCompleteLog(final List<PersistedRecord> records) {
      assertThat(records).hasSize(15);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count())
          .isEqualTo(14);

      final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
      assertThat(maxIndex).isEqualTo(15);
      final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
      assertThat(minIndex).isEqualTo(1);

      final var maxPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getHighestPosition)
              .max(Long::compareTo)
              .orElseThrow();
      assertThat(maxPosition).isEqualTo(63);
      final var minPosition =
          records.stream()
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .map(ApplicationRecord::getLowestPosition)
              .min(Long::compareTo)
              .orElseThrow();
      assertThat(minPosition).isEqualTo(1);
    }

    @Test
    public void shouldReturnLogContentAsDotFile() throws JsonProcessingException {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);
      final var content = logContentReader.readAll();

      // when
      final var dotFileContent = content.asDotFile();

      // then
      assertThat(dotFileContent).startsWith("digraph log {").endsWith("}");
    }

    @Test
    public void shouldContainNoDuplicatesInLogContent() throws JsonProcessingException {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logContentReader = new LogContentReader(logPath);

      // when
      final var content = logContentReader.readAll();

      // then
      // validate that records are not duplicated in LogContent
      assertThat(content.getRecords())
          .filteredOn(ApplicationRecord.class::isInstance)
          .asInstanceOf(InstanceOfAssertFactories.list(ApplicationRecord.class))
          .flatExtracting(ApplicationRecord::getEntries)
          .extracting(Record::getPosition)
          .doesNotHaveDuplicates();
    }

    @Test
    public void shouldSearchPositionInLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logSearch = new LogSearch(logPath);
      final var position = 1;

      // when
      final Record record = logSearch.searchPosition(position);

      // then
      assertThat(record).isNotNull();
      assertThat(record.getPosition()).isEqualTo(position);
    }

    @Test
    public void shouldReturnNullOnNegPosition() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logSearch = new LogSearch(logPath);

      // when
      final Record record = logSearch.searchPosition(-1);

      // then
      assertThat(record).isNull();
    }

    @Test
    public void shouldReturnNullOnToBigPosition() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logSearch = new LogSearch(logPath);

      // when
      final Record record = logSearch.searchPosition(Long.MAX_VALUE);

      // then
      assertThat(record).isNull();
    }

    @Test
    public void shouldSearchIndexInLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logSearch = new LogSearch(logPath);
      final var index = 7;

      // when
      final var record = logSearch.searchIndex(index);

      // then
      assertThat(record).isNotNull();
    }

    @Test
    public void shouldNotReturnDuplicatesWhenSearchForIndexInLog() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logSearch = new LogSearch(logPath);
      final var index = 7;

      // when
      final var record = logSearch.searchIndex(index);

      // then
      // validate that records are not duplicated in LogContent
      assertThat(record)
          .asInstanceOf(InstanceOfAssertFactories.type(ApplicationRecord.class))
          .extracting(ApplicationRecord::getEntries)
          .asInstanceOf(InstanceOfAssertFactories.list(Record.class))
          .extracting(Record::getPosition)
          .doesNotHaveDuplicates();
    }

    @Test
    public void shouldReturnNullOnNegIndex() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logSearch = new LogSearch(logPath);

      // when
      final var logContent = logSearch.searchIndex(-1);

      // then
      assertThat(logContent).isNull();
    }

    @Test
    public void shouldReturnNullOnToBigIndex() {
      // given
      final var logPath = ZeebePaths.Companion.getLogPath(TEMP_DIR, "1");
      final var logSearch = new LogSearch(logPath);

      // when
      final var logContent = logSearch.searchIndex(Long.MAX_VALUE);

      // then
      assertThat(logContent).isNull();
    }
  }

  @Nested
  public class ZeebeStateTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZeebeStateTest.class);
    private static final File TEMP_DIR = TestUtils.newTmpFolder(ZeebeStateTest.class);
    private static final ZeebeContentCreator zeebeContentCreator = new ZeebeContentCreator(PROCESS);

    @Container
    public static ZeebeContainer zeebeContainer =
        new ZeebeContainer(DockerImageName.parse("camunda/zeebe:8.3.0"))
            /* run the container with the current user, in order to access the data and delete it later */
            .withCreateContainerCmdModifier(cmd -> cmd.withUser(TestUtils.getRunAsUser()))
            // with 8.2 we disabled WAL per default
            // we have to enabled it inorder to access the data from RocksDB
            .withEnv("ZEEBE_BROKER_EXPERIMENTAL_ROCKSDB_DISABLEWAL", "false")
            .withLogConsumer(new Slf4jLogConsumer(LOGGER))
            .withFileSystemBind(TEMP_DIR.getPath(), TestUtils.CONTAINER_PATH, BindMode.READ_WRITE);

    static {
      TEMP_DIR.mkdirs();
    }

    @BeforeAll
    public static void setup() {
      zeebeContentCreator.createContent(zeebeContainer.getExternalGatewayAddress());
    }

    @AfterAll
    public static void cleanup() throws Exception {
      FileUtil.deleteFolderIfExists(TEMP_DIR.toPath());
    }

    @Test
    public void shouldCreateStatsForCompleteState() {
      // given
      final var experimental =
          new ZeebeDbReader(ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1"));

      // when
      final var cfMap = experimental.stateStatistics();

      // then
      assertThat(cfMap)
          .containsEntry(ZbColumnFamilies.JOBS.name(), 1)
          .containsEntry(ZbColumnFamilies.VARIABLES.name(), 4)
          .containsEntry(ZbColumnFamilies.INCIDENTS.name(), 1)
          .containsEntry(ZbColumnFamilies.ELEMENT_INSTANCE_KEY.name(), 3);
    }

    @Test
    public void shouldVisitValuesAsJson() {
      // given
      final var experimental =
          new ZeebeDbReader(ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1"));
      final var incidentMap = new HashMap<String, String>();
      final ZeebeDbReader.JsonValueVisitor jsonVisitor =
          (cf, k, v) -> {
            if (cf.equals(ZbColumnFamilies.INCIDENTS.name())) {
              incidentMap.put(new String(k), v);
            }
          };

      // when
      experimental.visitDBWithJsonValues(jsonVisitor);

      // then
      assertThat(incidentMap)
          .containsValue(
              "{\"incidentRecord\":{\"errorType\":\"EXTRACT_VALUE_ERROR\",\"errorMessage\":\"Expected result of the expression 'foo' to be 'NUMBER', but was 'NULL'.\",\"bpmnProcessId\":\"process\",\"processDefinitionKey\":2251799813685249,\"processInstanceKey\":2251799813685252,\"elementId\":\"incidentTask\",\"elementInstanceKey\":2251799813685261,\"jobKey\":-1,\"variableScopeKey\":2251799813685261,\"tenantId\":\"<default>\"}}");
    }

    @Test
    public void shouldListProcesses() {
      // given
      final var processState = new ProcessState(ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1"));
      final var processes = new HashMap<Long, String>();

      // when
      processState.listProcesses(
          (key, valueJson) ->
              processes.put(
                  new UnsafeBuffer(key)
                      .getLong(key.length - Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER),
                  valueJson));

      // then
      assertThat(processes).containsKey(2251799813685249L).containsKey(2251799813685250L);
    }

    @Test
    public void shouldGetProcessDetails() throws JsonProcessingException {
      // given
      final var processes = new ArrayList<String>();
      final Path runtimePath = ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1");
      final var processState = new ProcessState(runtimePath);
      final var returnedProcess = zeebeContentCreator.deploymentEvent.getProcesses().get(0);

      // when
      processState.processDetails(
          returnedProcess.getProcessDefinitionKey(), (k, v) -> processes.add(v));

      // then
      assertThat(processes).hasSize(1);

      final var objectMapper = new ObjectMapper();

      final var jsonNode = objectMapper.readTree(processes.get(0));

      assertThat(jsonNode.get("bpmnProcessId").asText())
          .isEqualTo(returnedProcess.getBpmnProcessId());
      assertThat(jsonNode.get("key").asLong()).isEqualTo(returnedProcess.getProcessDefinitionKey());
      assertThat(jsonNode.get("resourceName").asText())
          .isEqualTo(returnedProcess.getResourceName());
      assertThat(jsonNode.get("version").asInt()).isEqualTo(returnedProcess.getVersion());
      assertThat(jsonNode.get("resource").asText())
          .isEqualTo(
              Base64.getEncoder()
                  .encodeToString(Bpmn.convertToString(PROCESS).getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void shouldNotFailOnNonExistingProcess() {
      // given
      final var processes = new ArrayList<String>();

      // when
      final var processState = new ProcessState(ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1"));
      processState.processDetails(0xCAFE, (k, v) -> processes.add(v));

      // then
      assertThat(processes).isEmpty();
    }

    @Test
    public void shouldGetProcessInstanceDetails() throws JsonProcessingException {
      // given
      final var processInstanceEvent = zeebeContentCreator.processInstanceEvent;
      final var processInstanceKey = processInstanceEvent.getProcessInstanceKey();

      // when
      final var processState =
          new InstanceState(ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1"));
      final var actualInstanceDetails = processState.getInstance(processInstanceKey);

      // then
      assertThat(actualInstanceDetails).isNotNull();
      final var instanceAsJson = OBJECT_MAPPER.readTree(actualInstanceDetails);
      final var elementRecord = instanceAsJson.get("elementRecord");
      assertThat(elementRecord.get("key").asLong()).isEqualTo(processInstanceKey);
      assertThat(elementRecord.get("state").asText())
          .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
      final var processInstanceRecord = elementRecord.get("processInstanceRecord");
      assertThat(processInstanceRecord.get("bpmnProcessId").asText())
          .isEqualTo(processInstanceEvent.getBpmnProcessId());
      assertThat(processInstanceRecord.get("processDefinitionKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessDefinitionKey());
      assertThat(processInstanceRecord.get("version").asInt())
          .isEqualTo(processInstanceEvent.getVersion());
      assertThat(processInstanceRecord.get("processInstanceKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessInstanceKey());
      assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo("process");
      assertThat(processInstanceRecord.get("bpmnElementType").asText())
          .isEqualTo(BpmnElementType.PROCESS.name());
      assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
      assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
      assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(-1);
      assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(2);
    }

    @Test
    public void shouldGetElementInstanceDetails() throws JsonProcessingException {
      // given
      final var processInstanceEvent = zeebeContentCreator.processInstanceEvent;
      final var processInstanceKey = processInstanceEvent.getProcessInstanceKey();
      final var elementInstanceKey = 2251799813685263L;

      // when
      final var processState =
          new InstanceState(ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1"));
      final var actualInstanceDetails = processState.getInstance(elementInstanceKey);

      // then
      assertThat(actualInstanceDetails).isNotNull();
      final var instanceAsJson = OBJECT_MAPPER.readTree(actualInstanceDetails);
      final var elementRecord = instanceAsJson.get("elementRecord");
      assertThat(elementRecord.get("key").asLong()).isEqualTo(elementInstanceKey);
      assertThat(elementRecord.get("state").asText())
          .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
      final var processInstanceRecord = elementRecord.get("processInstanceRecord");
      assertThat(processInstanceRecord.get("bpmnProcessId").asText())
          .isEqualTo(processInstanceEvent.getBpmnProcessId());
      assertThat(processInstanceRecord.get("processDefinitionKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessDefinitionKey());
      assertThat(processInstanceRecord.get("version").asInt())
          .isEqualTo(processInstanceEvent.getVersion());
      assertThat(processInstanceRecord.get("processInstanceKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessInstanceKey());
      assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo("task");
      assertThat(processInstanceRecord.get("bpmnElementType").asText())
          .isEqualTo(BpmnElementType.SERVICE_TASK.name());
      assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
      assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
      assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(processInstanceKey);
      assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(0);
    }

    @Test
    public void shouldListElementInstanceDetails() throws JsonProcessingException {
      // given
      final var processInstanceEvent = zeebeContentCreator.processInstanceEvent;
      final var processInstanceKey = processInstanceEvent.getProcessInstanceKey();
      final var elementInstanceKey = 2251799813685263L;
      final var processState =
          new InstanceState(ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1"));
      final var list = new ArrayList<String>();

      // when
      processState.listInstances((key, valueJson) -> list.add(valueJson));

      // then
      assertThat(list).hasSize(3);

      JsonNode instanceAsJson = OBJECT_MAPPER.readTree(list.get(0));
      JsonNode elementRecord = instanceAsJson.get("elementRecord");
      assertThat(elementRecord.get("key").asLong()).isEqualTo(processInstanceKey);
      assertThat(elementRecord.get("state").asText())
          .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
      JsonNode processInstanceRecord = elementRecord.get("processInstanceRecord");
      assertThat(processInstanceRecord.get("bpmnProcessId").asText())
          .isEqualTo(processInstanceEvent.getBpmnProcessId());
      assertThat(processInstanceRecord.get("processDefinitionKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessDefinitionKey());
      assertThat(processInstanceRecord.get("version").asInt())
          .isEqualTo(processInstanceEvent.getVersion());
      assertThat(processInstanceRecord.get("processInstanceKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessInstanceKey());
      assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo("process");
      assertThat(processInstanceRecord.get("bpmnElementType").asText())
          .isEqualTo(BpmnElementType.PROCESS.name());
      assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
      assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
      assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(-1);
      assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(2);

      String elementInstanceJson = list.get(1);
      assertThat(elementInstanceJson).isNotNull();
      instanceAsJson = OBJECT_MAPPER.readTree(elementInstanceJson);
      elementRecord = instanceAsJson.get("elementRecord");
      assertThat(elementRecord.get("key").asLong()).isEqualTo(2251799813685261L);
      assertThat(elementRecord.get("state").asText())
          .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATING.toString());
      processInstanceRecord = elementRecord.get("processInstanceRecord");
      assertThat(processInstanceRecord.get("bpmnProcessId").asText())
          .isEqualTo(processInstanceEvent.getBpmnProcessId());
      assertThat(processInstanceRecord.get("processDefinitionKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessDefinitionKey());
      assertThat(processInstanceRecord.get("version").asInt())
          .isEqualTo(processInstanceEvent.getVersion());
      assertThat(processInstanceRecord.get("processInstanceKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessInstanceKey());
      assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo("incidentTask");
      assertThat(processInstanceRecord.get("bpmnElementType").asText())
          .isEqualTo(BpmnElementType.SERVICE_TASK.name());
      assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
      assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
      assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(processInstanceKey);
      assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(0);

      elementInstanceJson = list.get(2);
      assertThat(elementInstanceJson).isNotNull();
      instanceAsJson = OBJECT_MAPPER.readTree(elementInstanceJson);
      elementRecord = instanceAsJson.get("elementRecord");
      assertThat(elementRecord.get("key").asLong()).isEqualTo(elementInstanceKey);
      assertThat(elementRecord.get("state").asText())
          .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
      processInstanceRecord = elementRecord.get("processInstanceRecord");
      assertThat(processInstanceRecord.get("bpmnProcessId").asText())
          .isEqualTo(processInstanceEvent.getBpmnProcessId());
      assertThat(processInstanceRecord.get("processDefinitionKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessDefinitionKey());
      assertThat(processInstanceRecord.get("version").asInt())
          .isEqualTo(processInstanceEvent.getVersion());
      assertThat(processInstanceRecord.get("processInstanceKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessInstanceKey());
      assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo("task");
      assertThat(processInstanceRecord.get("bpmnElementType").asText())
          .isEqualTo(BpmnElementType.SERVICE_TASK.name());
      assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
      assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
      assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(processInstanceKey);
      assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(0);
    }

    @Test
    public void shouldNotFailOnNonExistingProcessInstance() {
      // given

      // when
      final var processState =
          new InstanceState(ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1"));
      final var actualInstanceDetails = processState.getInstance(0xCAFE);

      // then
      assertThat(actualInstanceDetails).isEqualTo("{}");
    }

    @Test
    public void shouldFindInstancesWithPredicate() {
      // given
      final var instanceState =
          new InstanceState(ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1"));
      final var processInstanceEvent = zeebeContentCreator.processInstanceEvent;
      final var processInstanceKey = processInstanceEvent.getProcessInstanceKey();
      final var processes = new HashMap<Long, String>();

      // when
      instanceState.listProcessInstances(
          processInstanceRecordDetails ->
              processInstanceRecordDetails.getBpmnProcessId().equals("process"),
          (key, valueJson) ->
              processes.put(
                  new UnsafeBuffer(key).getLong(Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER),
                  valueJson));

      // then
      assertThat(processes).containsKey(processInstanceKey);
    }

    @Test
    public void shouldGetIncidentDetails() throws JsonProcessingException {
      // given
      final var runtimePath = ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1");
      final var incidentKey = 2251799813685265L;
      final var processInstanceEvent = zeebeContentCreator.processInstanceEvent;

      // when
      final var incidentState = new IncidentState(runtimePath);
      final var incidentAsJson = incidentState.incidentDetails(incidentKey);

      // then
      assertThat(incidentAsJson).isNotNull();
      final var incident = OBJECT_MAPPER.readTree(incidentAsJson).get("incidentRecord");
      assertThat(incident.get("bpmnProcessId").asText()).isEqualTo("process");
      assertThat(incident.get("processDefinitionKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessDefinitionKey());
      assertThat(incident.get("processInstanceKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessInstanceKey());
      assertThat(incident.get("elementInstanceKey").asLong()).isEqualTo(2251799813685261L);
      assertThat(incident.get("elementId").asText()).isEqualTo("incidentTask");
      assertThat(incident.get("errorMessage").asText())
          .isEqualTo("Expected result of the expression 'foo' to be 'NUMBER', but was 'NULL'.");
      assertThat(incident.get("errorType").asText())
          .isEqualTo(ErrorType.EXTRACT_VALUE_ERROR.toString());
      assertThat(incident.get("variableScopeKey").asLong()).isEqualTo(2251799813685261L);
      assertThat(incident.get("jobKey").asLong()).isEqualTo(-1);
    }

    @Test
    public void shouldListIncidentDetails() throws JsonProcessingException {
      // given
      final var runtimePath = ZeebePaths.Companion.getRuntimePath(TEMP_DIR, "1");
      final var incidentKey = 2251799813685265L;
      final var processInstanceEvent = zeebeContentCreator.processInstanceEvent;
      final var list = new ArrayList<String>();

      // when
      final var incidentState = new IncidentState(runtimePath);
      incidentState.listIncidents(list::add);

      // then
      assertThat(list).hasSize(1);
      final var incidentAsJson = list.get(0);
      assertThat(incidentAsJson).isNotNull();
      final var incident = OBJECT_MAPPER.readTree(incidentAsJson);
      assertThat(incident.get("key").asLong()).isEqualTo(incidentKey);
      final var incidentRecord = incident.get("value").get("incidentRecord");
      assertThat(incidentRecord.get("bpmnProcessId").asText()).isEqualTo("process");
      assertThat(incidentRecord.get("processDefinitionKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessDefinitionKey());
      assertThat(incidentRecord.get("processInstanceKey").asLong())
          .isEqualTo(processInstanceEvent.getProcessInstanceKey());
      assertThat(incidentRecord.get("elementInstanceKey").asLong()).isEqualTo(2251799813685261L);
      assertThat(incidentRecord.get("elementId").asText()).isEqualTo("incidentTask");
      assertThat(incidentRecord.get("errorMessage").asText())
          .isEqualTo("Expected result of the expression 'foo' to be 'NUMBER', but was 'NULL'.");
      assertThat(incidentRecord.get("errorType").asText())
          .isEqualTo(ErrorType.EXTRACT_VALUE_ERROR.toString());
      assertThat(incidentRecord.get("variableScopeKey").asLong()).isEqualTo(2251799813685261L);
      assertThat(incidentRecord.get("jobKey").asLong()).isEqualTo(-1);
      assertThat(incidentRecord.get("jobKey").asLong()).isEqualTo(-1);
    }
  }
}
