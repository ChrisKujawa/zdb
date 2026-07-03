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

import static io.zell.zdb.TestUtils.TIMESTAMP_REGEX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.protocol.record.RecordType;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.zell.zdb.SnapshotFixture;
import io.zell.zdb.SnapshotMetadata;
import io.zell.zdb.ZeebePaths;
import io.zell.zdb.log.LogContentReader;
import io.zell.zdb.log.LogSearch;
import io.zell.zdb.log.LogStatus;
import io.zell.zdb.log.LogWriter;
import io.zell.zdb.log.records.ApplicationRecord;
import io.zell.zdb.log.records.PersistedRecord;
import io.zell.zdb.log.records.RaftRecord;
import io.zell.zdb.log.records.Record;
import io.zell.zdb.state.ZeebeDbReader;
import io.zell.zdb.state.instance.InstanceState;
import io.zell.zdb.state.process.ProcessState;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Migrated from a container-based integration test (Testcontainers spinning up
 * `camunda/camunda:8.8.0`) to run against the pre-committed {@code zeebe-states/v8.8.zip}
 * snapshot produced by {@link SnapshotGeneratorV88Test}. Same coverage — seek/limit/filter log
 * traversal, rejection handling, no-duplicate checks, log search, error paths, and
 * element-instance-by-key — now deterministic and Docker-free.
 */
class Version88Test {

  static final Path SNAPSHOT_ZIP = Path.of("src/test/resources/zeebe-states/v8.8.zip");
  static final String PARTITION = "1";
  static final int MAX_POSITION = 153;
  static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static SnapshotFixture fixture;
  static Path snapshotDir;
  static SnapshotMetadata metadata;

  @BeforeAll
  static void setUp() throws Exception {
    fixture = SnapshotFixture.unzip(SNAPSHOT_ZIP, "v8.8");
    snapshotDir = fixture.snapshotDir();
    metadata = fixture.metadata();
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (fixture != null) {
      fixture.cleanup();
    }
  }

  // ── Log traversal — seek/limit ────────────────────────────────────────────────

  @Test
  void shouldReadLogContentWithIterator() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    verifyCompleteLog(records);
  }

  @Test
  void shouldSkipFirstPartOfLog() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.seekToPosition(10);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(59);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(59);

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
    assertThat(maxIndex).isEqualTo(62);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
    assertThat(minIndex).isEqualTo(4);

    final var maxPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getHighestPosition)
            .max(Long::compareTo)
            .orElseThrow();
    assertThat(maxPosition).isEqualTo(MAX_POSITION);
    final var minPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getLowestPosition)
            .min(Long::compareTo)
            .orElseThrow();
    assertThat(minPosition).isGreaterThan(1);
  }

  @Test
  void shouldNotSkipIfNegativeSeek() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.seekToPosition(-1);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    verifyCompleteLog(records);
  }

  @Test
  void shouldNotSkipIfZeroSeek() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.seekToPosition(0);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    verifyCompleteLog(records);
  }

  @Test
  void shouldSeekToEndOfLogIfNoExistingSeek() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.seekToPosition(Long.MAX_VALUE);

    // when
    logContentReader.forEachRemaining(records::add);

    // then — only the last batch is returned
    assertThat(records).hasSize(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isOne();

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
    assertThat(maxIndex).isEqualTo(62);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
    assertThat(minIndex).isEqualTo(62);

    final var maxPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getHighestPosition)
            .max(Long::compareTo)
            .orElseThrow();
    assertThat(maxPosition).isEqualTo(MAX_POSITION);
    final var minPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getLowestPosition)
            .min(Long::compareTo)
            .orElseThrow();
    assertThat(minPosition).isEqualTo(MAX_POSITION);
  }

  @Test
  void shouldLimitLogToPosition() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.limitToPosition(30);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(4);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isOne();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(3);

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
    assertThat(maxIndex).isEqualTo(4);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
    assertThat(minIndex).isOne();

    final var maxPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getHighestPosition)
            .max(Long::compareTo)
            .orElseThrow();
    assertThat(maxPosition).isGreaterThan(30);
    final var minPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getLowestPosition)
            .min(Long::compareTo)
            .orElseThrow();
    assertThat(minPosition).isOne();
  }

  @Test
  void shouldLimitViaPositionExclusive() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.limitToPosition(1);

    // when
    logContentReader.forEachRemaining(records::add);

    // then — limit=1 stops after the raft record; no app record has lowestPosition < 1
    assertThat(records).hasSize(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isOne();
    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
    assertThat(maxIndex).isOne();
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
    assertThat(minIndex).isOne();
  }

  @Test
  void shouldSeekAndLimitLogWithPosition() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.seekToPosition(5);
    logContentReader.limitToPosition(30);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSizeGreaterThanOrEqualTo(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count())
        .isGreaterThanOrEqualTo(1);

    final var maxPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getHighestPosition)
            .max(Long::compareTo)
            .orElseThrow();
    assertThat(maxPosition).isGreaterThan(30);
    final var minPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getLowestPosition)
            .min(Long::compareTo)
            .orElseThrow();
    assertThat(minPosition).isLessThan(5);
  }

  // ── Log traversal — filter ────────────────────────────────────────────────────

  @Test
  void shouldFilterWithProcessInstanceKey() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();
    final long processInstanceKey = metadata.processInstanceKey();
    logContentReader.filterForProcessInstance(processInstanceKey);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isOne();
    assertThat(records.get(0).toString()).contains(Long.toString(processInstanceKey));
  }

  @Test
  void shouldFilterWithNoExistingProcessInstanceKey() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.filterForProcessInstance(0xCAFE);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).isEmpty();
  }

  @Test
  void shouldFilterWithProcessInstanceKeyAndSetBeginAndEndOfLogPosition() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.filterForProcessInstance(metadata.processInstanceKey());
    logContentReader.seekToPosition(5);
    logContentReader.limitToPosition(100);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isOne();

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
    assertThat(maxIndex).isEqualTo(52);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
    assertThat(minIndex).isEqualTo(52);

    final var maxPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getHighestPosition)
            .max(Long::compareTo)
            .orElseThrow();
    assertThat(maxPosition).isLessThan(MAX_POSITION);
    final var minPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getLowestPosition)
            .min(Long::compareTo)
            .orElseThrow();
    assertThat(minPosition).isGreaterThan(5);
  }

  // ── Log traversal — rejection ─────────────────────────────────────────────────

  @Test
  void shouldReadRejection() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    logContentReader.filterForRejections();

    // when
    final var rejection =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .flatMap(r -> r.getEntries().stream())
            .filter(r -> r.getRejectionType() != RejectionType.NULL_VAL)
            .findFirst();

    // then
    assertThat(rejection).isPresent();
    assertThat(rejection.get().getRejectionType()).isEqualTo(RejectionType.NOT_FOUND);
    assertThat(rejection.get().getRejectionReason())
        .isEqualTo(
            "Expected to find process definition with process ID 'nonExisting', but none found");
  }

  @Test
  void shouldSerializeRejectionToJson() throws JsonProcessingException {
    // given — position 153 is MAX_POSITION (confirmed from log-status golden file)
    final var expectedJson =
        OBJECT_MAPPER.readTree(
            "{\"position\":153,\"sourceRecordPosition\":152,\"key\":-1,\"recordType\":\"COMMAND_REJECTION\","
                + "\"valueType\":\"PROCESS_INSTANCE_CREATION\",\"intent\":\"CREATE\","
                + "\"rejectionType\":\"NOT_FOUND\",\"rejectionReason\":\"Expected to find process definition"
                + " with process ID 'nonExisting', but none found\",\"requestId\":-1,"
                + "\"requestStreamId\":-2147483648,\"protocolVersion\":6,\"brokerVersion\":\"8.8.0\","
                + "\"recordVersion\":1,\"recordValue\":{\"bpmnProcessId\":\"nonExisting\","
                + "\"processDefinitionKey\":0,\"processInstanceKey\":-1,\"version\":-1,"
                + "\"variables\":\"gA==\",\"fetchVariables\":[],\"startInstructions\":[],"
                + "\"runtimeInstructions\":[],\"tenantId\":\"<default>\",\"tags\":[]}}");
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    logContentReader.filterForRejections();

    // when
    final var rejection =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .flatMap(r -> r.getEntries().stream())
            .filter(r -> r.getRejectionType() != RejectionType.NULL_VAL)
            .findFirst();

    // then
    assertThat(rejection).isPresent();
    final var recordJson = rejection.get().toString().replaceFirst(TIMESTAMP_REGEX, "");
    final var actualJson = OBJECT_MAPPER.readTree(recordJson);
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  void shouldSerializeRecordToJson() throws JsonProcessingException {
    // given
    final long processInstanceKey = metadata.processInstanceKey();
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    logContentReader.filterForProcessInstance(processInstanceKey);

    // when — find the ELEMENT_ACTIVATED PROCESS event for the process instance
    final var piActivated =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .flatMap(r -> r.getEntries().stream())
            .filter(r -> r.getValueType() == ValueType.PROCESS_INSTANCE)
            .filter(r -> r.getIntent() == ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .filter(r -> r.getPiRelatedValue() != null)
            .filter(r -> r.getPiRelatedValue().getBpmnElementType() == BpmnElementType.PROCESS)
            .findFirst();

    // then — verify key fields; positions are snapshot-specific and not asserted here
    assertThat(piActivated).isPresent();
    final var record = piActivated.get();
    assertThat(record.getRecordType()).isEqualTo(RecordType.EVENT);
    assertThat(record.getValueType()).isEqualTo(ValueType.PROCESS_INSTANCE);
    assertThat(record.getIntent()).isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED);
    assertThat(record.getKey()).isEqualTo(processInstanceKey);

    final var recordValue =
        OBJECT_MAPPER
            .readTree(record.toString().replaceFirst(TIMESTAMP_REGEX, ""))
            .get("recordValue");
    assertThat(recordValue.get("bpmnElementType").asText()).isEqualTo("PROCESS");
    assertThat(recordValue.get("elementId").asText()).isEqualTo("process");
    assertThat(recordValue.get("processDefinitionKey").asLong())
        .isEqualTo(metadata.firstProcessKey());
    assertThat(recordValue.get("processInstanceKey").asLong()).isEqualTo(processInstanceKey);
    assertThat(recordValue.get("flowScopeKey").asLong()).isEqualTo(-1L);
  }

  // ── Log traversal — table writer ──────────────────────────────────────────────

  @Test
  void shouldConvertRecordToColumn() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.limitToPosition(2);

    // when
    logContentReader.forEachRemaining(records::add);

    // then — first application record (index 2) contains the identity setup command at position 1
    assertThat(records).hasSize(2);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isOne();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isOne();

    final var record = (ApplicationRecord) records.get(1);
    final String columnString = record.asColumnString();
    final String[] elements = columnString.trim().split(" ");
    // format: index(2) term(1) position(1) srcPos(-1) ts key(-1) COMMAND IDENTITY_SETUP INITIALIZE
    assertThat(elements)
        .hasSize(9)
        .containsSubsequence("2", PARTITION, PARTITION, "-1")
        .containsSubsequence("-1", "COMMAND", "IDENTITY_SETUP", "INITIALIZE");
  }

  @Test
  void shouldWriteTableHeaderToStreamWhenNoDataFound() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var outputStream = new ByteArrayOutputStream();
    logContentReader.limitToPosition(30);
    logContentReader.seekToPosition(3);
    logContentReader.filterForProcessInstance(2251799813685254L); // non-existent key
    final var logWriter = new LogWriter(outputStream, logContentReader);

    // when
    logWriter.writeAsTable();

    // then — only the header is written since nothing matched the filter
    assertThat(outputStream.toString().trim())
        .isEqualTo(
            "Index Term Position SourceRecordPosition Timestamp Key RecordType ValueType Intent ProcessInstanceKey BPMNElementType");
  }

  @Test
  void shouldWriteTableToStream() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);
    final var outputStream = new ByteArrayOutputStream();
    final long processInstanceKey = metadata.processInstanceKey();
    logContentReader.limitToPosition(600);
    logContentReader.seekToPosition(6);
    logContentReader.filterForProcessInstance(processInstanceKey);
    final var logWriter = new LogWriter(outputStream, logContentReader);

    // when
    logWriter.writeAsTable();

    // then
    assertThat(outputStream.toString())
        .startsWith(
            "Index Term Position SourceRecordPosition Timestamp Key RecordType ValueType Intent ProcessInstanceKey BPMNElementType")
        .contains(processInstanceKey + 1 + " EVENT VARIABLE CREATED " + processInstanceKey)
        .contains(
            "EVENT PROCESS_INSTANCE ELEMENT_ACTIVATING " + processInstanceKey + " START_EVENT");
  }

  // ── Log — no duplicates ───────────────────────────────────────────────────────

  @Test
  void shouldContainNoDuplicatesInLogContent() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logContentReader = new LogContentReader(logPath);

    // when
    final var content = logContentReader.readAll();

    // then
    assertThat(content.getRecords())
        .filteredOn(ApplicationRecord.class::isInstance)
        .asInstanceOf(InstanceOfAssertFactories.list(ApplicationRecord.class))
        .flatExtracting(ApplicationRecord::getEntries)
        .extracting(Record::getPosition)
        .doesNotHaveDuplicates();
  }

  // ── Log search ────────────────────────────────────────────────────────────────

  @Test
  void shouldSearchPositionInLog() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logSearch = new LogSearch(logPath);

    // when
    final Record record = logSearch.searchPosition(1);

    // then
    assertThat(record).isNotNull();
    assertThat(record.getPosition()).isEqualTo(1);
  }

  @Test
  void shouldReturnNullOnNegPosition() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logSearch = new LogSearch(logPath);

    // when / then
    assertThat(logSearch.searchPosition(-1)).isNull();
  }

  @Test
  void shouldReturnNullOnToBigPosition() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logSearch = new LogSearch(logPath);

    // when / then
    assertThat(logSearch.searchPosition(Long.MAX_VALUE)).isNull();
  }

  @Test
  void shouldSearchIndexInLog() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logSearch = new LogSearch(logPath);

    // when
    final var record = logSearch.searchIndex(7);

    // then
    assertThat(record).isNotNull();
  }

  @Test
  void shouldNotReturnDuplicatesWhenSearchForIndexInLog() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logSearch = new LogSearch(logPath);

    // when
    final var record = logSearch.searchIndex(7);

    // then
    assertThat(record)
        .isNotNull()
        .asInstanceOf(InstanceOfAssertFactories.type(ApplicationRecord.class))
        .extracting(ApplicationRecord::getEntries)
        .asInstanceOf(InstanceOfAssertFactories.list(Record.class))
        .extracting(Record::getPosition)
        .doesNotHaveDuplicates();
  }

  @Test
  void shouldReturnNullOnNegIndex() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logSearch = new LogSearch(logPath);

    // when / then
    assertThat(logSearch.searchIndex(-1)).isNull();
  }

  @Test
  void shouldReturnNullOnToBigIndex() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
    final var logSearch = new LogSearch(logPath);

    // when / then
    assertThat(logSearch.searchIndex(Long.MAX_VALUE)).isNull();
  }

  // ── State — error paths ───────────────────────────────────────────────────────

  @Test
  void shouldThrowWhenReadStatusFromNonExistingLog() {
    // given
    final var logPath =
        ZeebePaths.Companion.getLogPath(new File("/tmp/doesntExist"), PARTITION);

    // when / then
    assertThatThrownBy(() -> new LogStatus(logPath))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expected to read segments, but there was nothing to read");
  }

  @Test
  void shouldNotFailOnNonExistingProcess() {
    // given
    final var runtimePath =
        ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION);
    final var processes = new ArrayList<String>();

    // when
    new ProcessState(runtimePath).processDetails(0xCAFE, (key, valueJson) -> processes.add(valueJson));

    // then
    assertThat(processes).isEmpty();
  }

  @Test
  void shouldNotFailOnNonExistingProcessInstance() {
    // given
    final var runtimePath =
        ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION);

    // when
    final var result = new InstanceState(runtimePath).getInstance(0xCAFE);

    // then
    assertThat(result).isEqualTo("{}");
  }

  // ── State — element instance by element key ───────────────────────────────────

  @Test
  void shouldGetElementInstanceDetails() throws JsonProcessingException {
    // given
    final var runtimePath =
        ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION);
    final long processInstanceKey = metadata.processInstanceKey();
    final long elementInstanceKey = metadata.elementInstanceKey();

    // when
    final var actualInstanceDetails =
        new InstanceState(runtimePath).getInstance(elementInstanceKey);

    // then — elementInstanceKey points to the "task" service task
    assertThat(actualInstanceDetails).isNotNull();
    final var instanceAsJson = OBJECT_MAPPER.readTree(actualInstanceDetails);
    final var elementRecord = instanceAsJson.get("elementRecord");
    assertThat(elementRecord.get("key").asLong()).isEqualTo(elementInstanceKey);
    assertThat(elementRecord.get("state").asText())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
    final var piRecord = elementRecord.get("processInstanceRecord");
    assertThat(piRecord.get("elementId").asText()).isEqualTo("task");
    assertThat(piRecord.get("bpmnElementType").asText())
        .isEqualTo(BpmnElementType.SERVICE_TASK.name());
    assertThat(piRecord.get("processInstanceKey").asLong()).isEqualTo(processInstanceKey);
    assertThat(piRecord.get("flowScopeKey").asLong()).isEqualTo(processInstanceKey);
    assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(0);
  }

  // ── State — statistics assertions ─────────────────────────────────────────────

  @Test
  void shouldCreateStatsForCompleteState() {
    // given
    final var runtimePath =
        ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION);

    // when
    final var cfMap = new ZeebeDbReader(runtimePath).stateStatistics();

    // then
    assertThat(cfMap)
        .containsEntry(ZbColumnFamilies.JOBS.name(), 1)
        .containsEntry(ZbColumnFamilies.VARIABLES.name(), 4)
        .containsEntry(ZbColumnFamilies.INCIDENTS.name(), 1)
        .containsEntry(ZbColumnFamilies.ELEMENT_INSTANCE_KEY.name(), 3);
  }

  // ── Helpers ───────────────────────────────────────────────────────────────────

  private static void verifyCompleteLog(final List<PersistedRecord> records) {
    assertThat(records).hasSize(62);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isOne();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(61);

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).get();
    assertThat(maxIndex).isEqualTo(62);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).get();
    assertThat(minIndex).isOne();

    final var maxPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getHighestPosition)
            .max(Long::compareTo)
            .orElseThrow();
    assertThat(maxPosition).isEqualTo(MAX_POSITION);
    final var minPosition =
        records.stream()
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .map(ApplicationRecord::getLowestPosition)
            .min(Long::compareTo)
            .orElseThrow();
    assertThat(minPosition).isOne();
  }
}
