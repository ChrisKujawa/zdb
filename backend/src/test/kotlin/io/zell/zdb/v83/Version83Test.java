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
package io.zell.zdb.v83;

import static io.zell.zdb.TestUtils.TIMESTAMP_REGEX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.db.impl.ZeebeDbConstants;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import io.camunda.zeebe.protocol.record.RejectionType;
import io.camunda.zeebe.protocol.record.ValueType;
import io.camunda.zeebe.protocol.record.intent.ProcessInstanceIntent;
import io.camunda.zeebe.protocol.record.value.BpmnElementType;
import io.camunda.zeebe.protocol.record.value.ErrorType;
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
import io.zell.zdb.state.incident.IncidentState;
import io.zell.zdb.state.instance.InstanceState;
import io.zell.zdb.state.process.ProcessState;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.agrona.concurrent.UnsafeBuffer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Migrated from Testcontainers-backed v8.3 tests to a deterministic snapshot fixture at
 * {@code zeebe-states/v8.3.zip}. Coverage stays focused on log traversal, search/filter behavior,
 * state readers, rejection handling, and v8.3-specific element-instance assertions while avoiding
 * Docker in the regular test suite.
 */
class Version83Test {

  static final Path SNAPSHOT_ZIP = Path.of("src/test/resources/zeebe-states/v8.3.zip");
  static final String PARTITION = "1";
  static final int MAX_POSITION = 62;
   static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static SnapshotFixture fixture;
  static Path snapshotDir;
  static SnapshotMetadata metadata;

  @BeforeAll
  static void setUp() throws Exception {
    fixture = SnapshotFixture.unzip(SNAPSHOT_ZIP, "v8.3");
    snapshotDir = fixture.snapshotDir();
    metadata = fixture.metadata();
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (fixture != null) {
      fixture.cleanup();
    }
  }

  @Test
  void shouldReadStatusFromLog() {
    // given
    final var logStatus = new LogStatus(logPath());

    // when
    final var status = logStatus.status();

    // then
    assertThat(status.getHighestIndex()).isEqualTo(15);
    assertThat(status.getHighestTerm()).isEqualTo(1);
    assertThat(status.getHighestRecordPosition()).isEqualTo(MAX_POSITION);
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
  void shouldThrowWhenReadStatusFromNonExistingLog() {
    // given
    final var logPath = ZeebePaths.Companion.getLogPath(new File("/tmp/doesntExist"), PARTITION);

    // when / then
    assertThatThrownBy(() -> new LogStatus(logPath))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expected to read segments, but there was nothing to read");
  }

  @Test
  void shouldBuildLogContent() throws JsonProcessingException {
    // given
    final var logContentReader = new LogContentReader(logPath());

    // when
    final var content = logContentReader.readAll();

    // then
    verifyCompleteLog(content.getRecords());
    assertThat(OBJECT_MAPPER.readTree(content.toString())).isNotNull();
  }

  @Test
  void shouldReadLogContentWithIterator() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    verifyCompleteLog(records);
  }

  @Test
  void shouldReadRejection() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    logContentReader.filterForRejections();

    // when
    final var rejection =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .flatMap(applicationRecord -> applicationRecord.getEntries().stream())
            .filter(record -> record.getRejectionType() != RejectionType.NULL_VAL)
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
    // given
    final var expectedJson =
        OBJECT_MAPPER.readTree(
            """
            {"position":62,"sourceRecordPosition":61,"key":-1,"recordType":"COMMAND_REJECTION",
            "valueType":"PROCESS_INSTANCE_CREATION","intent":"CREATE","rejectionType":"NOT_FOUND",
            "rejectionReason":"Expected to find process definition with process ID 'nonExisting', but none found",
            "requestId":-1,"requestStreamId":-2147483648,"protocolVersion":4,"brokerVersion":"8.3.0",
            "recordVersion":1,
            "recordValue":{"bpmnProcessId":"nonExisting","processDefinitionKey":0,"processInstanceKey":-1,
            "version":-1,"variables":"gA==","fetchVariables":[],"startInstructions":[],"tenantId":"<default>"}}
            """);
    final var logContentReader = new LogContentReader(logPath());
    logContentReader.filterForRejections();

    // when
    final var rejection =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .flatMap(applicationRecord -> applicationRecord.getEntries().stream())
            .filter(record -> record.getRejectionType() != RejectionType.NULL_VAL)
            .findFirst();

    // then
    assertThat(rejection).isPresent();
    final var actualJson = OBJECT_MAPPER.readTree(rejection.get().toString().replaceFirst(TIMESTAMP_REGEX, ""));
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  void shouldSerializeRecordToJson() throws JsonProcessingException {
    // given
    final var expectedJson =
        OBJECT_MAPPER.readTree(
            ("""
            {"position":12,"sourceRecordPosition":5,"key":%d,"recordType":"EVENT",
            "valueType":"PROCESS_INSTANCE","intent":"ELEMENT_ACTIVATED","requestId":-1,
            "requestStreamId":-2147483648,"protocolVersion":4,"brokerVersion":"8.3.0","recordVersion":1,
            "recordValue":{"bpmnElementType":"PROCESS","elementId":"process","bpmnProcessId":"process",
            "version":1,"processDefinitionKey":%d,"processInstanceKey":%d,
            "flowScopeKey":-1,"bpmnEventType":"UNSPECIFIED","parentProcessInstanceKey":-1,
            "parentElementInstanceKey":-1,"tenantId":"<default>"}}
            """)
                .formatted(
                    metadata.processInstanceKey(),
                    metadata.firstProcessKey(),
                    metadata.processInstanceKey()));
    final var logContentReader = new LogContentReader(logPath());
    logContentReader.filterForProcessInstance(metadata.processInstanceKey());

    // when
    final var piActivated =
        StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
            .filter(ApplicationRecord.class::isInstance)
            .map(ApplicationRecord.class::cast)
            .flatMap(applicationRecord -> applicationRecord.getEntries().stream())
            .filter(record -> record.getValueType() == ValueType.PROCESS_INSTANCE)
            .filter(record -> record.getIntent() == ProcessInstanceIntent.ELEMENT_ACTIVATED)
            .filter(record -> record.getPiRelatedValue() != null)
            .filter(
                record ->
                    record.getPiRelatedValue().getBpmnElementType() == BpmnElementType.PROCESS)
            .findFirst();

    // then
    assertThat(piActivated).isPresent();
    final var actualJson = OBJECT_MAPPER.readTree(piActivated.get().toString().replaceFirst(TIMESTAMP_REGEX, ""));
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  void shouldSkipFirstPartOfLog() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.seekToPosition(10);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(11);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(11);

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).orElseThrow();
    assertThat(maxIndex).isEqualTo(15);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).orElseThrow();
    assertThat(minIndex).isEqualTo(5);

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
    assertThat(minPosition).isEqualTo(6);
  }

  @Test
  void shouldNotSkipIfNegativeSeek() {
    // given
    final var logContentReader = new LogContentReader(logPath());
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
    final var logContentReader = new LogContentReader(logPath());
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
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.seekToPosition(Long.MAX_VALUE);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isOne();

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).orElseThrow();
    assertThat(maxIndex).isEqualTo(15);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).orElseThrow();
    assertThat(minIndex).isEqualTo(15);

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
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.limitToPosition(30);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(5);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isOne();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(4);

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).orElseThrow();
    assertThat(maxIndex).isEqualTo(5);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).orElseThrow();
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
  void shouldLimitViaPositionExclusive() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.limitToPosition(1);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isOne();
    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).orElseThrow();
    assertThat(maxIndex).isEqualTo(1);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).orElseThrow();
    assertThat(minIndex).isEqualTo(1);
  }

  @Test
  void shouldConvertRecordToColumn() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.limitToPosition(2);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(2);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isOne();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isOne();

    final var record = (ApplicationRecord) records.get(1);
    final String[] elements = record.asColumnString().trim().split(" ");
    assertThat(elements).hasSize(9);
    assertThat(elements).containsSubsequence("2", "1", "1", "-1");
    assertThat(elements).containsSubsequence("-1", "COMMAND", "DEPLOYMENT", "CREATE");
  }

  @Test
  void shouldWriteTableHeaderToStreamWhenNoDataFound() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var outputStream = new ByteArrayOutputStream();
    logContentReader.limitToPosition(30);
    logContentReader.seekToPosition(3);
    logContentReader.filterForProcessInstance(0xCAFE);
    final var logWriter = new LogWriter(outputStream, logContentReader);

    // when
    logWriter.writeAsTable();

    // then
    assertThat(outputStream.toString().trim())
        .isEqualTo(
            "Index Term Position SourceRecordPosition Timestamp Key RecordType ValueType Intent ProcessInstanceKey BPMNElementType");
  }

  @Test
  void shouldWriteTableToStream() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var outputStream = new ByteArrayOutputStream();
    logContentReader.limitToPosition(600);
    logContentReader.seekToPosition(6);
    logContentReader.filterForProcessInstance(metadata.processInstanceKey());
    final var logWriter = new LogWriter(outputStream, logContentReader);

    // when
    logWriter.writeAsTable();

    // then
    assertThat(outputStream.toString())
        .startsWith(
            "Index Term Position SourceRecordPosition Timestamp Key RecordType ValueType Intent ProcessInstanceKey BPMNElementType")
        .contains((metadata.processInstanceKey() + 1) + " EVENT VARIABLE CREATED " + metadata.processInstanceKey())
        .contains("EVENT PROCESS_INSTANCE ELEMENT_ACTIVATING " + metadata.processInstanceKey() + " START_EVENT");
  }

  @Test
  void shouldSeekAndLimitLogWithPosition() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.seekToPosition(5);
    logContentReader.limitToPosition(30);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(2);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(2);

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).orElseThrow();
    assertThat(maxIndex).isEqualTo(5);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).orElseThrow();
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
  void shouldFilterWithProcessInstanceKey() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.filterForProcessInstance(metadata.processInstanceKey());

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isOne();

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).orElseThrow();
    assertThat(maxIndex).isEqualTo(5);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).orElseThrow();
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
  void shouldFilterWithNoExistingProcessInstanceKey() {
    // given
    final var logContentReader = new LogContentReader(logPath());
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
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.filterForProcessInstance(metadata.processInstanceKey());
    logContentReader.seekToPosition(5);
    logContentReader.limitToPosition(30);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isOne();

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).orElseThrow();
    assertThat(maxIndex).isEqualTo(5);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).orElseThrow();
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
  void shouldReturnLogContentAsDotFile() {
    // given
    final var content = new LogContentReader(logPath()).readAll();

    // when
    final var dotFileContent = content.asDotFile();

    // then
    assertThat(dotFileContent).startsWith("digraph log {").endsWith("}");
  }

  @Test
  void shouldContainNoDuplicatesInLogContent() {
    // given
    final var logContentReader = new LogContentReader(logPath());

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

  @Test
  void shouldSearchPositionInLog() {
    // given
    final var logSearch = new LogSearch(logPath());

    // when
    final Record record = logSearch.searchPosition(1);

    // then
    assertThat(record).isNotNull();
    assertThat(record.getPosition()).isEqualTo(1);
  }

  @Test
  void shouldReturnNullOnNegPosition() {
    // given
    final var logSearch = new LogSearch(logPath());

    // when / then
    assertThat(logSearch.searchPosition(-1)).isNull();
  }

  @Test
  void shouldReturnNullOnToBigPosition() {
    // given
    final var logSearch = new LogSearch(logPath());

    // when / then
    assertThat(logSearch.searchPosition(Long.MAX_VALUE)).isNull();
  }

  @Test
  void shouldSearchIndexInLog() {
    // given
    final var logSearch = new LogSearch(logPath());

    // when
    final var record = logSearch.searchIndex(7);

    // then
    assertThat(record).isNotNull();
  }

  @Test
  void shouldNotReturnDuplicatesWhenSearchForIndexInLog() {
    // given
    final var logSearch = new LogSearch(logPath());

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
    final var logSearch = new LogSearch(logPath());

    // when / then
    assertThat(logSearch.searchIndex(-1)).isNull();
  }

  @Test
  void shouldReturnNullOnToBigIndex() {
    // given
    final var logSearch = new LogSearch(logPath());

    // when / then
    assertThat(logSearch.searchIndex(Long.MAX_VALUE)).isNull();
  }

  @Test
  void shouldCreateStatsForCompleteState() {
    // given
    final var reader = new ZeebeDbReader(runtimePath());

    // when
    final var cfMap = reader.stateStatistics();

    // then
    assertThat(cfMap)
        .containsEntry(ZbColumnFamilies.JOBS.name(), 1)
        .containsEntry(ZbColumnFamilies.VARIABLES.name(), 4)
        .containsEntry(ZbColumnFamilies.INCIDENTS.name(), 1)
        .containsEntry(ZbColumnFamilies.ELEMENT_INSTANCE_KEY.name(), 3);
  }

  @Test
  void shouldVisitValuesAsJson() throws JsonProcessingException {
    // given
    final var reader = new ZeebeDbReader(runtimePath());
    final var incidentMap = new HashMap<String, String>();

    // when
    reader.visitDBWithJsonValues(
        (cf, k, v) -> {
          if (cf.equals(ZbColumnFamilies.INCIDENTS.name())) {
            incidentMap.put(new String(k), v);
          }
        });

    // then
    assertThat(incidentMap).hasSize(1);
    final var incident = OBJECT_MAPPER.readTree(incidentMap.values().iterator().next()).get("incidentRecord");
    assertThat(incident.get("errorType").asText()).isEqualTo(ErrorType.EXTRACT_VALUE_ERROR.toString());
    assertThat(incident.get("errorMessage").asText())
        .isEqualTo("Expected result of the expression 'foo' to be 'NUMBER', but was 'NULL'.");
    assertThat(incident.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(incident.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
    assertThat(incident.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(incident.get("elementId").asText()).isEqualTo("incidentTask");
    assertThat(incident.get("elementInstanceKey").asLong()).isEqualTo(incidentElementInstanceKey());
    assertThat(incident.get("variableScopeKey").asLong()).isEqualTo(incidentElementInstanceKey());
    assertThat(incident.get("jobKey").asLong()).isEqualTo(-1);
  }

  @Test
  void shouldListProcesses() {
    // given
    final var processState = new ProcessState(runtimePath());
    final var processes = new HashMap<Long, String>();

    // when
    processState.listProcesses(
        (key, valueJson) ->
            processes.put(
                new UnsafeBuffer(key)
                    .getLong(key.length - Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER),
                valueJson));

    // then
    assertThat(processes)
        .containsKey(metadata.firstProcessKey())
        .containsKey(metadata.secondProcessKey());
  }

  @Test
  void shouldGetProcessDetails() throws JsonProcessingException {
    // given
    final var processes = new ArrayList<String>();
    final var processState = new ProcessState(runtimePath());

    // when
    processState.processDetails(metadata.firstProcessKey(), (k, v) -> processes.add(v));

    // then
    assertThat(processes).hasSize(1);
    final var jsonNode = OBJECT_MAPPER.readTree(processes.get(0));
    assertThat(jsonNode.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(jsonNode.get("key").asLong()).isEqualTo(metadata.firstProcessKey());
    assertThat(jsonNode.get("resourceName").asText()).isEqualTo("process.bpmn");
    assertThat(jsonNode.get("version").asInt()).isEqualTo(1);
    final var resource =
        new String(Base64.getDecoder().decode(jsonNode.get("resource").asText()), StandardCharsets.UTF_8);
    assertThat(resource)
        .contains("<process id=\"process\" isExecutable=\"true\">")
        .contains("<serviceTask id=\"task\"")
        .contains("<serviceTask id=\"incidentTask\"")
        .contains("<ns0:input source=\"=foo\" target=\"bar\"/>")
        .contains("<ns0:taskDefinition retries=\"=foo\" type=\"type\"/>");
  }

  @Test
  void shouldNotFailOnNonExistingProcess() {
    // given
    final var processes = new ArrayList<String>();

    // when
    new ProcessState(runtimePath()).processDetails(0xCAFE, (k, v) -> processes.add(v));

    // then
    assertThat(processes).isEmpty();
  }

  @Test
  void shouldGetProcessInstanceDetails() throws JsonProcessingException {
    // given
    final var processState = new InstanceState(runtimePath());

    // when
    final var actualInstanceDetails = processState.getInstance(metadata.processInstanceKey());

    // then
    assertThat(actualInstanceDetails).isNotNull();
    final var instanceAsJson = OBJECT_MAPPER.readTree(actualInstanceDetails);
    final var elementRecord = instanceAsJson.get("elementRecord");
    assertThat(elementRecord.get("key").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(elementRecord.get("state").asText())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
    final var processInstanceRecord = elementRecord.get("processInstanceRecord");
    assertThat(processInstanceRecord.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(processInstanceRecord.get("processDefinitionKey").asLong())
        .isEqualTo(metadata.firstProcessKey());
    assertThat(processInstanceRecord.get("version").asInt()).isEqualTo(1);
    assertThat(processInstanceRecord.get("processInstanceKey").asLong())
        .isEqualTo(metadata.processInstanceKey());
    assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo("process");
    assertThat(processInstanceRecord.get("bpmnElementType").asText())
        .isEqualTo(BpmnElementType.PROCESS.name());
    assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(-1);
    assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(2);
  }

  @Test
  void shouldGetElementInstanceDetails() throws JsonProcessingException {
    // given
    final var processState = new InstanceState(runtimePath());

    // when
    final var actualInstanceDetails = processState.getInstance(metadata.elementInstanceKey());

    // then
    assertThat(actualInstanceDetails).isNotNull();
    final var instanceAsJson = OBJECT_MAPPER.readTree(actualInstanceDetails);
    final var elementRecord = instanceAsJson.get("elementRecord");
    assertThat(elementRecord.get("key").asLong()).isEqualTo(metadata.elementInstanceKey());
    assertThat(elementRecord.get("state").asText())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
    final var processInstanceRecord = elementRecord.get("processInstanceRecord");
    assertThat(processInstanceRecord.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(processInstanceRecord.get("processDefinitionKey").asLong())
        .isEqualTo(metadata.firstProcessKey());
    assertThat(processInstanceRecord.get("version").asInt()).isEqualTo(1);
    assertThat(processInstanceRecord.get("processInstanceKey").asLong())
        .isEqualTo(metadata.processInstanceKey());
    assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo("task");
    assertThat(processInstanceRecord.get("bpmnElementType").asText())
        .isEqualTo(BpmnElementType.SERVICE_TASK.name());
    assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("flowScopeKey").asLong())
        .isEqualTo(metadata.processInstanceKey());
    assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(0);
  }

  @Test
  void shouldListElementInstanceDetails() throws JsonProcessingException {
    // given
    final var processState = new InstanceState(runtimePath());
    final var list = new ArrayList<String>();

    // when
    processState.listInstances((key, valueJson) -> list.add(valueJson));

    // then
    assertThat(list).hasSize(3);
    final var instancesByKey = new HashMap<Long, JsonNode>();
    for (final var json : list) {
      final var node = OBJECT_MAPPER.readTree(json);
      instancesByKey.put(node.get("elementRecord").get("key").asLong(), node);
    }

    assertProcessInstanceNode(instancesByKey.get(metadata.processInstanceKey()));

    final var incidentElement = instancesByKey.get(incidentElementInstanceKey());
    assertThat(incidentElement).isNotNull();
    assertThat(incidentElement.get("elementRecord").get("state").asText())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATING.toString());
    final var incidentProcessInstanceRecord = incidentElement.get("elementRecord").get("processInstanceRecord");
    assertThat(incidentProcessInstanceRecord.get("elementId").asText()).isEqualTo("incidentTask");
    assertThat(incidentProcessInstanceRecord.get("bpmnElementType").asText())
        .isEqualTo(BpmnElementType.SERVICE_TASK.name());
    assertThat(incidentProcessInstanceRecord.get("processInstanceKey").asLong())
        .isEqualTo(metadata.processInstanceKey());
    assertThat(incidentProcessInstanceRecord.get("flowScopeKey").asLong())
        .isEqualTo(metadata.processInstanceKey());
    assertThat(incidentElement.get("childCount").asInt()).isEqualTo(0);

    final var taskElement = instancesByKey.get(metadata.elementInstanceKey());
    assertThat(taskElement).isNotNull();
    assertThat(taskElement.get("elementRecord").get("state").asText())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
    final var taskProcessInstanceRecord = taskElement.get("elementRecord").get("processInstanceRecord");
    assertThat(taskProcessInstanceRecord.get("elementId").asText()).isEqualTo("task");
    assertThat(taskProcessInstanceRecord.get("bpmnElementType").asText())
        .isEqualTo(BpmnElementType.SERVICE_TASK.name());
    assertThat(taskProcessInstanceRecord.get("processInstanceKey").asLong())
        .isEqualTo(metadata.processInstanceKey());
    assertThat(taskProcessInstanceRecord.get("flowScopeKey").asLong())
        .isEqualTo(metadata.processInstanceKey());
    assertThat(taskElement.get("childCount").asInt()).isEqualTo(0);
  }

  @Test
  void shouldNotFailOnNonExistingProcessInstance() {
    // given
    final var processState = new InstanceState(runtimePath());

    // when
    final var actualInstanceDetails = processState.getInstance(0xCAFE);

    // then
    assertThat(actualInstanceDetails).isEqualTo("{}");
  }

  @Test
  void shouldFindInstancesWithPredicate() {
    // given
    final var instanceState = new InstanceState(runtimePath());
    final var processes = new HashMap<Long, String>();

    // when
    instanceState.listProcessInstances(
        processInstanceRecordDetails -> processInstanceRecordDetails.getBpmnProcessId().equals("process"),
        (key, valueJson) ->
            processes.put(
                new UnsafeBuffer(key).getLong(Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER),
                valueJson));

    // then
    assertThat(processes).containsKey(metadata.processInstanceKey());
  }

  @Test
  void shouldGetIncidentDetails() throws JsonProcessingException {
    // given
    final var incidentState = new IncidentState(runtimePath());

    // when
    final var incidentAsJson = incidentState.incidentDetails(metadata.incidentKey());

    // then
    assertThat(incidentAsJson).isNotNull();
    final var incident = OBJECT_MAPPER.readTree(incidentAsJson).get("incidentRecord");
    assertThat(incident.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(incident.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
    assertThat(incident.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(incident.get("elementInstanceKey").asLong()).isEqualTo(incidentElementInstanceKey());
    assertThat(incident.get("elementId").asText()).isEqualTo("incidentTask");
    assertThat(incident.get("errorMessage").asText())
        .isEqualTo("Expected result of the expression 'foo' to be 'NUMBER', but was 'NULL'.");
    assertThat(incident.get("errorType").asText()).isEqualTo(ErrorType.EXTRACT_VALUE_ERROR.toString());
    assertThat(incident.get("variableScopeKey").asLong()).isEqualTo(incidentElementInstanceKey());
    assertThat(incident.get("jobKey").asLong()).isEqualTo(-1);
  }

  @Test
  void shouldListIncidentDetails() throws JsonProcessingException {
    // given
    final var incidentState = new IncidentState(runtimePath());
    final var list = new ArrayList<String>();

    // when
    incidentState.listIncidents(list::add);

    // then
    assertThat(list).hasSize(1);
    final var incident = OBJECT_MAPPER.readTree(list.get(0));
    assertThat(incident.get("key").asLong()).isEqualTo(metadata.incidentKey());
    final var incidentRecord = incident.get("value").get("incidentRecord");
    assertThat(incidentRecord.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(incidentRecord.get("processDefinitionKey").asLong())
        .isEqualTo(metadata.firstProcessKey());
    assertThat(incidentRecord.get("processInstanceKey").asLong())
        .isEqualTo(metadata.processInstanceKey());
    assertThat(incidentRecord.get("elementInstanceKey").asLong()).isEqualTo(incidentElementInstanceKey());
    assertThat(incidentRecord.get("elementId").asText()).isEqualTo("incidentTask");
    assertThat(incidentRecord.get("errorMessage").asText())
        .isEqualTo("Expected result of the expression 'foo' to be 'NUMBER', but was 'NULL'.");
    assertThat(incidentRecord.get("errorType").asText())
        .isEqualTo(ErrorType.EXTRACT_VALUE_ERROR.toString());
    assertThat(incidentRecord.get("variableScopeKey").asLong()).isEqualTo(incidentElementInstanceKey());
    assertThat(incidentRecord.get("jobKey").asLong()).isEqualTo(-1);
  }

  private static void verifyCompleteLog(final List<PersistedRecord> records) {
    assertThat(records).hasSize(15);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isOne();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(14);

    final var maxIndex = records.stream().map(PersistedRecord::index).max(Long::compareTo).orElseThrow();
    assertThat(maxIndex).isEqualTo(15);
    final var minIndex = records.stream().map(PersistedRecord::index).min(Long::compareTo).orElseThrow();
    assertThat(minIndex).isEqualTo(1);

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
    assertThat(minPosition).isEqualTo(1);
  }

  private static Path logPath() {
    return ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
  }

  private static Path runtimePath() {
    return ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION);
  }

  private long incidentElementInstanceKey() throws JsonProcessingException {
    return OBJECT_MAPPER
        .readTree(new IncidentState(runtimePath()).incidentDetails(metadata.incidentKey()))
        .get("incidentRecord")
        .get("elementInstanceKey")
        .asLong();
  }

  private void assertProcessInstanceNode(final JsonNode instanceAsJson) {
    assertThat(instanceAsJson).isNotNull();
    final var elementRecord = instanceAsJson.get("elementRecord");
    assertThat(elementRecord.get("key").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(elementRecord.get("state").asText())
        .isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
    final var processInstanceRecord = elementRecord.get("processInstanceRecord");
    assertThat(processInstanceRecord.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(processInstanceRecord.get("processDefinitionKey").asLong())
        .isEqualTo(metadata.firstProcessKey());
    assertThat(processInstanceRecord.get("version").asInt()).isEqualTo(1);
    assertThat(processInstanceRecord.get("processInstanceKey").asLong())
        .isEqualTo(metadata.processInstanceKey());
    assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo("process");
    assertThat(processInstanceRecord.get("bpmnElementType").asText())
        .isEqualTo(BpmnElementType.PROCESS.name());
    assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(-1);
    assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(2);
  }
}
