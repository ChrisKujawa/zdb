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
package io.zell.zdb.v84;

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
import io.zell.zdb.raft.RaftStatus;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

public class Version84Test {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Path SNAPSHOT_ZIP = Path.of("src/test/resources/zeebe-states/v8.4.zip");
  private static final String PARTITION = "1";
  private static final long NON_EXISTING_KEY = 0xCAFE;

  private static SnapshotFixture fixture;
  private static Path snapshotDir;
  private static SnapshotMetadata metadata;
  private static long incidentElementInstanceKey;

  @BeforeAll
  static void setUp() throws Exception {
    fixture = SnapshotFixture.unzip(SNAPSHOT_ZIP, "v8.4");
    snapshotDir = fixture.snapshotDir();
    metadata = fixture.metadata();
    incidentElementInstanceKey = incidentRecord().get("elementInstanceKey").asLong();
  }

  @AfterAll
  static void tearDown() throws Exception {
    if (fixture != null) {
      fixture.cleanup();
    }
  }

  @Nested
  class ZeebeLogTest {

    @Test
    void shouldReadMetaStore() {
      // given
      final var raftStatus = new RaftStatus(logPath());

      // when
      final var status = raftStatus.details();

      // then
      assertThat(status.meta().commitIndex()).as("no commit index in the meta store for 8.4").isZero();
      assertThat(status.meta().term()).isEqualTo(1);
      assertThat(status.meta().lastFlushedIndex()).isEqualTo(15);
      assertThat(status.config().term()).isZero();
      assertThat(status.config().force()).isFalse();
      assertThat(status.config().requiresJointConsensus()).isFalse();
      assertThat(status.config().time()).isPositive();
      assertThat(status.config().newMembers())
          .hasSize(1)
          .first()
          .returns("0", RaftStatus.RaftMemberDetails::id)
          .returns("ACTIVE", RaftStatus.RaftMemberDetails::type)
          .returns(384918240, RaftStatus.RaftMemberDetails::hash);
      assertThat(status.config().oldMembers()).isEmpty();
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
      assertThat(status.getHighestRecordPosition()).isEqualTo(62);
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
      final var path = ZeebePaths.Companion.getLogPath(new File("/tmp/doesntExist"), PARTITION);

      // when - throw
      assertThatThrownBy(() -> new LogStatus(path))
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

      // when
      logContentReader.filterForRejections();

      // then
      final var rejection =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
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
    void shouldSerializeRejectionToJson() throws JsonProcessingException {
      // given
      final var expectedJson =
          OBJECT_MAPPER.readTree(
              """
              {"position":62,"sourceRecordPosition":61,"key":-1,"recordType":"COMMAND_REJECTION",
              "valueType":"PROCESS_INSTANCE_CREATION","intent":"CREATE","rejectionType":"NOT_FOUND",
              "rejectionReason":"Expected to find process definition with process ID 'nonExisting', but none found",
              "requestId":-1,"requestStreamId":-2147483648,"protocolVersion":4,"brokerVersion":"8.4.0",
              "recordVersion":1,
              "recordValue":{"bpmnProcessId":"nonExisting","processDefinitionKey":0,"processInstanceKey":-1,
              "version":-1,"variables":"gA==","fetchVariables":[],
              "startInstructions":[],"tenantId":"<default>"}}
              """);
      final var logContentReader = new LogContentReader(logPath());

      // when
      logContentReader.filterForRejections();

      // then
      final var rejection =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
              .flatMap(applicationRecord -> applicationRecord.getEntries().stream())
              .filter(record -> !record.component8().equals(RejectionType.NULL_VAL.name()))
              .findFirst();

      assertThat(rejection).isPresent();
      final var recordJson = rejection.get().toString().replaceFirst(TIMESTAMP_REGEX, "");
      final var actualJson = OBJECT_MAPPER.readTree(recordJson);
      assertThat(actualJson).isEqualTo(expectedJson);
    }

    @Test
    void shouldSerializeRecordToJson() throws JsonProcessingException {
      // given
      final var expectedJson =
          OBJECT_MAPPER.readTree(
              """
              {"position":12,"sourceRecordPosition":5,"key":%d,"recordType":"EVENT",
              "valueType":"PROCESS_INSTANCE","intent":"ELEMENT_ACTIVATED","requestId":-1,
              "requestStreamId":-2147483648,"protocolVersion":4,"brokerVersion":"8.4.0","recordVersion":1,
              "recordValue":{"bpmnElementType":"PROCESS","elementId":"process","bpmnProcessId":"process",
              "version":1,"processDefinitionKey":%d,"processInstanceKey":%d,
              "flowScopeKey":-1,"bpmnEventType":"UNSPECIFIED","parentProcessInstanceKey":-1,
              "parentElementInstanceKey":-1,"tenantId":"<default>"}}
              """
                  .formatted(
                      metadata.processInstanceKey(), metadata.firstProcessKey(), metadata.processInstanceKey()));
      final var logContentReader = new LogContentReader(logPath());
      logContentReader.filterForProcessInstance(metadata.processInstanceKey());

      // when
      final var piActivated =
          StreamSupport.stream(
                  Spliterators.spliteratorUnknownSize(logContentReader, Spliterator.ORDERED), false)
              .filter(ApplicationRecord.class::isInstance)
              .map(ApplicationRecord.class::cast)
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
      assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(15L);
      assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(5L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getHighestPosition)
                  .max(Long::compareTo))
          .hasValue(62L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getLowestPosition)
                  .min(Long::compareTo))
          .hasValue(6L);
    }

    @Test
    void shouldNotSkipIfNegativeSeek() {
      final var logContentReader = new LogContentReader(logPath());
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(-1);
      logContentReader.forEachRemaining(records::add);
      verifyCompleteLog(records);
    }

    @Test
    void shouldNotSkipIfZeroSeek() {
      final var logContentReader = new LogContentReader(logPath());
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(0);
      logContentReader.forEachRemaining(records::add);
      verifyCompleteLog(records);
    }

    @Test
    void shouldSeekToEndOfLogIfNoExistingSeek() {
      final var logContentReader = new LogContentReader(logPath());
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(Long.MAX_VALUE);
      logContentReader.forEachRemaining(records::add);

      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(15L);
      assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(15L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getHighestPosition)
                  .max(Long::compareTo))
          .hasValue(62L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getLowestPosition)
                  .min(Long::compareTo))
          .hasValue(62L);
    }

    @Test
    void shouldLimitLogToPosition() {
      final var logContentReader = new LogContentReader(logPath());
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.limitToPosition(30);
      logContentReader.forEachRemaining(records::add);

      assertThat(records).hasSize(5);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(4);
      assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(5L);
      assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(1L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getHighestPosition)
                  .max(Long::compareTo))
          .hasValue(34L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getLowestPosition)
                  .min(Long::compareTo))
          .hasValue(1L);
    }

    @Test
    void shouldLimitViaPositionExclusive() {
      final var logContentReader = new LogContentReader(logPath());
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.limitToPosition(1);
      logContentReader.forEachRemaining(records::add);

      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(1L);
      assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(1L);
    }

    @Test
    void shouldConvertRecordToColumn() {
      final var logContentReader = new LogContentReader(logPath());
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.limitToPosition(2);
      logContentReader.forEachRemaining(records::add);

      assertThat(records).hasSize(2);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);

      final var record = (ApplicationRecord) records.get(1);
      final String[] elements = record.asColumnString().trim().split(" ");
      assertThat(elements).hasSize(9);
      assertThat(elements).containsSubsequence("2", "1", "1", "-1");
      assertThat(elements).containsSubsequence("-1", "COMMAND", "DEPLOYMENT", "CREATE");
    }

    @Test
    void shouldWriteTableHeaderToStreamWhenNoDataFound() {
      final var logContentReader = new LogContentReader(logPath());
      final var outputStream = new ByteArrayOutputStream();
      logContentReader.limitToPosition(30);
      logContentReader.seekToPosition(3);
      logContentReader.filterForProcessInstance(NON_EXISTING_KEY);
      final var logWriter = new LogWriter(outputStream, logContentReader);

      logWriter.writeAsTable();

      assertThat(outputStream.toString().trim())
          .isEqualTo(
              "Index Term Position SourceRecordPosition Timestamp Key RecordType ValueType Intent ProcessInstanceKey BPMNElementType");
    }

    @Test
    void shouldWriteTableToStream() {
      final var logContentReader = new LogContentReader(logPath());
      final var outputStream = new ByteArrayOutputStream();
      logContentReader.limitToPosition(600);
      logContentReader.seekToPosition(6);
      logContentReader.filterForProcessInstance(metadata.processInstanceKey());
      final var logWriter = new LogWriter(outputStream, logContentReader);

      logWriter.writeAsTable();

      assertThat(outputStream.toString())
          .startsWith(
              "Index Term Position SourceRecordPosition Timestamp Key RecordType ValueType Intent ProcessInstanceKey BPMNElementType")
          .contains("EVENT VARIABLE CREATED " + metadata.processInstanceKey())
          .contains(
              "EVENT PROCESS_INSTANCE ELEMENT_ACTIVATING "
                  + metadata.processInstanceKey()
                  + " START_EVENT");
    }

    @Test
    void shouldSeekAndLimitLogWithPosition() {
      final var logContentReader = new LogContentReader(logPath());
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.seekToPosition(5);
      logContentReader.limitToPosition(30);
      logContentReader.forEachRemaining(records::add);

      assertThat(records).hasSize(2);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(2);
      assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(5L);
      assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(4L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getHighestPosition)
                  .max(Long::compareTo))
          .hasValue(34L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getLowestPosition)
                  .min(Long::compareTo))
          .hasValue(5L);
    }

    @Test
    void shouldFilterWithProcessInstanceKey() {
      final var logContentReader = new LogContentReader(logPath());
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.filterForProcessInstance(metadata.processInstanceKey());
      logContentReader.forEachRemaining(records::add);

      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(5L);
      assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(5L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getHighestPosition)
                  .max(Long::compareTo))
          .hasValue(34L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getLowestPosition)
                  .min(Long::compareTo))
          .hasValue(6L);
    }

    @Test
    void shouldFilterWithNoExistingProcessInstanceKey() {
      final var logContentReader = new LogContentReader(logPath());
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.filterForProcessInstance(NON_EXISTING_KEY);
      logContentReader.forEachRemaining(records::add);
      assertThat(records).isEmpty();
    }

    @Test
    void shouldFilterWithProcessInstanceKeyAndSetBeginAndEndOfLogPosition() {
      final var logContentReader = new LogContentReader(logPath());
      final var records = new ArrayList<PersistedRecord>();
      logContentReader.filterForProcessInstance(metadata.processInstanceKey());
      logContentReader.seekToPosition(5);
      logContentReader.limitToPosition(30);
      logContentReader.forEachRemaining(records::add);

      assertThat(records).hasSize(1);
      assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
      assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);
      assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(5L);
      assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(5L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getHighestPosition)
                  .max(Long::compareTo))
          .hasValue(34L);
      assertThat(
              records.stream()
                  .filter(ApplicationRecord.class::isInstance)
                  .map(ApplicationRecord.class::cast)
                  .map(ApplicationRecord::getLowestPosition)
                  .min(Long::compareTo))
          .hasValue(6L);
    }

    @Test
    void shouldReturnLogContentAsDotFile() {
      final var content = new LogContentReader(logPath()).readAll();
      assertThat(content.asDotFile()).startsWith("digraph log {").endsWith("}");
    }

    @Test
    void shouldContainNoDuplicatesInLogContent() {
      final var content = new LogContentReader(logPath()).readAll();
      assertThat(content.getRecords())
          .filteredOn(ApplicationRecord.class::isInstance)
          .asInstanceOf(InstanceOfAssertFactories.list(ApplicationRecord.class))
          .flatExtracting(ApplicationRecord::getEntries)
          .extracting(Record::getPosition)
          .doesNotHaveDuplicates();
    }

    @Test
    void shouldSearchPositionInLog() {
      final var logSearch = new LogSearch(logPath());
      final Record record = logSearch.searchPosition(1);
      assertThat(record).isNotNull();
      assertThat(record.getPosition()).isEqualTo(1);
    }

    @Test
    void shouldReturnNullOnNegPosition() {
      assertThat(new LogSearch(logPath()).searchPosition(-1)).isNull();
    }

    @Test
    void shouldReturnNullOnToBigPosition() {
      assertThat(new LogSearch(logPath()).searchPosition(Long.MAX_VALUE)).isNull();
    }

    @Test
    void shouldSearchIndexInLog() {
      assertThat(new LogSearch(logPath()).searchIndex(7)).isNotNull();
    }

    @Test
    void shouldNotReturnDuplicatesWhenSearchForIndexInLog() {
      final var record = new LogSearch(logPath()).searchIndex(7);
      assertThat(record)
          .asInstanceOf(InstanceOfAssertFactories.type(ApplicationRecord.class))
          .extracting(ApplicationRecord::getEntries)
          .asInstanceOf(InstanceOfAssertFactories.list(Record.class))
          .extracting(Record::getPosition)
          .doesNotHaveDuplicates();
    }

    @Test
    void shouldReturnNullOnNegIndex() {
      assertThat(new LogSearch(logPath()).searchIndex(-1)).isNull();
    }

    @Test
    void shouldReturnNullOnToBigIndex() {
      assertThat(new LogSearch(logPath()).searchIndex(Long.MAX_VALUE)).isNull();
    }
  }

  @Nested
  class ZeebeStateTest {

    @Test
    void shouldCreateStatsForCompleteState() {
      final var state = new ZeebeDbReader(runtimePath());
      final var cfMap = state.stateStatistics();

      assertThat(cfMap)
          .containsEntry(ZbColumnFamilies.JOBS.name(), 1)
          .containsEntry(ZbColumnFamilies.VARIABLES.name(), 4)
          .containsEntry(ZbColumnFamilies.INCIDENTS.name(), 1)
          .containsEntry(ZbColumnFamilies.ELEMENT_INSTANCE_KEY.name(), 3);
    }

    @Test
    void shouldVisitValuesAsJson() throws JsonProcessingException {
      final var state = new ZeebeDbReader(runtimePath());
      final var incidentValues = new ArrayList<String>();
      final ZeebeDbReader.JsonValueVisitor jsonVisitor =
          (cf, k, v) -> {
            if (cf.equals(ZbColumnFamilies.INCIDENTS.name())) {
              incidentValues.add(v);
            }
          };

      state.visitDBWithJsonValues(jsonVisitor);

      assertThat(incidentValues).singleElement().satisfies(this::assertIncidentRecordJson);
    }

    @Test
    void shouldListProcesses() {
      final var processState = new ProcessState(runtimePath());
      final var processes = new HashMap<Long, String>();

      processState.listProcesses(
          (key, valueJson) ->
              processes.put(
                  new UnsafeBuffer(key)
                      .getLong(key.length - Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER),
                  valueJson));

      assertThat(processes)
          .containsKey(metadata.firstProcessKey())
          .containsKey(metadata.secondProcessKey());
    }

    @Test
    void shouldGetProcessDetails() throws JsonProcessingException {
      final var processes = new ArrayList<String>();
      final var processState = new ProcessState(runtimePath());

      processState.processDetails(metadata.firstProcessKey(), (k, v) -> processes.add(v));

      assertThat(processes).hasSize(1);
      final var jsonNode = OBJECT_MAPPER.readTree(processes.get(0));
      assertThat(jsonNode.get("bpmnProcessId").asText()).isEqualTo("process");
      assertThat(jsonNode.get("key").asLong()).isEqualTo(metadata.firstProcessKey());
      assertThat(jsonNode.get("resourceName").asText()).isEqualTo("process.bpmn");
      assertThat(jsonNode.get("version").asInt()).isEqualTo(1);
      final var deployedProcess =
          new String(
              Base64.getDecoder().decode(jsonNode.get("resource").asText()),
              StandardCharsets.UTF_8);
      assertThat(deployedProcess)
          .contains("process id=\"process\"")
          .contains("serviceTask id=\"task\"")
          .contains("serviceTask id=\"incidentTask\"")
          .contains("source=\"=foo\" target=\"bar\"")
          .contains("retries=\"=foo\" type=\"incidentTask\"");
    }

    @Test
    void shouldNotFailOnNonExistingProcess() {
      final var processes = new ArrayList<String>();
      new ProcessState(runtimePath()).processDetails(NON_EXISTING_KEY, (k, v) -> processes.add(v));
      assertThat(processes).isEmpty();
    }

    @Test
    void shouldGetProcessInstanceDetails() throws JsonProcessingException {
      final var actualInstanceDetails = new InstanceState(runtimePath()).getInstance(metadata.processInstanceKey());

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
      final var actualInstanceDetails = new InstanceState(runtimePath()).getInstance(metadata.elementInstanceKey());

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
      assertThat(instanceAsJson.get("childCount").asInt()).isZero();
    }

    @Test
    void shouldListElementInstanceDetails() throws JsonProcessingException {
      final var processState = new InstanceState(runtimePath());
      final var list = new ArrayList<String>();

      processState.listInstances((key, valueJson) -> list.add(valueJson));

      assertThat(list).hasSize(3);
      final var instances = list.stream().map(this::readTree).toList();
      assertProcessInstance(instancesByElementId(instances, "process"), ProcessInstanceIntent.ELEMENT_ACTIVATED, 2);
      assertProcessInstance(
          instancesByElementId(instances, "incidentTask"),
          ProcessInstanceIntent.ELEMENT_ACTIVATING,
          0);
      assertProcessInstance(instancesByElementId(instances, "task"), ProcessInstanceIntent.ELEMENT_ACTIVATED, 0);
    }

    @Test
    void shouldNotFailOnNonExistingProcessInstance() {
      assertThat(new InstanceState(runtimePath()).getInstance(NON_EXISTING_KEY)).isEqualTo("{}");
    }

    @Test
    void shouldFindInstancesWithPredicate() {
      final var instanceState = new InstanceState(runtimePath());
      final var processes = new HashMap<Long, String>();

      instanceState.listProcessInstances(
          processInstanceRecordDetails -> processInstanceRecordDetails.getBpmnProcessId().equals("process"),
          (key, valueJson) ->
              processes.put(
                  new UnsafeBuffer(key).getLong(Long.BYTES, ZeebeDbConstants.ZB_DB_BYTE_ORDER),
                  valueJson));

      assertThat(processes).containsKey(metadata.processInstanceKey());
    }

    @Test
    void shouldGetIncidentDetails() throws JsonProcessingException {
      final var incidentAsJson = new IncidentState(runtimePath()).incidentDetails(metadata.incidentKey());

      assertThat(incidentAsJson).isNotNull();
      final var incident = OBJECT_MAPPER.readTree(incidentAsJson).get("incidentRecord");
      assertIncidentRecordNode(incident);
    }

    @Test
    void shouldListIncidentDetails() throws JsonProcessingException {
      final var list = new ArrayList<String>();
      new IncidentState(runtimePath()).listIncidents(list::add);

      assertThat(list).hasSize(1);
      final var incident = OBJECT_MAPPER.readTree(list.get(0));
      assertThat(incident.get("key").asLong()).isEqualTo(metadata.incidentKey());
      assertIncidentRecordNode(incident.get("value").get("incidentRecord"));
    }

    private void assertIncidentRecordJson(final String incidentJson) {
      try {
        assertIncidentRecordNode(OBJECT_MAPPER.readTree(incidentJson).get("incidentRecord"));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    private JsonNode readTree(final String json) {
      try {
        return OBJECT_MAPPER.readTree(json);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }

    private JsonNode instancesByElementId(final List<JsonNode> instances, final String elementId) {
      return instances.stream()
          .filter(instance -> instance.get("elementRecord").get("processInstanceRecord").get("elementId").asText().equals(elementId))
          .findFirst()
          .orElseThrow();
    }

    private void assertProcessInstance(
        final JsonNode instanceAsJson,
        final ProcessInstanceIntent expectedIntent,
        final int expectedChildCount) {
      final var elementRecord = instanceAsJson.get("elementRecord");
      final var processInstanceRecord = elementRecord.get("processInstanceRecord");
      final var elementId = processInstanceRecord.get("elementId").asText();

      assertThat(elementRecord.get("state").asText()).isEqualTo(expectedIntent.toString());
      assertThat(processInstanceRecord.get("bpmnProcessId").asText()).isEqualTo("process");
      assertThat(processInstanceRecord.get("processDefinitionKey").asLong())
          .isEqualTo(metadata.firstProcessKey());
      assertThat(processInstanceRecord.get("version").asInt()).isEqualTo(1);
      assertThat(processInstanceRecord.get("processInstanceKey").asLong())
          .isEqualTo(metadata.processInstanceKey());
      assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
      assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
      assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(expectedChildCount);

      if ("process".equals(elementId)) {
        assertThat(elementRecord.get("key").asLong()).isEqualTo(metadata.processInstanceKey());
        assertThat(processInstanceRecord.get("bpmnElementType").asText())
            .isEqualTo(BpmnElementType.PROCESS.name());
        assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(-1);
      } else if ("incidentTask".equals(elementId)) {
        assertThat(elementRecord.get("key").asLong()).isEqualTo(incidentElementInstanceKey);
        assertThat(processInstanceRecord.get("bpmnElementType").asText())
            .isEqualTo(BpmnElementType.SERVICE_TASK.name());
        assertThat(processInstanceRecord.get("flowScopeKey").asLong())
            .isEqualTo(metadata.processInstanceKey());
      } else {
        assertThat(elementRecord.get("key").asLong()).isEqualTo(metadata.elementInstanceKey());
        assertThat(processInstanceRecord.get("bpmnElementType").asText())
            .isEqualTo(BpmnElementType.SERVICE_TASK.name());
        assertThat(processInstanceRecord.get("flowScopeKey").asLong())
            .isEqualTo(metadata.processInstanceKey());
      }
    }
  }

  private static void verifyCompleteLog(final List<PersistedRecord> records) {
    assertThat(records).hasSize(15);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(14);
    assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(15L);
    assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(1L);
    assertThat(
            records.stream()
                .filter(ApplicationRecord.class::isInstance)
                .map(ApplicationRecord.class::cast)
                .map(ApplicationRecord::getHighestPosition)
                .max(Long::compareTo))
        .hasValue(62L);
    assertThat(
            records.stream()
                .filter(ApplicationRecord.class::isInstance)
                .map(ApplicationRecord.class::cast)
                .map(ApplicationRecord::getLowestPosition)
                .min(Long::compareTo))
        .hasValue(1L);
  }

  private static Path runtimePath() {
    return ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION);
  }

  private static Path logPath() {
    return ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
  }

  private static JsonNode incidentRecord() throws JsonProcessingException {
    return OBJECT_MAPPER
        .readTree(new IncidentState(runtimePath()).incidentDetails(metadata.incidentKey()))
        .get("incidentRecord");
  }

  private static void assertIncidentRecordNode(final JsonNode incident) {
    assertThat(incident.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(incident.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
    assertThat(incident.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(incident.get("elementInstanceKey").asLong()).isEqualTo(incidentElementInstanceKey);
    assertThat(incident.get("elementId").asText()).isEqualTo("incidentTask");
    assertThat(incident.get("errorMessage").asText())
        .isEqualTo(
            """
            Expected result of the expression 'foo' to be 'NUMBER', but was 'NULL'. The evaluation reported the following warnings:
            [NO_VARIABLE_FOUND] No variable found with name 'foo'""");
    assertThat(incident.get("errorType").asText()).isEqualTo(ErrorType.EXTRACT_VALUE_ERROR.toString());
    assertThat(incident.get("variableScopeKey").asLong()).isEqualTo(incidentElementInstanceKey);
    assertThat(incident.get("jobKey").asLong()).isEqualTo(-1);
  }
}
