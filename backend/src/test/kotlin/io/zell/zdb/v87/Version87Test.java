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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;
import org.agrona.concurrent.UnsafeBuffer;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Migrated from the container-based v8.7 integration tests to run against the committed
 * {@code zeebe-states/v8.7.zip} snapshot produced by {@link SnapshotGeneratorV87Test}.
 */
public class Version87Test {

  private static final Path SNAPSHOT_ZIP = Path.of("src/test/resources/zeebe-states/v8.7.zip");
  private static final String PARTITION = "1";
  private static final int MAX_POSITION = 63;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static SnapshotFixture fixture;
  private static Path snapshotDir;
  private static SnapshotMetadata metadata;

  @BeforeAll
  public static void setUp() throws Exception {
    fixture = SnapshotFixture.unzip(SNAPSHOT_ZIP, "v8.7");
    snapshotDir = fixture.snapshotDir();
    metadata = fixture.metadata();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    if (fixture != null) {
      fixture.cleanup();
    }
  }

  @Test
  public void shouldReadMetaStore() {
    // given
    final var raftStatus = new RaftStatus(logPath());

    // when
    final var status = raftStatus.details();

    // then
    assertThat(status.meta().commitIndex()).as("no commit index in the meta store for 8.7").isZero();
    assertThat(status.meta().term()).isEqualTo(1);
    assertThat(status.meta().lastFlushedIndex()).isEqualTo(15);
    assertThat(status.config().term()).isZero();
    assertThat(status.config().force()).isFalse();
    assertThat(status.config().requiresJointConsensus()).isFalse();
    assertThat(status.config().time())
        .isBetween(Instant.now().minusSeconds(365L * 24 * 60 * 60).toEpochMilli(), Instant.now().toEpochMilli());
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
  public void shouldThrowWhenReadStatusFromNonExistingLog() {
    // given
    final var missingLogPath = ZeebePaths.Companion.getLogPath(new File("/tmp/doesntExist"), PARTITION);

    // when / then
    assertThatThrownBy(() -> new LogStatus(missingLogPath))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Expected to read segments, but there was nothing to read");
  }

  @Test
  public void shouldBuildLogContent() throws JsonProcessingException {
    // given
    final var logContentReader = new LogContentReader(logPath());

    // when
    final var content = logContentReader.readAll();

    // then
    verifyCompleteLog(content.getRecords());
    assertThat(OBJECT_MAPPER.readTree(content.toString())).isNotNull();
  }

  @Test
  public void shouldReadLogContentWithIterator() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    verifyCompleteLog(records);
  }

  @Test
  public void shouldReadRejection() {
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
            .filter(record -> record.component8() != RejectionType.NULL_VAL)
            .findFirst();

    // then
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
            "requestId":-1,"requestStreamId":-2147483648,"protocolVersion":5,"brokerVersion":"8.7.0",
            "recordVersion":1,
            "recordValue":{"bpmnProcessId":"nonExisting","processDefinitionKey":0,"processInstanceKey":-1,
            "version":-1,"variables":"gA==","fetchVariables":[],
            "startInstructions":[],"tenantId":"<default>"}}
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
            .filter(record -> !record.component8().equals(RejectionType.NULL_VAL.name()))
            .findFirst();

    // then
    assertThat(rejection).isPresent();
    final var actualJson = OBJECT_MAPPER.readTree(rejection.get().toString().replaceFirst(TIMESTAMP_REGEX, ""));
    assertThat(actualJson).isEqualTo(expectedJson);
  }

  @Test
  public void shouldSerializeRecordToJson() throws JsonProcessingException {
    // given
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
            .filter(record -> record.getPiRelatedValue().getBpmnElementType() == BpmnElementType.PROCESS)
            .findFirst();

    // then
    assertThat(piActivated).isPresent();
    final var actualJson = OBJECT_MAPPER.readTree(piActivated.get().toString().replaceFirst(TIMESTAMP_REGEX, ""));
    final var recordValue = actualJson.get("recordValue");
    assertThat(actualJson.get("valueType").asText()).isEqualTo(ValueType.PROCESS_INSTANCE.name());
    assertThat(actualJson.get("intent").asText()).isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.name());
    assertThat(recordValue.get("bpmnElementType").asText()).isEqualTo(BpmnElementType.PROCESS.name());
    assertThat(recordValue.get("elementId").asText()).isEqualTo("process");
    assertThat(recordValue.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
    assertThat(recordValue.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(recordValue.get("flowScopeKey").asLong()).isEqualTo(-1L);
  }

  @Test
  public void shouldSkipFirstPartOfLog() {
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
    assertThat(maxApplicationPosition(records)).isEqualTo(MAX_POSITION);
    assertThat(minApplicationPosition(records)).isEqualTo(6L);
  }

  @Test
  public void shouldNotSkipIfNegativeSeek() {
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
  public void shouldNotSkipIfZeroSeek() {
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
  public void shouldSeekToEndOfLogIfNoExistingSeek() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.seekToPosition(Long.MAX_VALUE);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);
    assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(15L);
    assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(15L);
    assertThat(maxApplicationPosition(records)).isEqualTo(MAX_POSITION);
    assertThat(minApplicationPosition(records)).isEqualTo(MAX_POSITION);
  }

  @Test
  public void shouldLimitLogToPosition() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.limitToPosition(30);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(5);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(4);
    assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(5L);
    assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(1L);
    assertThat(maxApplicationPosition(records)).isEqualTo(34L);
    assertThat(minApplicationPosition(records)).isEqualTo(1L);
  }

  @Test
  public void shouldLimitViaPositionExclusive() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.limitToPosition(1);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
    assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(1L);
    assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(1L);
  }

  @Test
  public void shouldConvertRecordToColumn() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.limitToPosition(2);

    // when
    logContentReader.forEachRemaining(records::add);

    // then
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
  public void shouldWriteTableHeaderToStreamWhenNoDataFound() {
    // given
    final var logContentReader = new LogContentReader(logPath());
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
  public void shouldSeekAndLimitLogWithPosition() {
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
    assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(5L);
    assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(4L);
    assertThat(maxApplicationPosition(records)).isEqualTo(34L);
    assertThat(minApplicationPosition(records)).isEqualTo(5L);
  }

  @Test
  public void shouldFilterWithProcessInstanceKey() {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var records = new ArrayList<PersistedRecord>();
    logContentReader.filterForProcessInstance(metadata.processInstanceKey());

    // when
    logContentReader.forEachRemaining(records::add);

    // then
    assertThat(records).hasSize(1);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isZero();
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);
    assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(5L);
    assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(5L);
    assertThat(maxApplicationPosition(records)).isEqualTo(34L);
    assertThat(minApplicationPosition(records)).isEqualTo(6L);
  }

  @Test
  public void shouldFilterWithNoExistingProcessInstanceKey() {
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
  public void shouldFilterWithProcessInstanceKeyAndSetBeginAndEndOfLogPosition() {
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
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(1);
    assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(5L);
    assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(5L);
    assertThat(maxApplicationPosition(records)).isEqualTo(34L);
    assertThat(minApplicationPosition(records)).isEqualTo(6L);
  }

  @Test
  public void shouldReturnLogContentAsDotFile() throws JsonProcessingException {
    // given
    final var logContentReader = new LogContentReader(logPath());
    final var content = logContentReader.readAll();

    // when
    final var dotFileContent = content.asDotFile();

    // then
    assertThat(dotFileContent).startsWith("digraph log {").endsWith("}");
  }

  @Test
  public void shouldContainNoDuplicatesInLogContent() {
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
  public void shouldSearchPositionInLog() {
    // given
    final var logSearch = new LogSearch(logPath());

    // when
    final Record record = logSearch.searchPosition(1);

    // then
    assertThat(record).isNotNull();
    assertThat(record.getPosition()).isEqualTo(1);
  }

  @Test
  public void shouldReturnNullOnNegPosition() {
    // given
    final var logSearch = new LogSearch(logPath());

    // when
    final Record record = logSearch.searchPosition(-1);

    // then
    assertThat(record).isNull();
  }

  @Test
  public void shouldReturnNullOnToBigPosition() {
    // given
    final var logSearch = new LogSearch(logPath());

    // when
    final Record record = logSearch.searchPosition(Long.MAX_VALUE);

    // then
    assertThat(record).isNull();
  }

  @Test
  public void shouldSearchIndexInLog() {
    // given
    final var logSearch = new LogSearch(logPath());

    // when
    final var record = logSearch.searchIndex(7);

    // then
    assertThat(record).isNotNull();
  }

  @Test
  public void shouldNotReturnDuplicatesWhenSearchForIndexInLog() {
    // given
    final var logSearch = new LogSearch(logPath());

    // when
    final var record = logSearch.searchIndex(7);

    // then
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
    final var logSearch = new LogSearch(logPath());

    // when
    final var logContent = logSearch.searchIndex(-1);

    // then
    assertThat(logContent).isNull();
  }

  @Test
  public void shouldReturnNullOnToBigIndex() {
    // given
    final var logSearch = new LogSearch(logPath());

    // when
    final var logContent = logSearch.searchIndex(Long.MAX_VALUE);

    // then
    assertThat(logContent).isNull();
  }

  @Test
  public void shouldCreateStatsForCompleteState() {
    // given
    final var experimental = new ZeebeDbReader(runtimePath());

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
  public void shouldVisitValuesAsJson() throws JsonProcessingException {
    // given
    final var experimental = new ZeebeDbReader(runtimePath());
    final var incidents = new ArrayList<String>();

    // when
    experimental.visitDBWithJsonValues(
        (cf, k, v) -> {
          if (cf.equals(ZbColumnFamilies.INCIDENTS.name())) {
            incidents.add(v);
          }
        });

    // then
    assertThat(incidents).hasSize(1);
    final var incident = OBJECT_MAPPER.readTree(incidents.get(0)).get("incidentRecord");
    final long incidentElementInstanceKey = incidentElementInstanceKey();
    assertThat(incident.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(incident.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
    assertThat(incident.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(incident.get("elementInstanceKey").asLong()).isEqualTo(incidentElementInstanceKey);
    assertThat(incident.get("elementId").asText()).isEqualTo("incidentTask");
    assertThat(incident.get("errorType").asText()).isEqualTo(ErrorType.EXTRACT_VALUE_ERROR.toString());
    assertThat(incident.get("variableScopeKey").asLong()).isEqualTo(incidentElementInstanceKey);
  }

  @Test
  public void shouldListProcesses() {
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
    assertThat(processes).containsKeys(metadata.firstProcessKey(), metadata.secondProcessKey());
  }

  @Test
  public void shouldGetProcessDetails() throws JsonProcessingException {
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
    assertThat(new String(Base64.getDecoder().decode(jsonNode.get("resource").asText()), StandardCharsets.UTF_8))
        .contains("process")
        .contains("incidentTask")
        .contains("taskDefinition type=\"type\"")
        .contains("taskDefinition retries=\"=foo\" type=\"incidentTask\"");
  }

  @Test
  public void shouldNotFailOnNonExistingProcess() {
    // given
    final var processes = new ArrayList<String>();

    // when
    new ProcessState(runtimePath()).processDetails(0xCAFE, (k, v) -> processes.add(v));

    // then
    assertThat(processes).isEmpty();
  }

  @Test
  public void shouldGetProcessInstanceDetails() throws JsonProcessingException {
    // given
    final var actualInstanceDetails = new InstanceState(runtimePath()).getInstance(metadata.processInstanceKey());

    // then
    assertThat(actualInstanceDetails).isNotNull();
    final var instanceAsJson = OBJECT_MAPPER.readTree(actualInstanceDetails);
    final var elementRecord = instanceAsJson.get("elementRecord");
    assertThat(elementRecord.get("key").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(elementRecord.get("state").asText()).isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
    final var processInstanceRecord = elementRecord.get("processInstanceRecord");
    assertThat(processInstanceRecord.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(processInstanceRecord.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
    assertThat(processInstanceRecord.get("version").asInt()).isEqualTo(1);
    assertThat(processInstanceRecord.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo("process");
    assertThat(processInstanceRecord.get("bpmnElementType").asText()).isEqualTo(BpmnElementType.PROCESS.name());
    assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(-1);
    assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(2);
  }

  @Test
  public void shouldGetElementInstanceDetails() throws JsonProcessingException {
    // given
    final var actualInstanceDetails = new InstanceState(runtimePath()).getInstance(metadata.elementInstanceKey());

    // then
    assertThat(actualInstanceDetails).isNotNull();
    final var instanceAsJson = OBJECT_MAPPER.readTree(actualInstanceDetails);
    final var elementRecord = instanceAsJson.get("elementRecord");
    assertThat(elementRecord.get("key").asLong()).isEqualTo(metadata.elementInstanceKey());
    assertThat(elementRecord.get("state").asText()).isEqualTo(ProcessInstanceIntent.ELEMENT_ACTIVATED.toString());
    final var processInstanceRecord = elementRecord.get("processInstanceRecord");
    assertThat(processInstanceRecord.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(processInstanceRecord.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
    assertThat(processInstanceRecord.get("version").asInt()).isEqualTo(1);
    assertThat(processInstanceRecord.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo("task");
    assertThat(processInstanceRecord.get("bpmnElementType").asText()).isEqualTo(BpmnElementType.SERVICE_TASK.name());
    assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(0);
  }

  @Test
  public void shouldListElementInstanceDetails() throws JsonProcessingException {
    // given
    final var processState = new InstanceState(runtimePath());
    final var list = new ArrayList<String>();

    // when
    processState.listInstances((key, valueJson) -> list.add(valueJson));

    // then
    assertThat(list).hasSize(3);
    final var byKey = new HashMap<Long, JsonNode>();
    for (final var json : list) {
      final var instance = OBJECT_MAPPER.readTree(json);
      byKey.put(instance.get("elementRecord").get("key").asLong(), instance);
    }

    assertInstance(
        byKey.get(metadata.processInstanceKey()),
        metadata.processInstanceKey(),
        ProcessInstanceIntent.ELEMENT_ACTIVATED,
        "process",
        BpmnElementType.PROCESS,
        metadata.processInstanceKey(),
        -1L,
        2);
    assertInstance(
        byKey.get(incidentElementInstanceKey()),
        incidentElementInstanceKey(),
        ProcessInstanceIntent.ELEMENT_ACTIVATING,
        "incidentTask",
        BpmnElementType.SERVICE_TASK,
        metadata.processInstanceKey(),
        metadata.processInstanceKey(),
        0);
    assertInstance(
        byKey.get(metadata.elementInstanceKey()),
        metadata.elementInstanceKey(),
        ProcessInstanceIntent.ELEMENT_ACTIVATED,
        "task",
        BpmnElementType.SERVICE_TASK,
        metadata.processInstanceKey(),
        metadata.processInstanceKey(),
        0);
  }

  @Test
  public void shouldNotFailOnNonExistingProcessInstance() {
    // when
    final var actualInstanceDetails = new InstanceState(runtimePath()).getInstance(0xCAFE);

    // then
    assertThat(actualInstanceDetails).isEqualTo("{}");
  }

  @Test
  public void shouldFindInstancesWithPredicate() {
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
  public void shouldGetIncidentDetails() throws JsonProcessingException {
    // when
    final var incidentAsJson = new IncidentState(runtimePath()).incidentDetails(metadata.incidentKey());

    // then
    assertThat(incidentAsJson).isNotNull();
    final var incident = OBJECT_MAPPER.readTree(incidentAsJson).get("incidentRecord");
    assertThat(incident.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(incident.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
    assertThat(incident.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(incident.get("elementInstanceKey").asLong()).isEqualTo(incidentElementInstanceKey());
    assertThat(incident.get("elementId").asText()).isEqualTo("incidentTask");
    assertThat(incident.get("errorMessage").asText())
        .isEqualTo(
            """
            Expected result of the expression 'foo' to be 'NUMBER', but was 'NULL'. The evaluation reported the following warnings:
            [NO_VARIABLE_FOUND] No variable found with name 'foo'""");
    assertThat(incident.get("errorType").asText()).isEqualTo(ErrorType.EXTRACT_VALUE_ERROR.toString());
    assertThat(incident.get("variableScopeKey").asLong()).isEqualTo(incidentElementInstanceKey());
    assertThat(incident.get("jobKey").asLong()).isEqualTo(-1);
  }

  @Test
  public void shouldListIncidentDetails() throws JsonProcessingException {
    // given
    final var list = new ArrayList<String>();

    // when
    new IncidentState(runtimePath()).listIncidents(list::add);

    // then
    assertThat(list).hasSize(1);
    final var incidentAsJson = list.get(0);
    assertThat(incidentAsJson).isNotNull();
    final var incident = OBJECT_MAPPER.readTree(incidentAsJson);
    assertThat(incident.get("key").asLong()).isEqualTo(metadata.incidentKey());
    final var incidentRecord = incident.get("value").get("incidentRecord");
    assertThat(incidentRecord.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(incidentRecord.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
    assertThat(incidentRecord.get("processInstanceKey").asLong()).isEqualTo(metadata.processInstanceKey());
    assertThat(incidentRecord.get("elementInstanceKey").asLong()).isEqualTo(incidentElementInstanceKey());
    assertThat(incidentRecord.get("elementId").asText()).isEqualTo("incidentTask");
    assertThat(incidentRecord.get("errorMessage").asText())
        .isEqualTo(
            """
            Expected result of the expression 'foo' to be 'NUMBER', but was 'NULL'. The evaluation reported the following warnings:
            [NO_VARIABLE_FOUND] No variable found with name 'foo'""");
    assertThat(incidentRecord.get("errorType").asText()).isEqualTo(ErrorType.EXTRACT_VALUE_ERROR.toString());
    assertThat(incidentRecord.get("variableScopeKey").asLong()).isEqualTo(incidentElementInstanceKey());
    assertThat(incidentRecord.get("jobKey").asLong()).isEqualTo(-1);
  }

  private static void verifyCompleteLog(final List<PersistedRecord> records) {
    assertThat(records).hasSize(15);
    assertThat(records.stream().filter(RaftRecord.class::isInstance).count()).isEqualTo(1);
    assertThat(records.stream().filter(ApplicationRecord.class::isInstance).count()).isEqualTo(14);
    assertThat(records.stream().map(PersistedRecord::index).max(Long::compareTo)).hasValue(15L);
    assertThat(records.stream().map(PersistedRecord::index).min(Long::compareTo)).hasValue(1L);
    assertThat(maxApplicationPosition(records)).isEqualTo(MAX_POSITION);
    assertThat(minApplicationPosition(records)).isEqualTo(1L);
  }

  private static Path runtimePath() {
    return ZeebePaths.Companion.getRuntimePath(snapshotDir.toFile(), PARTITION);
  }

  private static Path logPath() {
    return ZeebePaths.Companion.getLogPath(snapshotDir.toFile(), PARTITION);
  }

  private static long maxApplicationPosition(final List<PersistedRecord> records) {
    return records.stream()
        .filter(ApplicationRecord.class::isInstance)
        .map(ApplicationRecord.class::cast)
        .map(ApplicationRecord::getHighestPosition)
        .max(Long::compareTo)
        .orElseThrow();
  }

  private static long minApplicationPosition(final List<PersistedRecord> records) {
    return records.stream()
        .filter(ApplicationRecord.class::isInstance)
        .map(ApplicationRecord.class::cast)
        .map(ApplicationRecord::getLowestPosition)
        .min(Long::compareTo)
        .orElseThrow();
  }

  private static long incidentElementInstanceKey() throws JsonProcessingException {
    return OBJECT_MAPPER
        .readTree(new IncidentState(runtimePath()).incidentDetails(metadata.incidentKey()))
        .get("incidentRecord")
        .get("elementInstanceKey")
        .asLong();
  }

  private static void assertInstance(
      final JsonNode instanceAsJson,
      final long key,
      final ProcessInstanceIntent state,
      final String elementId,
      final BpmnElementType bpmnElementType,
      final long processInstanceKey,
      final long flowScopeKey,
      final int childCount) {
    assertThat(instanceAsJson).isNotNull();
    final var elementRecord = instanceAsJson.get("elementRecord");
    assertThat(elementRecord.get("key").asLong()).isEqualTo(key);
    assertThat(elementRecord.get("state").asText()).isEqualTo(state.toString());
    final var processInstanceRecord = elementRecord.get("processInstanceRecord");
    assertThat(processInstanceRecord.get("bpmnProcessId").asText()).isEqualTo("process");
    assertThat(processInstanceRecord.get("processDefinitionKey").asLong()).isEqualTo(metadata.firstProcessKey());
    assertThat(processInstanceRecord.get("version").asInt()).isEqualTo(1);
    assertThat(processInstanceRecord.get("processInstanceKey").asLong()).isEqualTo(processInstanceKey);
    assertThat(processInstanceRecord.get("elementId").asText()).isEqualTo(elementId);
    assertThat(processInstanceRecord.get("bpmnElementType").asText()).isEqualTo(bpmnElementType.name());
    assertThat(processInstanceRecord.get("parentProcessInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("parentElementInstanceKey").asLong()).isEqualTo(-1);
    assertThat(processInstanceRecord.get("flowScopeKey").asLong()).isEqualTo(flowScopeKey);
    assertThat(instanceAsJson.get("childCount").asInt()).isEqualTo(childCount);
  }
}
