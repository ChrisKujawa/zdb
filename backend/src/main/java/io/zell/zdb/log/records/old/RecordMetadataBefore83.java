/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under
 * one or more contributor license agreements. See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Licensed under the Zeebe Community License 1.1. You may not use this file
 * except in compliance with the Zeebe Community License 1.1.
 */
package io.zell.zdb.log.records.old;

import io.camunda.zeebe.protocol.Protocol;
import io.camunda.zeebe.protocol.impl.record.VersionInfo;
import io.camunda.zeebe.protocol.record.*;
import io.camunda.zeebe.protocol.record.intent.Intent;
import io.camunda.zeebe.util.VersionUtil;
import io.camunda.zeebe.util.buffer.BufferReader;
import io.camunda.zeebe.util.buffer.BufferUtil;
import io.camunda.zeebe.util.buffer.BufferWriter;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;

public final class RecordMetadataBefore83 implements BufferWriter, BufferReader {
  public static final int BLOCK_LENGTH =
      MessageHeaderEncoder.ENCODED_LENGTH + RecordMetadataEncoderBefore83.BLOCK_LENGTH;

  private static final VersionInfo CURRENT_BROKER_VERSION =
      VersionInfo.parse(VersionUtil.getVersion());

  private final MessageHeaderEncoder headerEncoder = new MessageHeaderEncoder();
  private final MessageHeaderDecoder headerDecoder = new MessageHeaderDecoder();
  private final RecordMetadataEncoderBefore83 encoder = new RecordMetadataEncoderBefore83();
  private final RecordMetadataDecoderBefore83 decoder = new RecordMetadataDecoderBefore83();

  private RecordType recordType = RecordType.NULL_VAL;
  private ValueType valueType = ValueType.NULL_VAL;
  private Intent intent = null;
  private long requestId;
  private short intentValue = Intent.NULL_VAL;
  private int requestStreamId;
  private RejectionType rejectionType;
  private final UnsafeBuffer rejectionReason = new UnsafeBuffer(0, 0);

  // always the current version by default
  private int protocolVersion = 3;
  private VersionInfo brokerVersion = CURRENT_BROKER_VERSION;

  public RecordMetadataBefore83() {
    reset();
  }

  @Override
  public void wrap(final DirectBuffer buffer, int offset, final int length) {
    reset();

    headerDecoder.wrap(buffer, offset);

    offset += headerDecoder.encodedLength();

    decoder.wrap(buffer, offset, headerDecoder.blockLength(), headerDecoder.version());

    recordType = decoder.recordType();
    requestStreamId = decoder.requestStreamId();
    requestId = decoder.requestId();
    protocolVersion = decoder.protocolVersion();
    valueType = decoder.valueType();
    intent = Intent.fromProtocolValue(valueType, decoder.intent());
    rejectionType = decoder.rejectionType();

    brokerVersion =
        Optional.ofNullable(decoder.brokerVersion())
            .map(
                versionDecoder ->
                    new VersionInfo(
                        versionDecoder.majorVersion(),
                        versionDecoder.minorVersion(),
                        versionDecoder.patchVersion()))
            .orElse(VersionInfo.UNKNOWN);

    final int rejectionReasonLength = decoder.rejectionReasonLength();

    if (rejectionReasonLength > 0) {
      offset += headerDecoder.blockLength();
      offset += RecordMetadataDecoderBefore83.rejectionReasonHeaderLength();

      rejectionReason.wrap(buffer, offset, rejectionReasonLength);
    }
  }

  @Override
  public int getLength() {
    return BLOCK_LENGTH
        + RecordMetadataEncoderBefore83.rejectionReasonHeaderLength()
        + rejectionReason.capacity();
  }

  @Override
  public void write(final MutableDirectBuffer buffer, int offset) {
    headerEncoder.wrap(buffer, offset);

    headerEncoder
        .blockLength(encoder.sbeBlockLength())
        .templateId(encoder.sbeTemplateId())
        .schemaId(encoder.sbeSchemaId())
        .version(encoder.sbeSchemaVersion());

    offset += headerEncoder.encodedLength();

    encoder.wrap(buffer, offset);

    encoder
        .recordType(recordType)
        .requestStreamId(requestStreamId)
        .requestId(requestId)
        .protocolVersion(protocolVersion)
        .valueType(valueType)
        .intent(intentValue)
        .rejectionType(rejectionType);

    encoder
        .brokerVersion()
        .majorVersion(brokerVersion.getMajorVersion())
        .minorVersion(brokerVersion.getMinorVersion())
        .patchVersion(brokerVersion.getPatchVersion());

    offset += RecordMetadataEncoderBefore83.BLOCK_LENGTH;

    if (rejectionReason.capacity() > 0) {
      encoder.putRejectionReason(rejectionReason, 0, rejectionReason.capacity());
    } else {
      buffer.putInt(offset, 0);
    }
  }

  public long getRequestId() {
    return requestId;
  }

  public RecordMetadataBefore83 requestId(final long requestId) {
    this.requestId = requestId;
    return this;
  }

  public int getRequestStreamId() {
    return requestStreamId;
  }

  public RecordMetadataBefore83 requestStreamId(final int requestStreamId) {
    this.requestStreamId = requestStreamId;
    return this;
  }

  public RecordMetadataBefore83 protocolVersion(final int protocolVersion) {
    this.protocolVersion = protocolVersion;
    return this;
  }

  public int getProtocolVersion() {
    return protocolVersion;
  }

  public ValueType getValueType() {
    return valueType;
  }

  public RecordMetadataBefore83 valueType(final ValueType eventType) {
    valueType = eventType;
    return this;
  }

  public RecordMetadataBefore83 intent(final Intent intent) {
    this.intent = intent;
    intentValue = intent.value();
    return this;
  }

  public Intent getIntent() {
    return intent;
  }

  public RecordMetadataBefore83 recordType(final RecordType recordType) {
    this.recordType = recordType;
    return this;
  }

  public RecordType getRecordType() {
    return recordType;
  }

  public RecordMetadataBefore83 rejectionType(final RejectionType rejectionType) {
    this.rejectionType = rejectionType;
    return this;
  }

  public RejectionType getRejectionType() {
    return rejectionType;
  }

  public RecordMetadataBefore83 rejectionReason(final String rejectionReason) {
    final byte[] bytes = rejectionReason.getBytes(StandardCharsets.UTF_8);
    this.rejectionReason.wrap(bytes);
    return this;
  }

  public RecordMetadataBefore83 rejectionReason(final DirectBuffer buffer) {
    rejectionReason.wrap(buffer);
    return this;
  }

  public String getRejectionReason() {
    return BufferUtil.bufferAsString(rejectionReason);
  }

  public RecordMetadataBefore83 brokerVersion(final VersionInfo brokerVersion) {
    this.brokerVersion = brokerVersion;
    return this;
  }

  public VersionInfo getBrokerVersion() {
    return brokerVersion;
  }

  public RecordMetadataBefore83 reset() {
    recordType = RecordType.NULL_VAL;
    requestId = RecordMetadataEncoderBefore83.requestIdNullValue();
    requestStreamId = RecordMetadataEncoderBefore83.requestStreamIdNullValue();
    protocolVersion = Protocol.PROTOCOL_VERSION;
    valueType = ValueType.NULL_VAL;
    intentValue = Intent.NULL_VAL;
    intent = null;
    rejectionType = RejectionType.NULL_VAL;
    rejectionReason.wrap(0, 0);
    brokerVersion = CURRENT_BROKER_VERSION;
    return this;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        requestId,
        valueType,
        recordType,
        intentValue,
        requestStreamId,
        rejectionType,
        rejectionReason,
        protocolVersion,
        brokerVersion);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RecordMetadataBefore83 that = (RecordMetadataBefore83) o;
    return requestId == that.requestId
        && intentValue == that.intentValue
        && requestStreamId == that.requestStreamId
        && protocolVersion == that.protocolVersion
        && valueType == that.valueType
        && recordType == that.recordType
        && rejectionType == that.rejectionType
        && rejectionReason.equals(that.rejectionReason)
        && brokerVersion.equals(that.brokerVersion);
  }

  @Override
  public String toString() {
    // The toString is intentionally cut-down to the only important properties for debugging
    // (mostly for tests).
    // If the record is already written to the log (in production) we have other ways to make
    // it readable again.
    final var builder =
        new StringBuilder(
            "RecordMetadata{"
                + "recordType="
                + recordType
                + ", valueType="
                + valueType
                + ", intent="
                + intent);
    if (!rejectionType.equals(RejectionType.NULL_VAL)) {
      builder.append(", rejectionType=").append(rejectionType);
    }
    if (rejectionReason.capacity() > 0) {
      builder.append(", rejectionReason=").append(BufferUtil.bufferAsString(rejectionReason));
    }

    builder.append('}');
    return builder.toString();
  }
}
