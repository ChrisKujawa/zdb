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
/* Generated SBE (Simple Binary Encoding) message codec. */
package io.zell.zdb.log.records.old;

import io.camunda.zeebe.protocol.record.*;
import org.agrona.MutableDirectBuffer;
import org.agrona.DirectBuffer;
import org.agrona.sbe.*;


/**
 * Descriptor for Record Metadata
 */
@SuppressWarnings("all")
public final class RecordMetadataEncoderBefore83 implements MessageEncoderFlyweight
{
    public static final int BLOCK_LENGTH = 30;
    public static final int TEMPLATE_ID = 200;
    public static final int SCHEMA_ID = 0;
    public static final int SCHEMA_VERSION = 3;
    public static final String SEMANTIC_VERSION = "8.2.15";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final RecordMetadataEncoderBefore83 parentMessage = this;
    private MutableDirectBuffer buffer;
    private int initialOffset;
    private int offset;
    private int limit;

    public int sbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public int sbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public String sbeSemanticType()
    {
        return "";
    }

    public MutableDirectBuffer buffer()
    {
        return buffer;
    }

    public int initialOffset()
    {
        return initialOffset;
    }

    public int offset()
    {
        return offset;
    }

    public RecordMetadataEncoderBefore83 wrap(final MutableDirectBuffer buffer, final int offset)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.initialOffset = offset;
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);

        return this;
    }

    public RecordMetadataEncoderBefore83 wrapAndApplyHeader(
        final MutableDirectBuffer buffer, final int offset, final MessageHeaderEncoder headerEncoder)
    {
        headerEncoder
            .wrap(buffer, offset)
            .blockLength(BLOCK_LENGTH)
            .templateId(TEMPLATE_ID)
            .schemaId(SCHEMA_ID)
            .version(SCHEMA_VERSION);

        return wrap(buffer, offset + MessageHeaderEncoder.ENCODED_LENGTH);
    }

    public int encodedLength()
    {
        return limit - offset;
    }

    public int limit()
    {
        return limit;
    }

    public void limit(final int limit)
    {
        this.limit = limit;
    }

    public static int recordTypeId()
    {
        return 1;
    }

    public static int recordTypeSinceVersion()
    {
        return 0;
    }

    public static int recordTypeEncodingOffset()
    {
        return 0;
    }

    public static int recordTypeEncodingLength()
    {
        return 1;
    }

    public static String recordTypeMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public RecordMetadataEncoderBefore83 recordType(final RecordType value)
    {
        buffer.putByte(offset + 0, (byte)value.value());
        return this;
    }

    public static int requestStreamIdId()
    {
        return 2;
    }

    public static int requestStreamIdSinceVersion()
    {
        return 0;
    }

    public static int requestStreamIdEncodingOffset()
    {
        return 1;
    }

    public static int requestStreamIdEncodingLength()
    {
        return 4;
    }

    public static String requestStreamIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int requestStreamIdNullValue()
    {
        return -2147483648;
    }

    public static int requestStreamIdMinValue()
    {
        return -2147483647;
    }

    public static int requestStreamIdMaxValue()
    {
        return 2147483647;
    }

    public RecordMetadataEncoderBefore83 requestStreamId(final int value)
    {
        buffer.putInt(offset + 1, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int requestIdId()
    {
        return 3;
    }

    public static int requestIdSinceVersion()
    {
        return 0;
    }

    public static int requestIdEncodingOffset()
    {
        return 5;
    }

    public static int requestIdEncodingLength()
    {
        return 8;
    }

    public static String requestIdMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static long requestIdNullValue()
    {
        return 0xffffffffffffffffL;
    }

    public static long requestIdMinValue()
    {
        return 0x0L;
    }

    public static long requestIdMaxValue()
    {
        return 0xfffffffffffffffeL;
    }

    public RecordMetadataEncoderBefore83 requestId(final long value)
    {
        buffer.putLong(offset + 5, value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int protocolVersionId()
    {
        return 4;
    }

    public static int protocolVersionSinceVersion()
    {
        return 0;
    }

    public static int protocolVersionEncodingOffset()
    {
        return 13;
    }

    public static int protocolVersionEncodingLength()
    {
        return 2;
    }

    public static String protocolVersionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int protocolVersionNullValue()
    {
        return 65535;
    }

    public static int protocolVersionMinValue()
    {
        return 0;
    }

    public static int protocolVersionMaxValue()
    {
        return 65534;
    }

    public RecordMetadataEncoderBefore83 protocolVersion(final int value)
    {
        buffer.putShort(offset + 13, (short)value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int valueTypeId()
    {
        return 5;
    }

    public static int valueTypeSinceVersion()
    {
        return 0;
    }

    public static int valueTypeEncodingOffset()
    {
        return 15;
    }

    public static int valueTypeEncodingLength()
    {
        return 1;
    }

    public static String valueTypeMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public RecordMetadataEncoderBefore83 valueType(final ValueType value)
    {
        buffer.putByte(offset + 15, (byte)value.value());
        return this;
    }

    public static int intentId()
    {
        return 6;
    }

    public static int intentSinceVersion()
    {
        return 0;
    }

    public static int intentEncodingOffset()
    {
        return 16;
    }

    public static int intentEncodingLength()
    {
        return 1;
    }

    public static String intentMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static short intentNullValue()
    {
        return (short)255;
    }

    public static short intentMinValue()
    {
        return (short)0;
    }

    public static short intentMaxValue()
    {
        return (short)254;
    }

    public RecordMetadataEncoderBefore83 intent(final short value)
    {
        buffer.putByte(offset + 16, (byte)value);
        return this;
    }


    public static int rejectionTypeId()
    {
        return 7;
    }

    public static int rejectionTypeSinceVersion()
    {
        return 0;
    }

    public static int rejectionTypeEncodingOffset()
    {
        return 17;
    }

    public static int rejectionTypeEncodingLength()
    {
        return 1;
    }

    public static String rejectionTypeMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public RecordMetadataEncoderBefore83 rejectionType(final RejectionType value)
    {
        buffer.putByte(offset + 17, (byte)value.value());
        return this;
    }

    public static int brokerVersionId()
    {
        return 9;
    }

    public static int brokerVersionSinceVersion()
    {
        return 2;
    }

    public static int brokerVersionEncodingOffset()
    {
        return 18;
    }

    public static int brokerVersionEncodingLength()
    {
        return 12;
    }

    public static String brokerVersionMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "optional";
        }

        return "";
    }

    private final VersionEncoder brokerVersion = new VersionEncoder();

    public VersionEncoder brokerVersion()
    {
        brokerVersion.wrap(buffer, offset + 18);
        return brokerVersion;
    }

    public static int rejectionReasonId()
    {
        return 8;
    }

    public static String rejectionReasonCharacterEncoding()
    {
        return java.nio.charset.StandardCharsets.UTF_8.name();
    }

    public static String rejectionReasonMetaAttribute(final MetaAttribute metaAttribute)
    {
        if (MetaAttribute.PRESENCE == metaAttribute)
        {
            return "required";
        }

        return "";
    }

    public static int rejectionReasonHeaderLength()
    {
        return 4;
    }

    public RecordMetadataEncoderBefore83 putRejectionReason(final DirectBuffer src, final int srcOffset, final int length)
    {
        if (length > 2147483647)
        {
            throw new IllegalStateException("length > maxValue for type: " + length);
        }

        final int headerLength = 4;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public RecordMetadataEncoderBefore83 putRejectionReason(final byte[] src, final int srcOffset, final int length)
    {
        if (length > 2147483647)
        {
            throw new IllegalStateException("length > maxValue for type: " + length);
        }

        final int headerLength = 4;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public RecordMetadataEncoderBefore83 rejectionReason(final String value)
    {
        final byte[] bytes = (null == value || value.isEmpty()) ? org.agrona.collections.ArrayUtil.EMPTY_BYTE_ARRAY : value.getBytes(java.nio.charset.StandardCharsets.UTF_8);

        final int length = bytes.length;
        if (length > 2147483647)
        {
            throw new IllegalStateException("length > maxValue for type: " + length);
        }

        final int headerLength = 4;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putInt(limit, length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, bytes, 0, length);

        return this;
    }

    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        return appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        if (null == buffer)
        {
            return builder;
        }

        final RecordMetadataDecoderBefore83 decoder = new RecordMetadataDecoderBefore83();
        decoder.wrap(buffer, initialOffset, BLOCK_LENGTH, SCHEMA_VERSION);

        return decoder.appendTo(builder);
    }
}
