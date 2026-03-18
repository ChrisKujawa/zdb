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
public final class RecordMetadataDecoderBefore83 implements MessageDecoderFlyweight
{
    public static final int BLOCK_LENGTH = 30;
    public static final int TEMPLATE_ID = 200;
    public static final int SCHEMA_ID = 0;
    public static final int SCHEMA_VERSION = 3;
    public static final String SEMANTIC_VERSION = "8.2.15";
    public static final java.nio.ByteOrder BYTE_ORDER = java.nio.ByteOrder.LITTLE_ENDIAN;

    private final RecordMetadataDecoderBefore83 parentMessage = this;
    private DirectBuffer buffer;
    private int initialOffset;
    private int offset;
    private int limit;
    int actingBlockLength;
    int actingVersion;

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

    public DirectBuffer buffer()
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

    public RecordMetadataDecoderBefore83 wrap(
        final DirectBuffer buffer,
        final int offset,
        final int actingBlockLength,
        final int actingVersion)
    {
        if (buffer != this.buffer)
        {
            this.buffer = buffer;
        }
        this.initialOffset = offset;
        this.offset = offset;
        this.actingBlockLength = actingBlockLength;
        this.actingVersion = actingVersion;
        limit(offset + actingBlockLength);

        return this;
    }

    public RecordMetadataDecoderBefore83 wrapAndApplyHeader(
        final DirectBuffer buffer,
        final int offset,
        final MessageHeaderDecoder headerDecoder)
    {
        headerDecoder.wrap(buffer, offset);

        final int templateId = headerDecoder.templateId();
        if (TEMPLATE_ID != templateId)
        {
            throw new IllegalStateException("Invalid TEMPLATE_ID: " + templateId);
        }

        return wrap(
            buffer,
            offset + MessageHeaderDecoder.ENCODED_LENGTH,
            headerDecoder.blockLength(),
            headerDecoder.version());
    }

    public RecordMetadataDecoderBefore83 sbeRewind()
    {
        return wrap(buffer, initialOffset, actingBlockLength, actingVersion);
    }

    public int sbeDecodedLength()
    {
        final int currentLimit = limit();
        sbeSkip();
        final int decodedLength = encodedLength();
        limit(currentLimit);

        return decodedLength;
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

    public short recordTypeRaw()
    {
        return ((short)(buffer.getByte(offset + 0) & 0xFF));
    }

    public RecordType recordType()
    {
        return RecordType.get(((short)(buffer.getByte(offset + 0) & 0xFF)));
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

    public int requestStreamId()
    {
        return buffer.getInt(offset + 1, java.nio.ByteOrder.LITTLE_ENDIAN);
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

    public long requestId()
    {
        return buffer.getLong(offset + 5, java.nio.ByteOrder.LITTLE_ENDIAN);
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

    public int protocolVersion()
    {
        return (buffer.getShort(offset + 13, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF);
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

    public short valueTypeRaw()
    {
        return ((short)(buffer.getByte(offset + 15) & 0xFF));
    }

    public ValueType valueType()
    {
        return ValueType.get(((short)(buffer.getByte(offset + 15) & 0xFF)));
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

    public short intent()
    {
        return ((short)(buffer.getByte(offset + 16) & 0xFF));
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

    public short rejectionTypeRaw()
    {
        return ((short)(buffer.getByte(offset + 17) & 0xFF));
    }

    public RejectionType rejectionType()
    {
        return RejectionType.get(((short)(buffer.getByte(offset + 17) & 0xFF)));
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

    private final VersionDecoder brokerVersion = new VersionDecoder();

    public VersionDecoder brokerVersion()
    {
        if (parentMessage.actingVersion < 2)
        {
            return null;
        }

        brokerVersion.wrap(buffer, offset + 18);
        return brokerVersion;
    }

    public static int rejectionReasonId()
    {
        return 8;
    }

    public static int rejectionReasonSinceVersion()
    {
        return 0;
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

    public int rejectionReasonLength()
    {
        final int limit = parentMessage.limit();
        return (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
    }

    public int skipRejectionReason()
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
        final int dataOffset = limit + headerLength;
        parentMessage.limit(dataOffset + dataLength);

        return dataLength;
    }

    public int getRejectionReason(final MutableDirectBuffer dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int getRejectionReason(final byte[] dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public void wrapRejectionReason(final DirectBuffer wrapBuffer)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
        parentMessage.limit(limit + headerLength + dataLength);
        wrapBuffer.wrap(buffer, limit + headerLength, dataLength);
    }

    public String rejectionReason()
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (int)(buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFFL);
        parentMessage.limit(limit + headerLength + dataLength);

        if (0 == dataLength)
        {
            return "";
        }

        final byte[] tmp = new byte[dataLength];
        buffer.getBytes(limit + headerLength, tmp, 0, dataLength);

        return new String(tmp, java.nio.charset.StandardCharsets.UTF_8);
    }

    public String toString()
    {
        if (null == buffer)
        {
            return "";
        }

        final RecordMetadataDecoderBefore83 decoder = new RecordMetadataDecoderBefore83();
        decoder.wrap(buffer, initialOffset, actingBlockLength, actingVersion);

        return decoder.appendTo(new StringBuilder()).toString();
    }

    public StringBuilder appendTo(final StringBuilder builder)
    {
        if (null == buffer)
        {
            return builder;
        }

        final int originalLimit = limit();
        limit(initialOffset + actingBlockLength);
        builder.append("[RecordMetadata](sbeTemplateId=");
        builder.append(TEMPLATE_ID);
        builder.append("|sbeSchemaId=");
        builder.append(SCHEMA_ID);
        builder.append("|sbeSchemaVersion=");
        if (parentMessage.actingVersion != SCHEMA_VERSION)
        {
            builder.append(parentMessage.actingVersion);
            builder.append('/');
        }
        builder.append(SCHEMA_VERSION);
        builder.append("|sbeBlockLength=");
        if (actingBlockLength != BLOCK_LENGTH)
        {
            builder.append(actingBlockLength);
            builder.append('/');
        }
        builder.append(BLOCK_LENGTH);
        builder.append("):");
        builder.append("recordType=");
        builder.append(this.recordType());
        builder.append('|');
        builder.append("requestStreamId=");
        builder.append(this.requestStreamId());
        builder.append('|');
        builder.append("requestId=");
        builder.append(this.requestId());
        builder.append('|');
        builder.append("protocolVersion=");
        builder.append(this.protocolVersion());
        builder.append('|');
        builder.append("valueType=");
        builder.append(this.valueType());
        builder.append('|');
        builder.append("intent=");
        builder.append(this.intent());
        builder.append('|');
        builder.append("rejectionType=");
        builder.append(this.rejectionType());
        builder.append('|');
        builder.append("brokerVersion=");
        final VersionDecoder brokerVersion = this.brokerVersion();
        if (brokerVersion != null)
        {
            brokerVersion.appendTo(builder);
        }
        else
        {
            builder.append("null");
        }
        builder.append('|');
        builder.append("rejectionReason=");
        builder.append('\'').append(rejectionReason()).append('\'');

        limit(originalLimit);

        return builder;
    }
    
    public RecordMetadataDecoderBefore83 sbeSkip()
    {
        sbeRewind();
        skipRejectionReason();

        return this;
    }
}
