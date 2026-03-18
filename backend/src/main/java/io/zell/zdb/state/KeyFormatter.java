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
package io.zell.zdb.state;

import io.camunda.zeebe.db.DbValue;
import io.camunda.zeebe.db.impl.*;
import java.util.HexFormat;
import org.agrona.concurrent.UnsafeBuffer;

public interface KeyFormatter {
  String formatKey(final byte[] key);

  final class HexFormatter implements KeyFormatter {
    @Override
    public String formatKey(final byte[] key) {
      return HexFormat.ofDelimiter(" ").formatHex(key);
    }
  }

  final class DbValueFormatter implements KeyFormatter {
    final DbValue[] values;

    private DbValueFormatter(final DbValue[] values) {
      this.values = values;
    }

    /**
     * Takes a format string consisting of a sequence of 's', 'l', 'i', 'b', and 'B' characters
     * which specify the format of the keys. 's' is a string, 'l' is a long, 'i' is an int, 'b' is a
     * byte, and 'B' is a byte array.
     */
    public static DbValueFormatter of(final String keyFormat) {
      final var components = new DbValue[keyFormat.length()];
      final var chars = keyFormat.toCharArray();
      for (int i = 0; i < chars.length; i++) {
        components[i] =
            switch (chars[i]) {
              case 's' -> new DbString();
              case 'l' -> new DbLong();
              case 'i' -> new DbInt();
              case 'b' -> new DbByte();
              case 'B' -> new DbBytes();
              default -> throw new IllegalArgumentException(
                  "Unknown key format component: " + chars[i]);
            };
      }
      return new DbValueFormatter(components);
    }

    @Override
    public String formatKey(final byte[] key) {
      final var formatted = new StringBuilder();
      final var keyBuffer = new UnsafeBuffer(key);
      int offset = 8;
      try {
        for (final var dbValue : this.values) {
          dbValue.wrap(keyBuffer, offset, key.length - offset);
          offset += dbValue.getLength();
          if (!formatted.isEmpty()) {
            formatted.append(":");
          }
          switch (dbValue) {
            case final DbString dbString -> formatted.append(dbString);
            case final DbLong dbLong -> formatted.append(dbLong.getValue());
            case final DbInt dbInt -> formatted.append(dbInt.getValue());
            case final DbByte dbByte -> formatted.append(dbByte.getValue());
            case final DbBytes dbBytes -> {
              final var buf = dbBytes.getDirectBuffer();
              final var bytes = new byte[dbBytes.getLength()];
              buf.getBytes(0, bytes);
              formatted.append(KeyFormatters.HEX_FORMATTER.formatKey(bytes));
            }
            default -> formatted.append(dbValue);
          }
        }
      } catch (final IndexOutOfBoundsException indexOutOfBoundsException) {
        return KeyFormatters.HEX_FORMATTER.formatKey(key);
      }
      return formatted.toString();
    }
  }
}
