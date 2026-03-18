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

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.db.impl.*;
import io.camunda.zeebe.protocol.ZbColumnFamilies;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.junit.jupiter.api.Test;

final class KeyFormattersTest {
  @Test
  void shouldFallbackToHex() {
    // given
    final var key = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    // when -- using a column family that is not registered with a specific formatter
    final var formatter =
        KeyFormatters.ofDefault().forColumnFamily(ZbColumnFamilies.DEPRECATED_PROCESS_CACHE);

    // then
    assertThat(formatter.formatKey(key)).isEqualTo("01 02 03 04 05 06 07 08 09 0a");
  }

  @Test
  void shouldDecodeWithHex() {
    // given
    final var key = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

    // when
    final var formatter = KeyFormatters.ofHex().forColumnFamily(ZbColumnFamilies.DEFAULT);

    // then
    assertThat(formatter.formatKey(key)).isEqualTo("01 02 03 04 05 06 07 08 09 0a");
  }

  @Test
  void shouldDecodeWithFormat() {
    // given -- key consisting of ColumnFamily, DbLong, DbInt, DbString, DbByte and DbBytes
    final var cf = new DbLong();
    final var dbLong = new DbLong();
    final var dbInt = new DbInt();
    final var dbString = new DbString();
    final var dbByte = new DbByte();
    final var dbBytes = new DbBytes();
    cf.wrapLong(1);
    dbLong.wrapLong(5);
    dbInt.wrapInt(987);
    dbString.wrapString("hello");
    dbByte.wrapByte((byte) 123);
    dbBytes.wrapBytes(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10});

    final var keyBuffer = new ExpandableArrayBuffer();
    final var offset = new MutableInteger(0);
    cf.write(keyBuffer, offset.getAndAdd(cf.getLength()));
    dbLong.write(keyBuffer, offset.getAndAdd(dbLong.getLength()));
    dbInt.write(keyBuffer, offset.getAndAdd(dbInt.getLength()));
    dbString.write(keyBuffer, offset.getAndAdd(dbString.getLength()));
    dbByte.write(keyBuffer, offset.getAndAdd(dbByte.getLength()));
    dbBytes.write(keyBuffer, offset.getAndAdd(dbBytes.getLength()));
    final var key = new byte[offset.get()];
    keyBuffer.getBytes(0, key, 0, key.length);

    // when
    final var fullFormatter =
        KeyFormatters.ofFormat("lisbB").forColumnFamily(ZbColumnFamilies.DEFAULT);
    final var partialFormatter =
        KeyFormatters.ofFormat("lis").forColumnFamily(ZbColumnFamilies.DEFAULT);

    // then
    assertThat(fullFormatter.formatKey(key))
        .isEqualTo("5:987:hello:123:01 02 03 04 05 06 07 08 09 0a");
    assertThat(partialFormatter.formatKey(key)).isEqualTo("5:987:hello");
  }

  @Test
  void shouldUseRegisteredFormatter() {
    // given
    final var cf = new DbLong();
    final var dbString = new DbString();
    cf.wrapLong(ZbColumnFamilies.DEFAULT.getValue());
    dbString.wrapString("hello");

    final var keyBuffer = new ExpandableArrayBuffer();
    final var offset = new MutableInteger(0);
    cf.write(keyBuffer, offset.getAndAdd(cf.getLength()));
    dbString.write(keyBuffer, offset.getAndAdd(dbString.getLength()));

    final var key = new byte[offset.get()];
    keyBuffer.getBytes(0, key, 0, key.length);

    // when
    final var formatter = KeyFormatters.ofDefault().forColumnFamily(ZbColumnFamilies.DEFAULT);

    // then
    assertThat(formatter.formatKey(key)).isEqualTo("hello");
  }
}
