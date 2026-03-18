/*
 * Copyright © 2020 camunda services GmbH (info@camunda.com)
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
package io.zell.zdb.journal.file;

import java.nio.ByteBuffer;

final class FrameUtil {

  private static final byte IGNORE = 0;
  private static final int LENGTH = 1;

  private FrameUtil() {}

  /**
   * Reads the version at buffer's current position. The position of the buffer will be advanced.
   */
  static int readVersion(final ByteBuffer buffer) {
    return buffer.get();
  }

  /**
   * Returns true if there is a valid version at buffer's current position. The position of the
   * buffer will be unchanged.
   */
  static boolean hasValidVersion(final ByteBuffer buffer) {
    if (buffer.capacity() < buffer.position() + LENGTH) {
      return false;
    }
    return buffer.get(buffer.position()) != IGNORE;
  }
}
