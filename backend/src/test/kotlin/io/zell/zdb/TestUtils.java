package io.zell.zdb;/*
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
import java.io.*;
import java.util.concurrent.ThreadLocalRandom;

public final class TestUtils {

  public static final String CONTAINER_PATH = "/usr/local/zeebe/data/";
  public static final String CONTAINER_PATH_88 = "/usr/local/camunda/data/";
  public static final String TIMESTAMP_REGEX = "\"timestamp\":[0-9]+,";
  private static final String TMP_FOLDER_FORMAT = "data-%s-%d";
  private TestUtils() {}

  public static File newTmpFolder(Class<?> clazz) {
    return new File("/tmp/", String.format(TMP_FOLDER_FORMAT, clazz.getName(), ThreadLocalRandom.current().nextLong()));
  }

  /**
   * Utility to get the current UID and GID such that a container can be run as that user.
   *
   * <p>NOTE: only works on Unix systems
   *
   * <p>This is especially useful if you need to mount a host file path with the right permissions.
   *
   * @return the current uid and gid as a string
   */
  public static String getRunAsUser() {
    return getUid() + ":" + getGid();
  }

  /**
   * NOTE: only works on Unix systems
   *
   * @return the current Unix group ID
   */
  static String getGid() {
    return execCommand("id -g");
  }

  /**
   * NOTE: only works on Unix systems
   *
   * @return the current Unix user ID
   */
  static String getUid() {
    return execCommand("id -u");
  }

  private static String execCommand(final String command) {
    try {
      final Process exec = Runtime.getRuntime().exec(command);
      final BufferedReader input = new BufferedReader(new InputStreamReader(exec.getInputStream()));
      return input.readLine();
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
