package io.zell.zdb;

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

import io.zeebe.containers.ZeebeContainer;
import io.zeebe.containers.ZeebePort;
import java.io.*;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

public final class TestUtils {

  public static final String CONTAINER_PATH = "/usr/local/zeebe/data/";
  public static final String CONTAINER_PATH_88 = "/usr/local/camunda/data/";
  public static final String TIMESTAMP_REGEX = "\"timestamp\":[0-9]+,";
  private static final String TMP_FOLDER_FORMAT = "data-%s-%d";

  private TestUtils() {}

  public static File newTmpFolder(final Class<?> clazz) {
    return new File(
        "/tmp/",
        String.format(TMP_FOLDER_FORMAT, clazz.getName(), ThreadLocalRandom.current().nextLong()));
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

  /**
   * Creates a ZeebeContainer with the given docker image name. The container will mount the given
   * tempDir to /usr/local/zeebe/data in order to access the RocksDB data.
   *
   * <p>The container will be started with the current user (see {@link #getRunAsUser()}) in order
   * to access the data and delete it later.
   *
   * <p>This method is for Zeebe versions before 8.5 (exclusive), as with newer Zeebe testcontainers
   * version the exposed ports, has changed. In earlier versions of Zeebe this port, wasn't exposed
   * and will fail.
   *
   * @param dockerImageName the docker image name of the Zeebe version to use
   * @param tempDir the temporary directory to mount to /usr/local/zeebe/data
   * @param logger the logger to use for the container logs
   * @return the ZeebeContainer
   */
  public static ZeebeContainer createZeebeContainerBefore85(
      final DockerImageName dockerImageName, final String tempDir, final Logger logger) {
    final ZeebeContainer container =
        new ZeebeContainer(dockerImageName)
            /* run the container with the current user, in order to access the data and delete it later */
            .withCreateContainerCmdModifier(cmd -> cmd.withUser(TestUtils.getRunAsUser()))
            // with 8.2 we disabled WAL per default
            // we have to enabled it inorder to access the data from RocksDB
            .withEnv("ZEEBE_BROKER_EXPERIMENTAL_ROCKSDB_DISABLEWAL", "false")
            .withLogConsumer(new Slf4jLogConsumer(logger))
            .withFileSystemBind(tempDir, CONTAINER_PATH, BindMode.READ_WRITE);

    container.setExposedPorts(
        List.of(
            ZeebePort.GATEWAY_GRPC.getPort(),
            ZeebePort.COMMAND.getPort(),
            ZeebePort.INTERNAL.getPort(),
            ZeebePort.MONITORING.getPort()));

    return container;
  }

  /**
   * Creates a ZeebeContainer with the given docker image name. The container will mount the given
   * tempDir to /usr/local/camunda/data in order to access the RocksDB data.
   *
   * <p>The container will be started with the current user (see {@link #getRunAsUser()}) in order
   * to access the data and delete it later.
   *
   * <p>The container will run with the profiles "broker" and "standalone" and with disabled
   * secondary storage, to make sure only Zeebe is running.
   *
   * <p>This method is for Camunda versions 8.8 and after, as with newer Camunda versions the data
   * path has changed and everything is running in one container.
   *
   * @param dockerImageName the docker image name of the Camunda version to use
   * @param tempDir the temporary directory to mount to /usr/local/camunda/data
   * @param logger the logger to use for the container logs
   * @return the ZeebeContainer
   */
  public static ZeebeContainer createZeebeContainerGreaterOrEquals88(
      final DockerImageName dockerImageName, final String tempDir, final Logger logger) {
    final ZeebeContainer container =
        new ZeebeContainer(dockerImageName)
            /* run the container with the current user, in order to access the data and delete it later */
            .withCreateContainerCmdModifier(cmd -> cmd.withUser(TestUtils.getRunAsUser()))
            // with 8.2 we disabled WAL per default
            // we have to enabled it inorder to access the data from RocksDB
            .withEnv("ZEEBE_BROKER_EXPERIMENTAL_ROCKSDB_DISABLEWAL", "false")
            // with 8.8 we have the OC with all component together
            // to run Zeebe only we need to disable the secondary storage
            // and set the active profiles to broker only
            .withEnv("CAMUNDA_DATA_SECONDARYSTORAGE_TYPE", "none")
            .withEnv("SPRING_PROFILES_ACTIVE", "broker,standalone")
            .withLogConsumer(new Slf4jLogConsumer(logger))
            .withFileSystemBind(tempDir, TestUtils.CONTAINER_PATH_88, BindMode.READ_WRITE);

    return container;
  }
}
