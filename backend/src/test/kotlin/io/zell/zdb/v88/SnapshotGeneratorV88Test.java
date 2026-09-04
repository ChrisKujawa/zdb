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
package io.zell.zdb.v88;

import io.zell.zdb.SnapshotGeneratorSupport;
import io.zell.zdb.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Tag("snapshot-generator")
class SnapshotGeneratorV88Test {

  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotGeneratorV88Test.class);
  private static SnapshotGeneratorSupport.GeneratorContext context;

  @BeforeAll
  static void startContainerAndCreateContent() {
    context =
        SnapshotGeneratorSupport.startContainerAndCreateContent(
            SnapshotGeneratorV88Test.class,
            "camunda/camunda:8.8.0",
            TestUtils::createZeebeContainerGreaterOrEquals88,
            LOGGER);
  }

  @AfterAll
  static void stopContainerAndCleanup() throws Exception {
    SnapshotGeneratorSupport.stopContainerAndCleanup(context);
  }

  @Test
  void generateSnapshot() throws Exception {
    // given/when/then
    SnapshotGeneratorSupport.generateSnapshot(context, "v8.8");
  }
}
