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
package io.zell.zdb.mcp.util;

import java.util.List;

/** Helper for building MCP list-tool JSON envelopes from already-serialized JSON items. */
public final class PaginatedResult {

  private PaginatedResult() {}

  /**
   * Build a paginated JSON envelope from a list of items already encoded as JSON strings.
   *
   * <p>Output shape: {@code {"total_fetched":N,"offset":O,"limit":L,"has_more":B,"data":[...]}}.
   *
   * @param allItems all items collected from the backend (already JSON-encoded)
   * @param offset the offset to start the page at
   * @param limit the maximum number of items to include in the page
   * @return a JSON object representing the page
   */
  public static String buildPageJson(
      final List<String> allItems, final int offset, final int limit) {
    final int size = allItems.size();
    final int safeOffset = Math.max(0, offset);
    final int safeLimit = Math.max(0, limit);
    final int from = Math.min(safeOffset, size);
    final int to = Math.min(safeOffset + safeLimit, size);
    final List<String> page = allItems.subList(from, to);

    final StringBuilder sb = new StringBuilder();
    sb.append("{\"total_fetched\":").append(size);
    sb.append(",\"offset\":").append(safeOffset);
    sb.append(",\"limit\":").append(safeLimit);
    sb.append(",\"has_more\":").append(to < size);
    sb.append(",\"data\":[");
    for (int i = 0; i < page.size(); i++) {
      if (i > 0) {
        sb.append(",");
      }
      sb.append(page.get(i));
    }
    sb.append("]}");
    return sb.toString();
  }
}
