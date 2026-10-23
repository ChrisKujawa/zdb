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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import java.util.Map;

/** Common utilities used by all MCP tool implementations. */
public final class ToolUtils {

  /** Shared Jackson ObjectMapper. Jackson 2 is on the classpath transitively. */
  public static final ObjectMapper MAPPER = new ObjectMapper();

  private ToolUtils() {}

  /** Build a successful tool result with the given JSON text payload. */
  public static CallToolResult ok(final String json) {
    return CallToolResult.builder().addTextContent(json).isError(false).build();
  }

  /** Build an error tool result wrapping the given exception's message. */
  public static CallToolResult error(final Throwable t) {
    final String message = t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
    return CallToolResult.builder().addTextContent("Error: " + message).isError(true).build();
  }

  /** Build an error tool result with a custom message. */
  public static CallToolResult error(final String message) {
    return CallToolResult.builder().addTextContent("Error: " + message).isError(true).build();
  }

  /** Read a required String argument; throws if missing or wrong type. */
  public static String requiredString(final Map<String, Object> args, final String key) {
    final Object value = args == null ? null : args.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing required argument: " + key);
    }
    if (!(value instanceof String s)) {
      throw new IllegalArgumentException("Argument '" + key + "' must be a string");
    }
    if (s.isEmpty()) {
      throw new IllegalArgumentException("Argument '" + key + "' must not be empty");
    }
    return s;
  }

  /** Read an optional String argument. Returns null when absent or empty. */
  public static String optionalString(final Map<String, Object> args, final String key) {
    final Object value = args == null ? null : args.get(key);
    if (value == null) {
      return null;
    }
    if (!(value instanceof String s)) {
      throw new IllegalArgumentException("Argument '" + key + "' must be a string");
    }
    return s.isEmpty() ? null : s;
  }

  /** Read a required integer argument as long; throws if missing. */
  public static long requiredLong(final Map<String, Object> args, final String key) {
    final Object value = args == null ? null : args.get(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing required argument: " + key);
    }
    if (!(value instanceof Number n)) {
      throw new IllegalArgumentException("Argument '" + key + "' must be a number");
    }
    return n.longValue();
  }

  /** Read an optional Long argument; returns null when absent. */
  public static Long optionalLong(final Map<String, Object> args, final String key) {
    final Object value = args == null ? null : args.get(key);
    if (value == null) {
      return null;
    }
    if (!(value instanceof Number n)) {
      throw new IllegalArgumentException("Argument '" + key + "' must be a number");
    }
    return n.longValue();
  }

  /** Read an optional integer argument with a default value. */
  public static int intOrDefault(
      final Map<String, Object> args, final String key, final int defaultValue) {
    final Object value = args == null ? null : args.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (!(value instanceof Number n)) {
      throw new IllegalArgumentException("Argument '" + key + "' must be a number");
    }
    return n.intValue();
  }
}
