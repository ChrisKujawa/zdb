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

import io.camunda.zeebe.protocol.ZbColumnFamilies;
import java.util.EnumMap;
import java.util.Map;

public abstract class KeyFormatters {
  public static final KeyFormatter HEX_FORMATTER = new KeyFormatter.HexFormatter();
  public static final Map<ZbColumnFamilies, KeyFormatter> FORMATTERS =
      new EnumMap<>(ZbColumnFamilies.class);

  static {
    FORMATTERS.put(ZbColumnFamilies.DEFAULT, KeyFormatter.DbValueFormatter.of("s"));
    FORMATTERS.put(ZbColumnFamilies.KEY, KeyFormatter.DbValueFormatter.of("s"));
    FORMATTERS.put(
        ZbColumnFamilies.ELEMENT_INSTANCE_PARENT_CHILD, KeyFormatter.DbValueFormatter.of("ll"));
    FORMATTERS.put(ZbColumnFamilies.ELEMENT_INSTANCE_KEY, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(
        ZbColumnFamilies.NUMBER_OF_TAKEN_SEQUENCE_FLOWS, KeyFormatter.DbValueFormatter.of("lss"));
    FORMATTERS.put(
        ZbColumnFamilies.ELEMENT_INSTANCE_CHILD_PARENT, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.VARIABLES, KeyFormatter.DbValueFormatter.of("ls"));
    FORMATTERS.put(ZbColumnFamilies.TIMERS, KeyFormatter.DbValueFormatter.of("ll"));
    FORMATTERS.put(ZbColumnFamilies.TIMER_DUE_DATES, KeyFormatter.DbValueFormatter.of("lll"));
    FORMATTERS.put(ZbColumnFamilies.PENDING_DEPLOYMENT, KeyFormatter.DbValueFormatter.of("li"));
    FORMATTERS.put(ZbColumnFamilies.DEPLOYMENT_RAW, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.JOBS, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.JOB_STATES, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.JOB_DEADLINES, KeyFormatter.DbValueFormatter.of("ll"));
    FORMATTERS.put(ZbColumnFamilies.MESSAGE_KEY, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.MESSAGE_DEADLINES, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.MESSAGE_IDS, KeyFormatter.DbValueFormatter.of("sss"));
    FORMATTERS.put(ZbColumnFamilies.MESSAGE_CORRELATED, KeyFormatter.DbValueFormatter.of("ls"));
    FORMATTERS.put(
        ZbColumnFamilies.MESSAGE_PROCESSES_ACTIVE_BY_CORRELATION_KEY,
        KeyFormatter.DbValueFormatter.of("ss"));
    FORMATTERS.put(
        ZbColumnFamilies.MESSAGE_PROCESS_INSTANCE_CORRELATION_KEYS,
        KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(
        ZbColumnFamilies.MESSAGE_SUBSCRIPTION_BY_KEY, KeyFormatter.DbValueFormatter.of("ls"));
    FORMATTERS.put(ZbColumnFamilies.INCIDENTS, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(
        ZbColumnFamilies.INCIDENT_PROCESS_INSTANCES, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.INCIDENT_JOBS, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.EVENT_SCOPE, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.EVENT_TRIGGER, KeyFormatter.DbValueFormatter.of("ll"));
    FORMATTERS.put(ZbColumnFamilies.BANNED_INSTANCE, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.EXPORTER, KeyFormatter.DbValueFormatter.of("s"));
    FORMATTERS.put(ZbColumnFamilies.AWAIT_WORKLOW_RESULT, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.JOB_BACKOFF, KeyFormatter.DbValueFormatter.of("ll"));
    FORMATTERS.put(ZbColumnFamilies.PENDING_DISTRIBUTION, KeyFormatter.DbValueFormatter.of("li"));
    FORMATTERS.put(
        ZbColumnFamilies.COMMAND_DISTRIBUTION_RECORD, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.MESSAGE_STATS, KeyFormatter.DbValueFormatter.of("ls"));
    FORMATTERS.put(
        ZbColumnFamilies.PROCESS_INSTANCE_KEY_BY_DEFINITION_KEY,
        KeyFormatter.DbValueFormatter.of("ll"));
    FORMATTERS.put(ZbColumnFamilies.MIGRATIONS_STATE, KeyFormatter.DbValueFormatter.of("s"));
    FORMATTERS.put(ZbColumnFamilies.PROCESS_VERSION, KeyFormatter.DbValueFormatter.of("ss"));
    FORMATTERS.put(ZbColumnFamilies.PROCESS_CACHE, KeyFormatter.DbValueFormatter.of("sl"));
    FORMATTERS.put(
        ZbColumnFamilies.PROCESS_CACHE_BY_ID_AND_VERSION, KeyFormatter.DbValueFormatter.of("ssl"));
    FORMATTERS.put(
        ZbColumnFamilies.PROCESS_CACHE_DIGEST_BY_ID, KeyFormatter.DbValueFormatter.of("ss"));
    FORMATTERS.put(ZbColumnFamilies.DMN_DECISIONS, KeyFormatter.DbValueFormatter.of("sl"));
    FORMATTERS.put(
        ZbColumnFamilies.DMN_DECISION_REQUIREMENTS, KeyFormatter.DbValueFormatter.of("sl"));
    FORMATTERS.put(
        ZbColumnFamilies.DMN_LATEST_DECISION_BY_ID, KeyFormatter.DbValueFormatter.of("ss"));
    FORMATTERS.put(
        ZbColumnFamilies.DMN_LATEST_DECISION_REQUIREMENTS_BY_ID,
        KeyFormatter.DbValueFormatter.of("ss"));
    FORMATTERS.put(
        ZbColumnFamilies.DMN_DECISION_KEY_BY_DECISION_REQUIREMENTS_KEY,
        KeyFormatter.DbValueFormatter.of("slsl"));
    FORMATTERS.put(
        ZbColumnFamilies.DMN_DECISION_KEY_BY_DECISION_ID_AND_VERSION,
        KeyFormatter.DbValueFormatter.of("ssi"));
    FORMATTERS.put(
        ZbColumnFamilies.DMN_DECISION_REQUIREMENTS_KEY_BY_DECISION_REQUIREMENT_ID_AND_VERSION,
        KeyFormatter.DbValueFormatter.of("ssi"));
    FORMATTERS.put(ZbColumnFamilies.FORMS, KeyFormatter.DbValueFormatter.of("sl"));
    FORMATTERS.put(ZbColumnFamilies.FORM_VERSION, KeyFormatter.DbValueFormatter.of("ss"));
    FORMATTERS.put(
        ZbColumnFamilies.FORM_BY_ID_AND_VERSION, KeyFormatter.DbValueFormatter.of("ssl"));
    FORMATTERS.put(ZbColumnFamilies.MESSAGES, KeyFormatter.DbValueFormatter.of("sssl"));
    FORMATTERS.put(
        ZbColumnFamilies.MESSAGE_START_EVENT_SUBSCRIPTION_BY_NAME_AND_KEY,
        KeyFormatter.DbValueFormatter.of("ssl"));
    FORMATTERS.put(
        ZbColumnFamilies.MESSAGE_START_EVENT_SUBSCRIPTION_BY_KEY_AND_NAME,
        KeyFormatter.DbValueFormatter.of("lss"));
    FORMATTERS.put(
        ZbColumnFamilies.MESSAGE_SUBSCRIPTION_BY_NAME_AND_CORRELATION_KEY,
        KeyFormatter.DbValueFormatter.of("sssl"));
    FORMATTERS.put(
        ZbColumnFamilies.PROCESS_SUBSCRIPTION_BY_KEY, KeyFormatter.DbValueFormatter.of("lss"));
    FORMATTERS.put(ZbColumnFamilies.JOB_ACTIVATABLE, KeyFormatter.DbValueFormatter.of("ssl"));
    FORMATTERS.put(
        ZbColumnFamilies.SIGNAL_SUBSCRIPTION_BY_NAME_AND_KEY,
        KeyFormatter.DbValueFormatter.of("ssl"));
    FORMATTERS.put(
        ZbColumnFamilies.SIGNAL_SUBSCRIPTION_BY_KEY_AND_NAME,
        KeyFormatter.DbValueFormatter.of("lss"));
    FORMATTERS.put(ZbColumnFamilies.USER_TASKS, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(ZbColumnFamilies.USER_TASK_STATES, KeyFormatter.DbValueFormatter.of("l"));
    FORMATTERS.put(
        ZbColumnFamilies.COMPENSATION_SUBSCRIPTION, KeyFormatter.DbValueFormatter.of("sll"));
      FORMATTERS.put(
              ZbColumnFamilies.ENTITIES_BY_RELATION, KeyFormatter.DbValueFormatter.of("bsbs"));
      FORMATTERS.put(
              ZbColumnFamilies.RELATIONS_BY_ENTITY, KeyFormatter.DbValueFormatter.of("bsbs"));
      FORMATTERS.put(
              ZbColumnFamilies.ROLES, KeyFormatter.DbValueFormatter.of("s"));
      FORMATTERS.put(
              ZbColumnFamilies.MAPPING_RULES, KeyFormatter.DbValueFormatter.of("ss"));
      FORMATTERS.put(
              ZbColumnFamilies.CLAIM_BY_ID, KeyFormatter.DbValueFormatter.of("s"));
      FORMATTERS.put(
              ZbColumnFamilies.PERMISSIONS, KeyFormatter.DbValueFormatter.of("sss"));
      FORMATTERS.put(
              ZbColumnFamilies.AUTHORIZATIONS, KeyFormatter.DbValueFormatter.of("l"));
      FORMATTERS.put(
              ZbColumnFamilies.AUTHORIZATION_KEYS_BY_OWNER, KeyFormatter.DbValueFormatter.of("ss"));
      FORMATTERS.put(
              ZbColumnFamilies.USAGE_METRICS, KeyFormatter.DbValueFormatter.of("b"));
  }

  public abstract KeyFormatter forColumnFamily(ZbColumnFamilies columnFamily);

  public static KeyFormatters ofDefault() {
    return new KeyFormatters() {
      @Override
      public KeyFormatter forColumnFamily(final ZbColumnFamilies columnFamily) {
        return FORMATTERS.getOrDefault(columnFamily, HEX_FORMATTER);
      }
    };
  }

  public static KeyFormatters ofHex() {
    return new KeyFormatters() {
      @Override
      public KeyFormatter forColumnFamily(final ZbColumnFamilies columnFamily) {
        return HEX_FORMATTER;
      }
    };
  }

  public static KeyFormatters ofFormat(final String format) {
    return new KeyFormatters() {
      @Override
      public KeyFormatter forColumnFamily(final ZbColumnFamilies columnFamily) {
        return KeyFormatter.DbValueFormatter.of(format);
      }
    };
  }
}
