package com.github.jshaptic.kafka.connect.transforms;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Debezium generates CDC records that are struct of values containing values <code>before</code> and <code>after</code>
 * and this SMT helps skip messages, which has same values before and after update in other words didn't change their
 * state. This helps to reduce amount of work for further processing.
 * 
 * <p>
 * This SMT works only with update events and has options to specify which fields must watched or ignored in the
 * skipping logic.
 *
 * @author Eugene Shatilo
 */
public class SkipUnchangedDebeziumRecord<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SkipUnchangedDebeziumRecord.class);

  private static final String DEBEZIUM_SCHEMA_NAME_SUFFIX = ".Envelope";

  private final ExtractField<R> beforeDelegate = new ExtractField.Value<R>();
  private final ExtractField<R> afterDelegate = new ExtractField.Value<R>();

  private TransformConfig config;

  @Override
  public void configure(Map<String, ?> configs) {
    this.config = new TransformConfig(configs);

    Map<String, String> delegateConfig = new HashMap<>();
    delegateConfig.put("field", "before");
    beforeDelegate.configure(delegateConfig);

    delegateConfig = new HashMap<>();
    delegateConfig.put("field", "after");
    afterDelegate.configure(delegateConfig);
  }

  @Override
  public R apply(final R record) {
    if (record.value() == null || record.valueSchema() == null || record.valueSchema().name() == null
        || !record.valueSchema().name().endsWith(DEBEZIUM_SCHEMA_NAME_SUFFIX)) {
      return record;
    }

    final R beforeRecord = beforeDelegate.apply(record);
    final R afterRecord = afterDelegate.apply(record);

    if (beforeRecord.value() == null || afterRecord.value() == null) {
      return record;
    }

    final Struct beforeValue =
        requireStruct(beforeRecord.value(), "Read before record state to compare with after state");
    final Struct afterValue =
        requireStruct(afterRecord.value(), "Read after record state to compare with before state");

    final boolean stateHasChanged = beforeRecord.valueSchema().fields().stream()
        .anyMatch(field -> {
          if (fieldIsWatched(field)) {
            return !beforeValue.get(field.name()).equals(afterValue.get(field.name()));
          }
          return false;
        });

    if (stateHasChanged && LOGGER.isTraceEnabled()) {
      beforeRecord.valueSchema().fields()
          .forEach(field -> {
            if (fieldIsWatched(field)) {
              if (!beforeValue.get(field.name()).equals(afterValue.get(field.name()))) {
                LOGGER.trace("Field {} has different values before and after UPDATE", field.name());
              }
            }
          });
    }

    if (stateHasChanged) {
      return record;
    } else {
      LOGGER.trace("Record {} has the same state and will be skipped", record.key());
      return null;
    }
  }

  @Override
  public void close() {
    beforeDelegate.close();
    afterDelegate.close();
  }

  @Override
  public ConfigDef config() {
    return TransformConfig.config();
  }

  private boolean fieldIsWatched(Field field) {
    final boolean fieldIsWatched = config.getWatchFields().isEmpty()
        || (!config.getWatchFields().isEmpty() && config.getWatchFields().contains(field.name()));
    final boolean isFieldIgnored =
        !config.getIgnoreFields().isEmpty() && config.getIgnoreFields().contains(field.name());

    return field.schema().type() == Schema.Type.STRING && fieldIsWatched && !isFieldIgnored;
  }

  static class TransformConfig extends AbstractConfig {

    static final String WATCH_FIELDS_CONFIG = "watch.fields";
    static final String IGNORE_FIELDS_CONFIG = "ignore.fields";

    TransformConfig(final Map<?, ?> originals) {
      super(config(), originals);
    }

    static ConfigDef config() {
      return new ConfigDef()
          .define(
              WATCH_FIELDS_CONFIG,
              ConfigDef.Type.LIST,
              "",
              ConfigDef.Importance.LOW,
              "List of fields, which must be watched, when before and after states are compared,"
                  + " if this list is empty all fields will be involved")
          .define(
              IGNORE_FIELDS_CONFIG,
              ConfigDef.Type.LIST,
              "",
              ConfigDef.Importance.LOW,
              "List of fields, which must be ignored, when before and after states are compared");
    }

    List<String> getWatchFields() {
      return getList(WATCH_FIELDS_CONFIG);
    }

    List<String> getIgnoreFields() {
      return getList(IGNORE_FIELDS_CONFIG);
    }

  }

}
