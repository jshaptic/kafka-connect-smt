package com.github.jshaptic.kafka.connect.transforms;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

public class DebeziumFieldChangeRouter<R extends ConnectRecord<R>> implements Transformation<R> {

  private DebeziumFieldChangeRouterConfig config;

  @Override
  public ConfigDef config() {
    return DebeziumFieldChangeRouterConfig.config();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.config = new DebeziumFieldChangeRouterConfig(configs);
  }

  @Override
  public R apply(R record) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void close() {}

  static final class DebeziumFieldChangeRouterConfig extends AbstractConfig {

    DebeziumFieldChangeRouterConfig(final Map<?, ?> originals) {
      super(config(), originals);
    }

    static ConfigDef config() {
      return new ConfigDef();
    }

  }

}
