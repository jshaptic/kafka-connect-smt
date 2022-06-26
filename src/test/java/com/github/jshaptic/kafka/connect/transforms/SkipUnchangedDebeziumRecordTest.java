package com.github.jshaptic.kafka.connect.transforms;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.testng.annotations.Test;

public class SkipUnchangedDebeziumRecordTest {

  final Schema recordSchema = SchemaBuilder.struct()
      .field("id", Schema.INT8_SCHEMA)
      .field("name", Schema.STRING_SCHEMA)
      .build();

  final Schema sourceSchema = SchemaBuilder.struct()
      .field("lsn", Schema.INT32_SCHEMA)
      .field("ts_ms", Schema.OPTIONAL_INT32_SCHEMA)
      .build();

  final Schema envelopeSchema = SchemaBuilder.struct()
      .name("dummy.Envelope")
      .field("before", recordSchema)
      .field("after", recordSchema)
      .field("source", sourceSchema)
      .field("op", Schema.STRING_SCHEMA)
      .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
      .field("transaction", SchemaBuilder.struct().optional()
          .field("id", Schema.STRING_SCHEMA)
          .field("total_order", Schema.INT64_SCHEMA)
          .field("data_collection_order", Schema.INT64_SCHEMA)
          .build())
      .build();

  @Test
  public void testTombstoneRecord() {
    try (final SkipUnchangedDebeziumRecord<SourceRecord> transform = new SkipUnchangedDebeziumRecord<>()) {
      final Map<String, String> props = new HashMap<>();
      transform.configure(props);

      final SourceRecord tombstone = new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", null, null);
      assertEquals(transform.apply(tombstone), tombstone);
    }
  }

  private SourceRecord createDeletedRecord() {
    final Schema deleteSourceSchema = SchemaBuilder.struct()
        .field("lsn", SchemaBuilder.int32())
        .field("version", SchemaBuilder.string())
        .build();

    final Schema deleteEnvelopeSchema = SchemaBuilder.struct()
        .name("dummy.Envelope")
        .field("before", recordSchema)
        .field("after", recordSchema)
        .field("source", deleteSourceSchema)
        .field("op", Schema.STRING_SCHEMA)
        .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
        .field("transaction", SchemaBuilder.struct().optional()
            .field("id", Schema.STRING_SCHEMA)
            .field("total_order", Schema.INT64_SCHEMA)
            .field("data_collection_order", Schema.INT64_SCHEMA)
            .build())
        .build();

    final Struct before = new Struct(recordSchema);
    final Struct source = new Struct(deleteSourceSchema);

    before.put("id", (byte) 1);
    before.put("name", "myRecord");
    source.put("lsn", 1234);
    source.put("version", "version!");

    final Struct payload = new Struct(deleteEnvelopeSchema);
    payload.put("op", "d");
    payload.put("before", before);
    payload.put("source", source);
    payload.put("ts_ms", Instant.now().toEpochMilli());

    return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", deleteEnvelopeSchema, payload);
  }

  @Test
  public void testDeletedRecord() {
    try (final SkipUnchangedDebeziumRecord<SourceRecord> transform = new SkipUnchangedDebeziumRecord<>()) {
      final Map<String, String> props = new HashMap<>();
      transform.configure(props);

      final SourceRecord deleteRecord = createDeletedRecord();
      assertEquals(transform.apply(deleteRecord), deleteRecord);
    }
  }

  private SourceRecord createCreatedRecord() {
    final Struct after = new Struct(recordSchema);
    final Struct source = new Struct(sourceSchema);

    after.put("id", (byte) 1);
    after.put("name", "myRecord");
    source.put("lsn", 1234);
    source.put("ts_ms", 12836);

    final Struct payload = new Struct(envelopeSchema);
    payload.put("op", "c");
    payload.put("after", after);
    payload.put("source", source);
    payload.put("ts_ms", Instant.now().toEpochMilli());
    return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelopeSchema, payload);
  }

  @Test
  public void testCreatedRecord() {
    try (final SkipUnchangedDebeziumRecord<SourceRecord> transform = new SkipUnchangedDebeziumRecord<>()) {
      final Map<String, String> props = new HashMap<>();
      transform.configure(props);

      final SourceRecord createRecord = createCreatedRecord();
      assertEquals(transform.apply(createRecord), createRecord);
    }
  }

  private SourceRecord createUnknownRecord() {
    final Schema recordSchema = SchemaBuilder.struct().name("unknown")
        .field("id", SchemaBuilder.int8())
        .build();
    final Struct before = new Struct(recordSchema);
    before.put("id", (byte) 1);
    return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", recordSchema, before);
  }

  private SourceRecord createUnknownUnnamedSchemaRecord() {
    final Schema recordSchema = SchemaBuilder.struct()
        .field("id", SchemaBuilder.int8())
        .build();
    final Struct before = new Struct(recordSchema);
    before.put("id", (byte) 1);
    return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", recordSchema, before);
  }

  @Test
  public void testUnknownRecord() {
    try (final SkipUnchangedDebeziumRecord<SourceRecord> transform = new SkipUnchangedDebeziumRecord<>()) {
      final Map<String, String> props = new HashMap<>();
      transform.configure(props);

      final SourceRecord unknownRecord = createUnknownRecord();
      assertEquals(transform.apply(unknownRecord), unknownRecord);

      final SourceRecord unnamedSchemaRecord = createUnknownUnnamedSchemaRecord();
      assertEquals(transform.apply(unnamedSchemaRecord), unnamedSchemaRecord);
    }
  }

  private SourceRecord createUpdatedRecord() {
    final Struct before = new Struct(recordSchema);
    final Struct after = new Struct(recordSchema);
    final Struct source = new Struct(sourceSchema);
    final Struct transaction = new Struct(SchemaBuilder.struct().optional()
        .field("id", Schema.STRING_SCHEMA)
        .field("total_order", Schema.INT64_SCHEMA)
        .field("data_collection_order", Schema.INT64_SCHEMA)
        .build());

    before.put("id", (byte) 1);
    before.put("name", "myRecord");
    after.put("id", (byte) 1);
    after.put("name", "updatedRecord");
    source.put("lsn", 1234);
    transaction.put("id", "571");
    transaction.put("total_order", 42L);
    transaction.put("data_collection_order", 42L);

    final Struct payload = new Struct(envelopeSchema);
    payload.put("op", "u");
    payload.put("before", before);
    payload.put("after", after);
    payload.put("source", source);
    payload.put("ts_ms", Instant.now().toEpochMilli());
    payload.put("transaction", transaction);

    return new SourceRecord(new HashMap<>(), new HashMap<>(), "dummy", envelopeSchema, payload);
  }

  @Test
  public void testUpdatedRecord() {
    try (final SkipUnchangedDebeziumRecord<SourceRecord> transform = new SkipUnchangedDebeziumRecord<>()) {
      final Map<String, String> props = new HashMap<>();
      transform.configure(props);

      final SourceRecord updateRecord = createUpdatedRecord();
      assertEquals(transform.apply(updateRecord), updateRecord);
    }
  }

  @Test
  public void testSkipUpdatedRecordWatchFields() {
    try (final SkipUnchangedDebeziumRecord<SourceRecord> transform = new SkipUnchangedDebeziumRecord<>()) {
      final Map<String, String> props = new HashMap<>();
      props.put("watch.fields", "id");
      transform.configure(props);

      final SourceRecord updateRecord = createUpdatedRecord();
      assertNull(transform.apply(updateRecord));
    }
  }

  @Test
  public void testProcessUpdatedRecordWatchFields() {
    try (final SkipUnchangedDebeziumRecord<SourceRecord> transform = new SkipUnchangedDebeziumRecord<>()) {
      final Map<String, String> props = new HashMap<>();
      props.put("watch.fields", "name");
      transform.configure(props);

      final SourceRecord updateRecord = createUpdatedRecord();
      assertEquals(transform.apply(updateRecord), updateRecord);
    }
  }

  @Test
  public void testSkipUpdatedRecordIgnoreFields() {
    try (final SkipUnchangedDebeziumRecord<SourceRecord> transform = new SkipUnchangedDebeziumRecord<>()) {
      final Map<String, String> props = new HashMap<>();
      props.put("ignore.fields", "name");
      transform.configure(props);

      final SourceRecord updateRecord = createUpdatedRecord();
      assertNull(transform.apply(updateRecord));
    }
  }

  @Test
  public void testProcessUpdatedRecordIgnoreFields() {
    try (final SkipUnchangedDebeziumRecord<SourceRecord> transform = new SkipUnchangedDebeziumRecord<>()) {
      final Map<String, String> props = new HashMap<>();
      props.put("ignore.fields", "id");
      transform.configure(props);

      final SourceRecord updateRecord = createUpdatedRecord();
      assertEquals(transform.apply(updateRecord), updateRecord);
    }
  }

  @Test
  public void testSkipUpdatedRecordWatchIgnoreFields() {
    try (final SkipUnchangedDebeziumRecord<SourceRecord> transform = new SkipUnchangedDebeziumRecord<>()) {
      final Map<String, String> props = new HashMap<>();
      props.put("watch.fields", "id,name");
      props.put("ignore.fields", "name");
      transform.configure(props);

      final SourceRecord updateRecord = createUpdatedRecord();
      assertNull(transform.apply(updateRecord));
    }
  }

  @Test
  public void testProcessUpdatedRecordWatchIgnoreFields() {
    try (final SkipUnchangedDebeziumRecord<SourceRecord> transform = new SkipUnchangedDebeziumRecord<>()) {
      final Map<String, String> props = new HashMap<>();
      props.put("watch.fields", "id,name");
      props.put("ignore.fields", "id");
      transform.configure(props);

      final SourceRecord updateRecord = createUpdatedRecord();
      assertEquals(transform.apply(updateRecord), updateRecord);
    }
  }

}
