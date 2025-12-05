package com.example.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Custom Kafka Connect SMT to add __deleted field for soft delete support.
 *
 * This transform inspects CDC records from Debezium and adds a __deleted field:
 * - For DELETE operations: __deleted = "true"
 * - For INSERT/UPDATE operations: __deleted = "false"
 *
 * Configuration:
 * - deleted.field (optional): Name of the field to add (default: "__deleted")
 * - true.value (optional): Value for deleted records (default: "true")
 * - false.value (optional): Value for non-deleted records (default: "false")
 *
 * Example configuration:
 * <pre>
 * "transforms": "unwrap,addDeleted,route",
 * "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
 * "transforms.unwrap.add.fields": "op",
 * "transforms.addDeleted.type": "com.example.kafka.connect.transforms.AddDeletedField$Value",
 * "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter"
 * </pre>
 */
public abstract class AddDeletedField<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String DELETED_FIELD_CONFIG = "deleted.field";
    private static final String DELETED_FIELD_DEFAULT = "__deleted";

    private static final String TRUE_VALUE_CONFIG = "true.value";
    private static final String TRUE_VALUE_DEFAULT = "true";

    private static final String FALSE_VALUE_CONFIG = "false.value";
    private static final String FALSE_VALUE_DEFAULT = "false";

    private static final String OP_FIELD = "op";
    private static final String DELETE_OP = "d";

    private String deletedFieldName;
    private String trueValue;
    private String falseValue;

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(config(), configs);
        deletedFieldName = config.getString(DELETED_FIELD_CONFIG);
        trueValue = config.getString(TRUE_VALUE_CONFIG);
        falseValue = config.getString(FALSE_VALUE_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            // Tombstone record - skip
            return record;
        }

        final Schema valueSchema = record.valueSchema();

        if (valueSchema == null) {
            // Schema-less record (using Map)
            return applySchemaless(record);
        } else {
            // Record with schema (using Struct)
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(record.value(), "schemaless value");
        final Map<String, Object> updatedValue = new HashMap<>(value);

        // Check if 'op' field exists (added by ExtractNewRecordState with add.fields=op)
        String operation = (String) value.get(OP_FIELD);
        String deletedValue = DELETE_OP.equals(operation) ? trueValue : falseValue;

        updatedValue.put(deletedFieldName, deletedValue);

        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), "value with schema");

        // Build new schema with __deleted field
        final SchemaBuilder builder = SchemaBuilder.struct();

        // Copy all existing fields
        for (Field field : value.schema().fields()) {
            builder.field(field.name(), field.schema());
        }

        // Add __deleted field if it doesn't exist
        if (value.schema().field(deletedFieldName) == null) {
            builder.field(deletedFieldName, Schema.STRING_SCHEMA);
        }

        final Schema newSchema = builder.build();
        final Struct newValue = new Struct(newSchema);

        // Copy all existing field values
        for (Field field : value.schema().fields()) {
            newValue.put(field.name(), value.get(field));
        }

        // Determine deleted value based on operation field
        String deletedValue = falseValue;  // Default to false

        Field opField = value.schema().field(OP_FIELD);
        if (opField != null) {
            String operation = (String) value.get(OP_FIELD);
            if (DELETE_OP.equals(operation)) {
                deletedValue = trueValue;
            }
        }

        newValue.put(deletedFieldName, deletedValue);

        return newRecord(record, newSchema, newValue);
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
            .define(DELETED_FIELD_CONFIG, ConfigDef.Type.STRING, DELETED_FIELD_DEFAULT,
                    ConfigDef.Importance.MEDIUM, "Name of the field to add for deleted flag")
            .define(TRUE_VALUE_CONFIG, ConfigDef.Type.STRING, TRUE_VALUE_DEFAULT,
                    ConfigDef.Importance.LOW, "Value to use when record is deleted")
            .define(FALSE_VALUE_CONFIG, ConfigDef.Type.STRING, FALSE_VALUE_DEFAULT,
                    ConfigDef.Importance.LOW, "Value to use when record is not deleted");
    }

    @Override
    public void close() {
        // Nothing to close
    }

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    private Struct requireStruct(Object value, String purpose) {
        if (!(value instanceof Struct)) {
            throw new IllegalArgumentException("Only Struct objects supported for " + purpose);
        }
        return (Struct) value;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> requireMap(Object value, String purpose) {
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException("Only Map objects supported for " + purpose);
        }
        return (Map<String, Object>) value;
    }

    /**
     * Transform for record values.
     */
    public static class Value<R extends ConnectRecord<R>> extends AddDeletedField<R> {
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp()
            );
        }
    }

    /**
     * Transform for record keys (rarely needed, but included for completeness).
     */
    public static class Key<R extends ConnectRecord<R>> extends AddDeletedField<R> {
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                updatedSchema,
                updatedValue,
                record.valueSchema(),
                record.value(),
                record.timestamp()
            );
        }
    }
}